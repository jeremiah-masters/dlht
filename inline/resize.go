package inline

import (
	"hash/maphash"
	"math/bits"
	"runtime"
	"sync/atomic"
	"unsafe"
)

const (
	CHUNK_SIZE     = 1024
	ResizeInactive = 0
	ResizeReserved = 1
	ResizeActive   = 2
)

type resizeContext[V Integer] struct {
	oldIndex       *index[V]
	newIndex       *index[V]
	numBins        uint64
	numChunks      uint64
	active         uint32
	chunkClaimed   uint64
	chunksDone     uint64
	linkNextResize uint32
}

func (m *Map[V]) getResizeContext() *resizeContext[V] { return m.resizeCtx.Load() }

func (m *Map[V]) setResizeContext(ctx *resizeContext[V]) { m.resizeCtx.Store(ctx) }

func (m *Map[V]) trySetResizeContext(ctx *resizeContext[V]) bool {
	return m.resizeCtx.CompareAndSwap(nil, ctx)
}

func (m *Map[V]) helpResize() {
	for {
		ctx := m.getResizeContext()
		if ctx == nil {
			return
		}
		switch atomic.LoadUint32(&ctx.active) {
		case ResizeInactive:
			return
		case ResizeReserved:
			runtime.Gosched()
			continue
		case ResizeActive:
			m.helpWithChunks(ctx)
		}
	}
}

func (m *Map[V]) helpWithChunks(ctx *resizeContext[V]) {
	for {
		chunk := atomic.AddUint64(&ctx.chunkClaimed, 1) - 1
		if chunk >= ctx.numChunks {
			return
		}
		m.transferChunk(ctx, chunk)
		done := atomic.AddUint64(&ctx.chunksDone, 1)

		if done == ctx.numChunks {
			atomic.StoreUint32(&ctx.newIndex.linkCapacity, ctx.linkNextResize-1)

			m.active.Store(ctx.newIndex)
			atomic.StoreUint32(&ctx.active, ResizeInactive)

			ctx.oldIndex = nil
			ctx.newIndex = nil

			m.setResizeContext(nil)
			return
		}
	}
}

func (m *Map[V]) triggerResize() bool {
	placeholder := &resizeContext[V]{active: ResizeReserved}
	if !m.trySetResizeContext(placeholder) {
		return false
	}

	oldIdx := m.getActiveIndex()
	oldSize := uint64(len(oldIdx.bins))

	var growth uint64
	if oldSize < 4*1024 {
		growth = 8
	} else if oldSize < 4*1024*1024 {
		growth = 4
	} else {
		growth = 2
	}

	newIdx := newIndex[V](oldSize * growth)

	resLinkBudget := uint32(len(oldIdx.links))*4/3 + 1
	newIdx.linkCapacity = uint32(len(newIdx.links)) - resLinkBudget

	oldIdx.indexNext.Store(newIdx)

	ctx := &resizeContext[V]{
		oldIndex:       oldIdx,
		newIndex:       newIdx,
		numBins:        oldSize,
		numChunks:      (oldSize + CHUNK_SIZE - 1) / CHUNK_SIZE,
		active:         ResizeActive,
		linkNextResize: uint32(len(newIdx.links)),
	}
	m.setResizeContext(ctx)

	m.helpWithChunks(ctx)

	return true
}

func (m *Map[V]) transferChunk(ctx *resizeContext[V], chunkIndex uint64) {
	start := chunkIndex * CHUNK_SIZE
	end := start + CHUNK_SIZE
	if end > ctx.numBins {
		end = ctx.numBins
	}
	for i := start; i < end; i++ {
		m.transferBin(ctx, i)
	}
}

func (m *Map[V]) transferBin(ctx *resizeContext[V], binIndex uint64) {
	oldBin := ctx.oldIndex.getBinByIndex(binIndex)

	h := Header(atomic.OrUint64((*uint64)(&oldBin.Header), uint64(BinInTransfer)<<binStateShift))

	sentinel := m.GetTransferSentinel(binIndex)

	// Transfer valid primary slots
	m.transferValidSlots(ctx, h.validMask3(), oldBin.slotAt(0), sentinel)

	// Transfer valid link slots
	lm := oldBin.LinkMeta
	if s := lm.getSingle(); s != NO_LINK {
		m.transferValidSlots(ctx, h.validMask4(PRIMARY_SLOTS), ctx.oldIndex.getLinkBucket(s).slotAt(0), sentinel)
	}
	if p := lm.getPairStart(); p != NO_LINK {
		m.transferValidSlots(ctx, h.validMask4(PRIMARY_SLOTS+LINK_SLOTS), ctx.oldIndex.getLinkBucket(p).slotAt(0), sentinel)
		m.transferValidSlots(ctx, h.validMask4(PRIMARY_SLOTS+2*LINK_SLOTS), ctx.oldIndex.getLinkBucket(p+1).slotAt(0), sentinel)
	}

	finalHeader := h.setBinState(BinDoneTransfer)
	atomic.StoreUint64((*uint64)(&oldBin.Header), uint64(finalHeader))
}

// transferValidSlots transfers all valid slots indicated by the spread bitmask vm.
func (m *Map[V]) transferValidSlots(ctx *resizeContext[V], vm uint32, baseSlot *Slot[V], sentinel uint64) {
	for vm != 0 {
		slotIdx := bits.TrailingZeros32(vm) >> 1
		slot := (*Slot[V])(unsafe.Add(unsafe.Pointer(baseSlot), uintptr(slotIdx)*unsafe.Sizeof(*baseSlot)))

		actualKey := slot.Key
		if actualKey == sentinel {
			vm &= vm - 1
			continue
		}

		// Sequentially consistent store: other threads must observe sentinel before we load val
		atomic.StoreUint64(&slot.Key, sentinel)

		val := atomicLoadSlotVal(slot)
		hash := maphash.Comparable(m.hashConfig.Seed, actualKey)
		m.insertIntoNewIndex(ctx, ctx.newIndex, actualKey, hash, val)

		vm &= vm - 1
	}
}

func (m *Map[V]) insertIntoNewIndex(ctx *resizeContext[V], idx *index[V], key uint64, hash uint64, val uint64) {
	pb := idx.getBin(hash)

	slotIndex := ctx.findFreeSlotOrChain(pb)
	slot := idx.getSlotByIndex(pb, slotIndex)

	// Plain stores safe: no concurrent access before BinDoneTransfer
	slot.Key = key
	slot.Val = val

	pb.Header = pb.Header.setSlotStateAndVersion(slotIndex, SlotValid)
}

func (ctx *resizeContext[V]) findFreeSlotOrChain(pb *PrimaryBucket[V]) int {
	h := pb.Header
	lm := pb.LinkMeta

	if mask := h.invalidMask3(); mask != 0 {
		return bits.TrailingZeros32(mask) >> 1
	}

	if lm.getSingle() == NO_LINK {
		if ctx.attachSingleForResize(pb) != NO_LINK {
			return PRIMARY_SLOTS
		}
	} else {
		if mask := h.invalidMask4(PRIMARY_SLOTS); mask != 0 {
			return PRIMARY_SLOTS + bits.TrailingZeros32(mask)>>1
		}
	}

	if lm.getPairStart() == NO_LINK {
		if ctx.attachPairForResize(pb) != NO_LINK {
			return PRIMARY_SLOTS + LINK_SLOTS
		}
	} else {
		if mask := h.invalidMask4(PRIMARY_SLOTS + LINK_SLOTS); mask != 0 {
			return PRIMARY_SLOTS + LINK_SLOTS + bits.TrailingZeros32(mask)>>1
		}
		if mask := h.invalidMask4(PRIMARY_SLOTS + 2*LINK_SLOTS); mask != 0 {
			return PRIMARY_SLOTS + 2*LINK_SLOTS + bits.TrailingZeros32(mask)>>1
		}
	}

	return -1
}

func (ctx *resizeContext[V]) nextSingleLinkForResize() uint32 {
	newVal := atomic.AddUint32(&ctx.linkNextResize, ^uint32(0))
	if newVal == 0 {
		return NO_LINK
	}
	return newVal
}

func (ctx *resizeContext[V]) nextLinkPairForResize() uint32 {
	newVal := atomic.AddUint32(&ctx.linkNextResize, ^uint32(1))
	if newVal <= 1 {
		return NO_LINK
	}
	return newVal
}

func (ctx *resizeContext[V]) attachSingleForResize(pb *PrimaryBucket[V]) uint32 {
	linkIdx := ctx.nextSingleLinkForResize()
	pb.LinkMeta = pb.LinkMeta.setSingle(linkIdx)
	return linkIdx
}

func (ctx *resizeContext[V]) attachPairForResize(pb *PrimaryBucket[V]) uint32 {
	ps := ctx.nextLinkPairForResize()
	pb.LinkMeta = pb.LinkMeta.setPairStart(ps)
	return ps
}
