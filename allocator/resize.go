package allocator

import (
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

type resizeContext[K Key, V any] struct {
	oldIndex       *index[K, V]
	newIndex       *index[K, V]
	numBins        uint64
	numChunks      uint64
	active         uint32
	chunkClaimed   uint64
	chunksDone     uint64
	linkNextResize uint32 // Resize link allocation (allocates from top, decreasing)
}

func (m *Map[K, V]) getResizeContext() *resizeContext[K, V] { return m.resizeCtx.Load() }

func (m *Map[K, V]) setResizeContext(ctx *resizeContext[K, V]) { m.resizeCtx.Store(ctx) }

func (m *Map[K, V]) trySetResizeContext(ctx *resizeContext[K, V]) bool {
	return m.resizeCtx.CompareAndSwap(nil, ctx)
}

func (m *Map[K, V]) helpResize() {
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

func (m *Map[K, V]) helpWithChunks(ctx *resizeContext[K, V]) {
	for {
		chunk := atomic.AddUint64(&ctx.chunkClaimed, 1) - 1
		if chunk >= ctx.numChunks {
			// No more chunks to claim - return immediately
			// Operations will handle bin states individually
			return
		}
		m.transferChunk(ctx, chunk)
		done := atomic.AddUint64(&ctx.chunksDone, 1)

		if done == ctx.numChunks { // last chunk complete
			// Set final capacity: linkNextResize represents remaining unused links
			atomic.StoreUint32(&ctx.newIndex.linkCapacity, ctx.linkNextResize-1)

			m.active.Store(ctx.newIndex)
			atomic.StoreUint32(&ctx.active, ResizeInactive)

			ctx.oldIndex = nil // remove to avoid preventing GC
			ctx.newIndex = nil

			m.setResizeContext(nil)
			return
		}
	}
}

func (m *Map[K, V]) triggerResize() bool {
	placeholder := &resizeContext[K, V]{active: ResizeReserved}
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

	newIdx := newIndex[K, V](oldSize * growth)

	// Reserve link capacity for the transfer phase. In the worst case,
	// it's possible for a full link bucket (4 slots) to overflow a primary
	// bucket (capacity 3) when a bin splits. This requires k ≥ 12 entries to
	// cause this (it already uses all 3 links), so at most ⌊used/3⌋ bins can
	// overflow by +1 each. Worst-case total: used + ⌊used/3⌋ = 4U/3.
	resLinkBudget := uint32(len(oldIdx.links))*4/3 + 1
	newIdx.linkCapacity = uint32(len(newIdx.links)) - resLinkBudget

	// Since we only linearize on the oldBin header, if an operation is multiple
	// resizes behind, it may see it's bin is DoneTransfer, but the most recent
	// index has not started transfering the most recent version yet, to avoid
	// extra writes to the new index, we can instead keep the invariant that
	// all new writes to the new bin occur after the transfer by forcing
	// lagging operations to load each resize generation in order.
	oldIdx.indexNext.Store(newIdx)

	ctx := &resizeContext[K, V]{
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

func (m *Map[K, V]) transferChunk(ctx *resizeContext[K, V], chunkIndex uint64) {
	start := chunkIndex * CHUNK_SIZE
	end := start + CHUNK_SIZE
	if end > ctx.numBins {
		end = ctx.numBins
	}
	for i := start; i < end; i++ {
		m.transferBin(ctx, i)
	}
}

func (m *Map[K, V]) transferBin(ctx *resizeContext[K, V], binIndex uint64) {
	oldBin := ctx.oldIndex.getBinByIndex(binIndex)

	// Use atomic OR to set BinInTransfer bit, since we claimed this chunk there will be
	// no concurrent writes to this bin's state.
	h := Header(atomic.OrUint64((*uint64)(&oldBin.Header), uint64(BinInTransfer)<<binStateShift))

	// Above atomic RMW provides an acq-rel barrier,
	// no further slots will become valid or be deleted from this point.

	sentinel := m.GetTransferSentinel(binIndex)

	// Transfer valid primary slots
	m.transferValidSlots(ctx, h.validMask3(), oldBin.slotAt(0), sentinel)

	// Transfer valid link slots
	lm := oldBin.LinkMeta // plain load safe: no new links after BinInTransfer
	if s := lm.getSingle(); s != NO_LINK {
		m.transferValidSlots(ctx, h.validMask4(PRIMARY_SLOTS), ctx.oldIndex.getLinkBucket(s).slotAt(0), sentinel)
	}
	if p := lm.getPairStart(); p != NO_LINK {
		m.transferValidSlots(ctx, h.validMask4(PRIMARY_SLOTS+LINK_SLOTS), ctx.oldIndex.getLinkBucket(p).slotAt(0), sentinel)
		m.transferValidSlots(ctx, h.validMask4(PRIMARY_SLOTS+2*LINK_SLOTS), ctx.oldIndex.getLinkBucket(p+1).slotAt(0), sentinel)
	}

	// Mark bin as complete
	finalHeader := h.setBinState(BinDoneTransfer)
	atomic.StoreUint64((*uint64)(&oldBin.Header), uint64(finalHeader))
}

// transferValidSlots transfers all valid slots indicated by the spread bitmask vm.
// baseSlot points to slot 0 of the bucket; vm has bits at positions 0,2,4,... for valid slots.
func (m *Map[K, V]) transferValidSlots(ctx *resizeContext[K, V], vm uint32, baseSlot *Slot[K, V], sentinel uint64) {
	for vm != 0 {
		slotIdx := bits.TrailingZeros32(vm) >> 1
		slot := (*Slot[K, V])(unsafe.Add(unsafe.Pointer(baseSlot), uintptr(slotIdx)*unsafe.Sizeof(*baseSlot)))

		keyHash := slot.Key

		// We must make sure other threads observe the sentinel before we load the latest slot entry
		// So this must be a sequentially consistent atomic store to be ordered before the next load
		atomic.StoreUint64(&slot.Key, sentinel)

		entry := atomicLoadSlotVal(slot)
		if entry != nil {
			m.insertIntoNewIndex(ctx, ctx.newIndex, keyHash, entry)
		}

		vm &= vm - 1
	}
}

func (m *Map[K, V]) insertIntoNewIndex(ctx *resizeContext[K, V], idx *index[K, V], hash uint64, entry *Entry[K, V]) {
	pb := idx.getBin(hash)

	// In the current implementation this can never fail as there is never
	// less room in the new bin than the previous bin
	slotIndex := ctx.findFreeSlotOrChain(pb)
	slot := idx.getSlotByIndex(pb, slotIndex)

	// Plain stores are safe here on the slot and header since no concurrent operations
	// will access this bucket until the bin is marked as BinDoneTransfer which acts as the release barrier
	slot.Key = hash
	slot.Val = entry

	pb.Header = pb.Header.setSlotStateAndVersion(slotIndex, SlotValid)
}

func (ctx *resizeContext[K, V]) findFreeSlotOrChain(pb *PrimaryBucket[K, V]) int {
	h := pb.Header
	lm := pb.LinkMeta

	// Check primary slots using bitmask (matches chooseInsertSlot pattern)
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

// nextSingleLinkForResize allocates a single link for resize operations from the top of the array
func (ctx *resizeContext[K, V]) nextSingleLinkForResize() uint32 {
	// Allocate from top, working downwards (subtract 1)
	newVal := atomic.AddUint32(&ctx.linkNextResize, ^uint32(0)) // Subtract 1 using two's complement
	if newVal == 0 {
		// This should never happen if reservation is correct, but handle gracefully
		return NO_LINK
	}
	return newVal
}

// nextLinkPairForResize allocates a pair of links for resize operations from the top of the array
func (ctx *resizeContext[K, V]) nextLinkPairForResize() uint32 {
	// Allocate pair from top, working downwards (subtract 2)
	newVal := atomic.AddUint32(&ctx.linkNextResize, ^uint32(1)) // Subtract 2 using two's complement
	if newVal <= 1 {
		// This should never happen if reservation is correct, but handle gracefully
		return NO_LINK
	}
	return newVal // Pair occupies (newVal, newVal+1), both at/above counter
}

// attachSingleForResize attaches a single link for resize operations
func (ctx *resizeContext[K, V]) attachSingleForResize(pb *PrimaryBucket[K, V]) uint32 {
	linkIdx := ctx.nextSingleLinkForResize()
	// Plain store is safe, this should only be called during resize
	pb.LinkMeta = pb.LinkMeta.setSingle(linkIdx)
	return linkIdx
}

// attachPairForResize attaches a pair of links for resize operations
func (ctx *resizeContext[K, V]) attachPairForResize(pb *PrimaryBucket[K, V]) uint32 {
	ps := ctx.nextLinkPairForResize()
	// Plain store is safe, this should only be called during resize
	pb.LinkMeta = pb.LinkMeta.setPairStart(ps)
	return ps
}
