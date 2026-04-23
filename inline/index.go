package inline

import (
	"math/bits"
	"unsafe"

	"github.com/jeremiah-masters/dlht/internal/cpu"
)

const (
	// Number of bins required to force mallocgc to allocate directly from the heap
	// https://github.com/golang/go/blob/go1.25.3/src/runtime/malloc.go#L998
	LargeBufferSize = 512
)

func newIndex[V Integer](numBins uint64) *index[V] {
	return &index[V]{
		mask:         numBins - 1,
		bins:         makePrimaryAlignedSlice[V](numBins),
		links:        makeLinkAlignedSlice[V](numBins / 8),
		linkCapacity: uint32(numBins / 8),
	}
}

func (m *Map[V]) getActiveIndex() *index[V] { return m.active.Load() }

func (m *index[V]) getNextIndex() *index[V] { return m.indexNext.Load() }

func (idx *index[V]) getBin(hash uint64) *PrimaryBucket[V] {
	return idx.getBinByIndex(hash & idx.mask)
}

func (idx *index[V]) getBinByIndex(binIndex uint64) *PrimaryBucket[V] {
	base := unsafe.SliceData(idx.bins)
	return (*PrimaryBucket[V])(unsafe.Add(unsafe.Pointer(base), uintptr(binIndex)*unsafe.Sizeof(*base)))
}

func (idx *index[V]) getLinkBucket(linkIndex uint32) *LinkBucket[V] {
	index := linkIndex - 1
	base := unsafe.SliceData(idx.links)
	return (*LinkBucket[V])(unsafe.Add(unsafe.Pointer(base), uintptr(index)*unsafe.Sizeof(*base)))
}

func (pb *PrimaryBucket[V]) slotAt(i int) *Slot[V] {
	return (*Slot[V])(unsafe.Add(unsafe.Pointer(&pb.Slots), uintptr(i)*unsafe.Sizeof(pb.Slots[0])))
}

func (b *LinkBucket[V]) slotAt(i int) *Slot[V] {
	return (*Slot[V])(unsafe.Add(unsafe.Pointer(&b.Slots), uintptr(i)*unsafe.Sizeof(b.Slots[0])))
}

func (idx *index[V]) getSlotByIndex(pb *PrimaryBucket[V], slotIndex int) *Slot[V] {
	if slotIndex < PRIMARY_SLOTS {
		return pb.slotAt(slotIndex)
	}
	li := slotIndex - PRIMARY_SLOTS
	bucketIdx := li >> 2
	slotInBucket := li & 3
	lm := atomicLoadLinkMeta(&pb.LinkMeta)
	addr := lm.resolveLink(bucketIdx)
	if addr == NO_LINK {
		return nil
	}
	bucket := idx.getLinkBucket(addr)
	return bucket.slotAt(slotInBucket)
}

// chooseInsertSlot returns the first free slot, checking primary slots before links.
func (m *Map[V]) chooseInsertSlot(idx *index[V], pb *PrimaryBucket[V]) int {
	h := atomicLoadHeader(&pb.Header)

	// Check primary bucket first (hot path)
	primaryMask := h.invalidMask3()
	if primaryMask != 0 {
		return bits.TrailingZeros32(primaryMask) >> 1
	}

	lm := atomicLoadLinkMeta(&pb.LinkMeta)

	// Primary full, check link buckets (cold path)
	hasSingle := lm.getSingle() != NO_LINK
	hasPair := lm.getPairStart() != NO_LINK

	// Check single link bucket if it exists
	if hasSingle {
		singleMask := h.invalidMask4(PRIMARY_SLOTS)
		if singleMask != 0 {
			return PRIMARY_SLOTS + bits.TrailingZeros32(singleMask)>>1
		}
	}

	// Check pair link buckets if they exist
	if hasPair {
		pair0Mask := h.invalidMask4(PRIMARY_SLOTS + LINK_SLOTS)
		if pair0Mask != 0 {
			return PRIMARY_SLOTS + LINK_SLOTS + bits.TrailingZeros32(pair0Mask)>>1
		}

		pair1Mask := h.invalidMask4(PRIMARY_SLOTS + 2*LINK_SLOTS)
		if pair1Mask != 0 {
			return PRIMARY_SLOTS + 2*LINK_SLOTS + bits.TrailingZeros32(pair1Mask)>>1
		}
	}

	// No free slots, try to attach next link bucket
	if !hasSingle {
		if idx.attachSingle(pb) != NO_LINK {
			// Successfully attached single, the first slot in this link is free
			return PRIMARY_SLOTS
		}
	} else if !hasPair {
		if idx.attachPair(pb) != NO_LINK {
			// Successfully attached pair, the first slot in link 1 is free
			return PRIMARY_SLOTS + LINK_SLOTS
		}
	}
	return -1
}

func makePrimaryAlignedSlice[V Integer](n uint64) []PrimaryBucket[V] {
	if n == 0 {
		return nil
	}
	buf := make([]PrimaryBucket[V], n+2)
	alignedPadding := (-uintptr(unsafe.Pointer(unsafe.SliceData(buf)))) & (cpu.CacheLineSize - 1)
	alignedPtr := unsafe.Add(unsafe.Pointer(unsafe.SliceData(buf)), alignedPadding)
	return unsafe.Slice((*PrimaryBucket[V])(alignedPtr), n)
}

func makeLinkAlignedSlice[V Integer](n uint64) []LinkBucket[V] {
	if n == 0 {
		return nil
	}
	buf := make([]LinkBucket[V], n+2)
	alignedPadding := (-uintptr(unsafe.Pointer(unsafe.SliceData(buf)))) & 15
	alignedPtr := unsafe.Add(unsafe.Pointer(unsafe.SliceData(buf)), alignedPadding)
	return unsafe.Slice((*LinkBucket[V])(alignedPtr), n)
}
