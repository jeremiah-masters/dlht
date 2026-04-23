package allocator

import (
	"math/bits"
	"unsafe"

	"github.com/jeremiah-masters/dlht/internal/cpu"
)

const (
	// Number of bins requred to force mallocgc to allocate directly from the heap
	// https://github.com/golang/go/blob/go1.25.3/src/runtime/malloc.go#L998
	LargeBufferSize = 512
)

func newIndex[K Key, V any](numBins uint64) *index[K, V] {
	return &index[K, V]{
		mask:         numBins - 1,
		bins:         makePrimaryAlignedSlice[K, V](numBins),
		links:        makeLinkAlignedSlice[K, V](numBins / 8),
		linkCapacity: uint32(numBins / 8),
	}
}

func (m *Map[K, V]) getActiveIndex() *index[K, V] { return m.active.Load() }

func (m *index[K, V]) getNextIndex() *index[K, V] { return m.indexNext.Load() }

func (idx *index[K, V]) getBin(hash uint64) *PrimaryBucket[K, V] {
	return idx.getBinByIndex(hash & idx.mask)
}

func (idx *index[K, V]) getBinByIndex(binIndex uint64) *PrimaryBucket[K, V] {
	base := unsafe.SliceData(idx.bins)
	return (*PrimaryBucket[K, V])(unsafe.Add(unsafe.Pointer(base), uintptr(binIndex)*unsafe.Sizeof(*base)))
}

func (idx *index[K, V]) getLinkBucket(linkIndex uint32) *LinkBucket[K, V] {
	index := linkIndex - 1
	base := unsafe.SliceData(idx.links)
	return (*LinkBucket[K, V])(unsafe.Add(unsafe.Pointer(base), uintptr(index)*unsafe.Sizeof(*base)))
}

func (pb *PrimaryBucket[K, V]) slotAt(i int) *Slot[K, V] {
	return (*Slot[K, V])(unsafe.Add(unsafe.Pointer(&pb.Slots), uintptr(i)*unsafe.Sizeof(pb.Slots[0])))
}

func (b *LinkBucket[K, V]) slotAt(i int) *Slot[K, V] {
	return (*Slot[K, V])(unsafe.Add(unsafe.Pointer(&b.Slots), uintptr(i)*unsafe.Sizeof(b.Slots[0])))
}

func (idx *index[K, V]) getSlotByIndex(pb *PrimaryBucket[K, V], slotIndex int) *Slot[K, V] {
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

// chooseInsertSlot inspects all slots and picks the first free slot
// in priority order: primary -> single-link -> pair0 -> pair1.
func (m *Map[K, V]) chooseInsertSlot(idx *index[K, V], pb *PrimaryBucket[K, V]) int {
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

type PrimaryBucketRot8[K Key, V any] struct {
	LinkMeta LinkMeta
	Slots    [3]Slot[K, V]
	Header   Header
}

type LinkBucketRot8[K Key, V any] struct {
	Val   *Entry[K, V]
	Slots [3]Slot[K, V]
	Key   uint64
}

// makePrimaryAlignedSlice creates a cache-line aligned PrimaryBucket slice
//
// Since allocation provided by Go may not be aligned, we can discard the first unaligned
// bytes of the slice. But this means each bucket may now start partway through one of
// the original bucket structs. This actually works fine, but we need to ensure that
// addresses containing pointers in the new slice are using addresses that were considered
// pointers in the old slice for GC tracking.
//
// To handle this, we allocate a new slice using a rotation of the struct fields to make
// the pointer addresses align properly with the GC's expectations.
//
// Memory Layout Example (64-byte cache line, 8-byte offset):
//
// Original PrimaryBucket layout:
// ┌────────┬──────────┬─────────────────────────────────────────────────────┐
// │ Header │ LinkMeta │           Slots[3] (3×16 bytes)                     │
// │  (8B)  │   (8B)   │       [Key,*Val] [Key,*Val] [Key,*Val]              │
// └────────┴──────────┴─────────────────────────────────────────────────────┘
// 0         8         16                32                48               64
//
// Unaligned allocation (8-byte offset from cache line boundary):
// Cache Line:  ┌─────────────────────────────────────────────────────────┐
// Allocation:  │   ┌────────┬──────────┬─────────────────────────────────────┐
//              └── │ Header │ LinkMeta │              Slots[3]               │
//                  └────────┴──────────┴─────────────────────────────────────┘
//                  8        16        24                                64  72
// If we use unsafe to start from next cache line boundary (64), we'd be starting
// partway through the original bucket struct:
//
//              0   8               64  72                128
// Cache Line:  ┌───────────────────┐┌────────────────────┐
// Allocation:                       ┌───┬───┬────────────┐
//                                   │ H │ L │  Slots[3]  │
//                                   └───┴───┴────────────┘
//                                    ↕ ↕
//                  ┌───┬───┬────────────┐
//                  │ H │ L │  Slots[3]  │
//                  └───┴───┴────────────┘
//                  8  16  24           72
/**/
// This is a problem as the GC expects pointers at specific offsets within PrimaryBucket.
// If we reinterpret memory starting at offset 64, the GC will look for pointers
// at addresses in the original layout, potentially missing Val pointers in Slots.
//
// To solve this, we use PrimaryBucketRot8 with rotated field layout:
//
// Cache Line:  ┌────────────────────────────────────────────────────┐
// Allocation:  │   ┌──────────┬─────────────────────────────────────┬────────┐
//              └── │ LinkMeta │              Slots[3]               │ Header │
//                  └──────────┴─────────────────────────────────────┴────────┘
//                  8        16        24                            64      72
//
// If we allocate the slice using the rotated layout, we directly map each field correctly to the
// expected positions in the original bucket struct. This ensures that the GC can track
// pointers at the correct offsets, regardless of the starting address.
//
//              0   8               64  72                128
// Cache Line:  ┌───────────────────┐┌─────────────────────┐
// Allocation:                       ┌───┬┬───┬────────────┐
//                                   │ H ││ L │  Slots[3]  │ ⬅ PrimaryBucket
//                                   └───┴┴───┴────────────┘
//                                    ↕ ↕   ↕       ↕
//                  ┌───┬────────────┬───┐┌───┬────────────┬───┐
//                  │ L │  Slots[3]  │ H ││ L │  Slots[3]  │ H │ ⬅ PrimaryBucketRot8
//                  └───┴────────────┴───┘└───┴────────────┴───┘
//                  8  16  24           72
//
// The pointers located in the Slots are now at the same relative positions the GC expects
// for a normal PrimaryBucket, ensuring proper garbage collection tracking.
func makePrimaryAlignedSlice[K Key, V any](n uint64) []PrimaryBucket[K, V] {
	// Add 2 extra buckets (both bucket types are 64 bytes) since ARM cache line size can be 128 bytes
	// Although it's not needed for the aligned base-case, we maintain consistency with the slice
	// sizes to avoid the slice promoting to a larger size class
	buf := make([]PrimaryBucket[K, V], n+2)
	offset := uintptr(unsafe.Pointer(unsafe.SliceData(buf))) & (cpu.CacheLineSize - 1)
	switch offset {
	case 0:
		return buf[:n:n]
	case 8:
		buf := make([]PrimaryBucketRot8[K, V], n+2)
		offset := uintptr(unsafe.Pointer(unsafe.SliceData(buf))) & (cpu.CacheLineSize - 1)

		if offset == 8 { // Make sure the offset hasn't changed for the newly allocated slice
			alignedPadding := (-uintptr(unsafe.Pointer(unsafe.SliceData(buf)))) & (cpu.CacheLineSize - 1)
			alignedPtr := unsafe.Add(unsafe.Pointer(unsafe.SliceData(buf)), alignedPadding)
			return unsafe.Slice((*PrimaryBucket[K, V])(alignedPtr), n)
		}
	}
	// Fallback - allocate large buffer to heap allocation to ensure alignment,
	// can handle additional offsets as a later improvement instead of this
	return make([]PrimaryBucket[K, V], max(n, LargeBufferSize))[:n:n]
}

// makeLinkAlignedSlice since LinkBucket is a sequence of (uint64, *V) pairs,
func makeLinkAlignedSlice[K Key, V any](n uint64) []LinkBucket[K, V] {
	buf := make([]LinkBucket[K, V], n+2)
	offset := uintptr(unsafe.Pointer(unsafe.SliceData(buf))) & (cpu.CacheLineSize - 1)
	switch offset {
	case 0:
		return buf[:n:n]
	case 8:
		buf := make([]LinkBucketRot8[K, V], n+2)
		offset := uintptr(unsafe.Pointer(unsafe.SliceData(buf))) & (cpu.CacheLineSize - 1)

		if offset == 8 {
			alignedPadding := (-uintptr(unsafe.Pointer(unsafe.SliceData(buf)))) & (cpu.CacheLineSize - 1)
			alignedPtr := unsafe.Add(unsafe.Pointer(unsafe.SliceData(buf)), alignedPadding)
			return unsafe.Slice((*LinkBucket[K, V])(alignedPtr), n)
		}
	}
	return make([]LinkBucket[K, V], max(n, LargeBufferSize))[:n:n]
}
