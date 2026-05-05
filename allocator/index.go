package allocator

import (
	"math/bits"
	"unsafe"

	"github.com/jeremiah-masters/dlht/internal/cpu"
)

const (
	// mallocgc prepends an 8-byte type pointer to scan allocations whose size
	// falls in (minSizeForMallocHeader, maxSmallSize-mallocHeaderSize]. Smaller
	// allocations keep their pointer bits in the span bitmap; larger ones go
	// through the large-object path on a page-aligned span. See
	// internal/runtime/gc in the Go tree.
	// https://github.com/golang/go/blob/go1.25.3/src/internal/runtime/gc/sizeclasses.go#L86
	minSizeForMallocHeader = 512
	mallocHeaderSize       = 8
	maxSmallSize           = 32 * 1024
	bucketByteSize         = 64

	// Smallest bucket count whose byte size overflows mallocgc's headered-small
	// ceiling, forcing the allocation onto the page-aligned large-object path.
	// https://github.com/golang/go/blob/go1.25.3/src/runtime/malloc.go#L998
	largeBufferSize = (maxSmallSize-mallocHeaderSize)/bucketByteSize + 1 // = 512
)

// mallocgcAddsHeader reports whether a (n+2)-bucket allocation will get the
// 8-byte malloc header. With the header, user data lands at addr%64 == 8 and
// we need the rotated layout. Without it, addr%64 == 0 and the natural layout
// is fine. The byte-size check is enough to decide because every size class
// in the header range has elemsize divisible by 64.
func mallocgcAddsHeader(n uint64) bool {
	total := (n + 2) * bucketByteSize
	return total > minSizeForMallocHeader && total+mallocHeaderSize <= maxSmallSize
}

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
// Cache Line:  ┌────────┬──────────┬─────────────────────────────────────┐
// Allocation:  │  H┌────┴───┬──────┴───┬─────────────────────────────────┴───┐
//              └── ┤ Header │ LinkMeta │              Slots[3]               │
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
// Cache Line:  ┌──────────┬─────────────────────────────────────┬───────┐
// Allocation:  │  H┌──────┴───┬─────────────────────────────────┴───┬───┴────┐
//              └── ┤ LinkMeta │              Slots[3]               │ Header │
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
	// n+2 buckets gives 128 bytes of leading slack, enough for the longest
	// padding the rotation needs (120 bytes when CacheLineSize=128). We pick
	// the layout from the byte size to avoid a wasted probe allocation; the
	// addr&63 checks just guard against the runtime contract changing.
	if mallocgcAddsHeader(n) {
		rotBuf := make([]PrimaryBucketRot8[K, V], n+2)
		rotAddr := uintptr(unsafe.Pointer(unsafe.SliceData(rotBuf)))
		if rotAddr&63 == 8 {
			padding := (-rotAddr) & (cpu.CacheLineSize - 1)
			alignedPtr := unsafe.Add(unsafe.Pointer(unsafe.SliceData(rotBuf)), padding)
			return unsafe.Slice((*PrimaryBucket[K, V])(alignedPtr), n)
		}
	} else {
		buf := make([]PrimaryBucket[K, V], n+2)
		addr := uintptr(unsafe.Pointer(unsafe.SliceData(buf)))
		if addr&63 == 0 {
			// On arm64 we may need to drop the first bucket to land on the next
			// 128-byte cache line; on amd64 the skip is always 0.
			skip := uint64((-addr)&(cpu.CacheLineSize-1)) / 64
			return buf[skip : skip+n : skip+n]
		}
	}
	// LargeBufferSize forces the large-object path, which gives a fresh
	// page-aligned span.
	return make([]PrimaryBucket[K, V], max(n, largeBufferSize))[:n:n]
}

// makeLinkAlignedSlice mirrors makePrimaryAlignedSlice; see that function for
// the alignment scheme.
func makeLinkAlignedSlice[K Key, V any](n uint64) []LinkBucket[K, V] {
	if mallocgcAddsHeader(n) {
		rotBuf := make([]LinkBucketRot8[K, V], n+2)
		rotAddr := uintptr(unsafe.Pointer(unsafe.SliceData(rotBuf)))
		if rotAddr&63 == 8 {
			padding := (-rotAddr) & (cpu.CacheLineSize - 1)
			alignedPtr := unsafe.Add(unsafe.Pointer(unsafe.SliceData(rotBuf)), padding)
			return unsafe.Slice((*LinkBucket[K, V])(alignedPtr), n)
		}
	} else {
		buf := make([]LinkBucket[K, V], n+2)
		addr := uintptr(unsafe.Pointer(unsafe.SliceData(buf)))
		if addr&63 == 0 {
			skip := uint64((-addr)&(cpu.CacheLineSize-1)) / 64
			return buf[skip : skip+n : skip+n]
		}
	}
	return make([]LinkBucket[K, V], max(n, largeBufferSize))[:n:n]
}
