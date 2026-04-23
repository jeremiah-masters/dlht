package allocator

import (
	"hash/maphash"
	"math/bits"
	"runtime"

	"github.com/jeremiah-masters/dlht/internal/cpu"
)

// Get retrieves (value, found) for the given key
func (m *Map[K, V]) Get(key K) (V, bool) {
	hash := maphash.Comparable(m.hashConfig.Seed, key)

	// Step 1: Load current active index
	idx := m.getActiveIndex()

retry:
	// Step 2: Get the primary bucket for this hash
	pb := idx.getBin(hash)

	// Step 3: Load header (for seqlock validation)
	h0 := atomicLoadHeader(&pb.Header)

	// Step 4: Check bin state
	if h0.getBinState() != BinNoTransfer {
		if h0.getBinState() == BinDoneTransfer {
			idx = idx.getNextIndex()
			goto retry
		}
		// BinInTransfer: short spin-wait, should complete quickly unless resizer is descheduled
		cpu.Yield()
		if atomicLoadHeader(&pb.Header).getBinState() == BinInTransfer {
			runtime.Gosched()
		}
		idx = m.getActiveIndex()
		goto retry
	}

	// Step 5: Scan slots for the key
	// Hot path (key in primary bucket) is split into multiple calls to allow inlining
	bitmap := m.matchPrimaryBucketKeys(pb, h0, hash)

	// Bitmap encodes matching slots with bit_{i*2} matching slot_i
	for bitmap != 0 {
		slotIdx := bits.TrailingZeros32(bitmap) >> 1
		candidate := atomicLoadSlotVal(pb.slotAt(slotIdx))
		if candidate != nil && candidate.Key == key {
			h1 := atomicLoadHeader(&pb.Header)
			if h0 != h1 {
				goto retry
			}
			return candidate.Value, true
		}
		// Clear bit and check next match
		bitmap &= bitmap - 1
	}

	// If no primary bucket match found, check links (cold path)
	entry := m.scanLinksForKey(idx, pb, h0, key, hash)

	// Step 6: Validate no concurrent writes (seqlock validation)
	h1 := atomicLoadHeader(&pb.Header)
	if h0 != h1 {
		goto retry
	}

	if entry != nil {
		return entry.Value, true
	}

	var zero V
	return zero, false
}

// Contains checks if a key exists in the map without returning the value
func (m *Map[K, V]) Contains(key K) bool {
	_, found := m.Get(key)
	return found
}

// matchPrimaryBucketKeys returns a spread bitmap of primary slots that are both valid and match the hash.
// The match bitmap has bits set only at positions 0, 2, 4 so ANDing with (uint32(h) >> 1)
// will match the valid bits in the header to the corresponding bits in the bitmap.
func (m *Map[K, V]) matchPrimaryBucketKeys(pb *PrimaryBucket[K, V], h Header, hash uint64) uint32 {
	k0 := pb.Slots[0].Key
	k1 := pb.Slots[1].Key
	k2 := pb.Slots[2].Key

	bitmap := (b2u32(k0 == hash) << 0) |
		(b2u32(k1 == hash) << 2) |
		(b2u32(k2 == hash) << 4)

	return bitmap & (uint32(h) >> 1)
}

func (m *Map[K, V]) scanLinksForKey(idx *index[K, V], pb *PrimaryBucket[K, V], h Header, key K, hash uint64) *Entry[K, V] {
	lm := atomicLoadLinkMeta(&pb.LinkMeta)

	// If the first link bucket does not exist, other two will not either
	s := lm.getSingle()
	if s == NO_LINK {
		return nil
	}

	// Check single link bucket
	vm := h.validMask4(PRIMARY_SLOTS) // slots 3-6
	if vm != 0 {
		if entry := probeLinkBucket(idx.getLinkBucket(s), vm, key, hash); entry != nil {
			return entry
		}
	}

	// Check pair link buckets if they exist
	p := lm.getPairStart()
	if p == NO_LINK {
		return nil
	}

	vm = h.validMask4(PRIMARY_SLOTS + LINK_SLOTS) // slots 7-10
	if vm != 0 {
		if entry := probeLinkBucket(idx.getLinkBucket(p), vm, key, hash); entry != nil {
			return entry
		}
	}

	vm = h.validMask4(PRIMARY_SLOTS + 2*LINK_SLOTS) // slots 11-14
	if vm != 0 {
		if entry := probeLinkBucket(idx.getLinkBucket(p+1), vm, key, hash); entry != nil {
			return entry
		}
	}

	return nil
}

func probeLinkBucket[K Key, V any](b *LinkBucket[K, V], vm4 uint32, key K, hash uint64) *Entry[K, V] {
	k0 := b.Slots[0].Key
	k1 := b.Slots[1].Key
	k2 := b.Slots[2].Key
	k3 := b.Slots[3].Key

	eq := (b2u32(k0 == hash) << 0) |
		(b2u32(k1 == hash) << 2) |
		(b2u32(k2 == hash) << 4) |
		(b2u32(k3 == hash) << 6)
	eq &= vm4

	for eq != 0 {
		slotIdx := bits.TrailingZeros32(eq) >> 1
		entry := atomicLoadSlotVal(b.slotAt(slotIdx))
		if entry != nil && entry.Key == key {
			return entry
		}
		eq &= eq - 1
	}
	return nil
}
