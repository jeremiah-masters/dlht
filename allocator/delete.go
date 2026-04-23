package allocator

import (
	"hash/maphash"
	"math/bits"
	"runtime"
	"unsafe"

	"github.com/jeremiah-masters/dlht/internal/asm"
	"github.com/jeremiah-masters/dlht/internal/cpu"
)

// Delete removes a key from the map if present.
// Returns (deletedValue, success) where success indicates if the deletion succeeded.
func (m *Map[K, V]) Delete(key K) (V, bool) {
	hash := maphash.Comparable(m.hashConfig.Seed, key)
	idx := m.getActiveIndex()

retry:
	pb := idx.getBin(hash)

	// Step 1: Load header (for seqlock validation)
	h0 := atomicLoadHeader(&pb.Header)

	// Step 2: Check bin state
	if h0.getBinState() != BinNoTransfer {
		if h0.getBinState() == BinDoneTransfer {
			idx = idx.getNextIndex()
			goto retry
		}
		cpu.Yield()
		if atomicLoadHeader(&pb.Header).getBinState() == BinInTransfer {
			runtime.Gosched()
		}
		idx = m.getActiveIndex()
		goto retry
	}

	// Step 3: Find the key using the same algorithm as Get and Insert
	var targetSlotIdx int
	var targetSlot *Slot[K, V]
	var targetEntry *Entry[K, V]
	var found bool

	// Generate bitmap representing (<valid slots> & <matching hash>)
	bitmap := m.matchPrimaryBucketKeys(pb, h0, hash)

	// Bitmap encodes matching slots with bit_{i*2} matching slot_i
	for bitmap != 0 {
		slotIdx := bits.TrailingZeros32(bitmap) >> 1
		slot := pb.slotAt(slotIdx)
		candidate := atomicLoadSlotVal(slot)
		if candidate != nil && candidate.Key == key {
			targetSlotIdx = slotIdx
			targetSlot = slot
			targetEntry = candidate
			found = true
			break
		}
		bitmap &= bitmap - 1
	}

	// Check link buckets if not found in primary slots (cold path)
	if !found {
		targetSlotIdx, targetSlot, targetEntry = m.findKeyInLinks(idx, pb, h0, key, hash)
		found = targetSlotIdx != -1
	}

	// Step 4: Validate no concurrent writes (seqlock validation)
	h1 := atomicLoadHeader(&pb.Header)
	if h0 != h1 {
		goto retry
	}

	if !found {
		var zero V
		return zero, false
	}

	// Step 5: Eager DWCAS using seqlock-validated search results
	if asm.DWCASPtr(unsafe.Pointer(targetSlot), hash, unsafe.Pointer(targetEntry), hash, nil) {
		m.markSlotInvalid(pb, targetSlotIdx)
		return targetEntry.Value, true
	}

	// Step 6: Retry loop (only entered on eager DWCAS failure)
	for {
		hLoop := atomicLoadHeader(&pb.Header)
		if hLoop.getBinState() != BinNoTransfer {
			goto retry
		}
		if hLoop.getSlotState(targetSlotIdx) != SlotValid {
			goto retry
		}

		curKey := targetSlot.Key
		if curKey != hash {
			goto retry
		}

		entry := atomicLoadSlotVal(targetSlot)
		if entry == nil || entry.Key != key {
			goto retry
		}

		if atomicLoadHeader(&pb.Header) != hLoop {
			continue
		}

		// TTAS (Test-and-Test-and-Set) filter, re-check entry to avoid cache coherence
		// overhead when contention is expected.
		if atomicLoadSlotVal(targetSlot) != entry {
			continue
		}

		if asm.DWCASPtr(unsafe.Pointer(targetSlot), curKey, unsafe.Pointer(entry), curKey, nil) {
			m.markSlotInvalid(pb, targetSlotIdx)
			return entry.Value, true
		}
	}
}

// markSlotInvalid transitions a slot from Valid to Invalid in the header.
// Best-effort cleanup after Delete's DWCAS linearization point.
//
// Uses atomic AND to unconditionally clear the slot-state bits without a CAS
// loop or version bump. This is safe because:
//   - The bit change itself makes h1 ≠ h0 for seqlock readers (Get/Put/Delete).
//   - ABA is impossible: any Insert re-filling this slot bumps the header version.
//   - No safety checks are needed: the slot must be Valid (DWCAS just succeeded),
//     entry must be nil (DWCAS cleared it), and resize reads entry values not slot states.
func (m *Map[K, V]) markSlotInvalid(pb *PrimaryBucket[K, V], slotIdx int) {
	mask := ^(uint64(0x3) << (uint(slotIdx*2) & 63))
	atomicAndHeader(&pb.Header, mask)
}

// findKeyInLinks searches for a key in link buckets and returns the slot index, slot, and entry if found
func (m *Map[K, V]) findKeyInLinks(idx *index[K, V], pb *PrimaryBucket[K, V], h0 Header, key K, hash uint64) (int, *Slot[K, V], *Entry[K, V]) {
	lm := atomicLoadLinkMeta(&pb.LinkMeta)

	s := lm.getSingle()
	if s == NO_LINK {
		return -1, nil, nil
	}

	if vm := h0.validMask4(PRIMARY_SLOTS); vm != 0 {
		if slotIdx, slot, entry := m.findKeyInLinkBucket(idx.getLinkBucket(s), vm, key, hash, PRIMARY_SLOTS); slotIdx != -1 {
			return slotIdx, slot, entry
		}
	}

	p := lm.getPairStart()
	if p == NO_LINK {
		return -1, nil, nil
	}

	if vm := h0.validMask4(PRIMARY_SLOTS + LINK_SLOTS); vm != 0 {
		if slotIdx, slot, entry := m.findKeyInLinkBucket(idx.getLinkBucket(p), vm, key, hash, PRIMARY_SLOTS+LINK_SLOTS); slotIdx != -1 {
			return slotIdx, slot, entry
		}
	}

	if vm := h0.validMask4(PRIMARY_SLOTS + 2*LINK_SLOTS); vm != 0 {
		if slotIdx, slot, entry := m.findKeyInLinkBucket(idx.getLinkBucket(p+1), vm, key, hash, PRIMARY_SLOTS+2*LINK_SLOTS); slotIdx != -1 {
			return slotIdx, slot, entry
		}
	}

	return -1, nil, nil
}

// findKeyInLinkBucket probe link bucket for key
func (m *Map[K, V]) findKeyInLinkBucket(b *LinkBucket[K, V], vm4 uint32, key K, hash uint64, baseSlotIdx int) (int, *Slot[K, V], *Entry[K, V]) {
	k0 := b.Slots[0].Key
	k1 := b.Slots[1].Key
	k2 := b.Slots[2].Key
	k3 := b.Slots[3].Key

	// Creates spread bitmap with bit_{i*2} set for slot_i with matching hash
	eq := (b2u32(k0 == hash) << 0) |
		(b2u32(k1 == hash) << 2) |
		(b2u32(k2 == hash) << 4) |
		(b2u32(k3 == hash) << 6)
	eq &= vm4

	for eq != 0 {
		slotIdx := bits.TrailingZeros32(eq) >> 1
		slot := b.slotAt(slotIdx)
		entry := atomicLoadSlotVal(slot)
		if entry != nil && entry.Key == key {
			return baseSlotIdx + slotIdx, slot, entry
		}
		eq &= eq - 1 // Clear bit and check next
	}

	return -1, nil, nil
}
