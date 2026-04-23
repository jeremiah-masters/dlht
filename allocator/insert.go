package allocator

import (
	"hash/maphash"
	"math/bits"
	"runtime"

	"github.com/jeremiah-masters/dlht/internal/cpu"
)

// Insert inserts (key, value) into the map. If the key already exists, it returns the existing value and false.
// Returns (previousValue, success) where success indicates if the insertion succeeded.
func (m *Map[K, V]) Insert(key K, value V) (V, bool) {
	hash := maphash.Comparable(m.hashConfig.Seed, key)
	idx := m.getActiveIndex()

retry:
	pb := idx.getBin(hash)

	// Step 1: Load header (for seqlock validation)
	h0 := atomicLoadHeader(&pb.Header)

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

	// Step 2: Run the Get algorithm - check if key already exists
	bitmap := m.matchPrimaryBucketKeys(pb, h0, hash)

	// Check all hash matches in the primary bucket
	for bitmap != 0 {
		slotIdx := bits.TrailingZeros32(bitmap) >> 1
		candidate := atomicLoadSlotVal(pb.slotAt(slotIdx))
		if candidate != nil && candidate.Key == key {
			// Key already exists, validate (seqlock) header and return existing value with false
			if h1 := atomicLoadHeader(&pb.Header); h0 == h1 {
				return candidate.Value, false
			}
			goto retry
		}
		bitmap &= bitmap - 1
	}

	if entry := m.scanLinksForKey(idx, pb, h0, key, hash); entry != nil {
		if h1 := atomicLoadHeader(&pb.Header); h0 == h1 {
			return entry.Value, false
		}
		goto retry
	}

	// Step 3: Find the first slot that is in Invalid state.
	// Fast path: derive free primary slot from h0 without reloading the header.
	// The reserve CAS compares against h0, so any staleness causes CAS failure and retry.
	var slotIndex int
	if im := h0.invalidMask3(); im != 0 {
		slotIndex = bits.TrailingZeros32(im) >> 1
	} else {
		// Cold path: primary full, scan link buckets (needs fresh header + LinkMeta)
		slotIndex = m.chooseInsertSlot(idx, pb)
	}
	if slotIndex < 0 { // No slot available. Trigger resize.
		m.triggerResize()
		m.helpResize()
		idx = m.getActiveIndex()
		goto retry
	}

	// Step 4: Reserve the slot (transition Invalid to TryInsert)
	h1 := h0.setSlotStateAndVersion(slotIndex, SlotTrying)
	if !m.reserveSlot(pb, h0, h1) {
		goto retry
	}

	// Step 4.1: Fill the slot
	slot := idx.getSlotByIndex(pb, slotIndex)
	if slot == nil {
		m.unreserveSlot(idx, pb, slotIndex)
		goto retry
	}

	slot.Key = hash
	entry := &Entry[K, V]{Key: key, Value: value}
	atomicStoreSlotVal(slot, entry) // We must release-store the entry to synchronize with any concurrent reads

	// Step 5: Transition slot TryInsert to Valid
	if m.finalizeSlot(pb, h1, slotIndex) {
		var zero V
		return zero, true
	}

	// Finalization failed - header changed. Check if key was inserted by another thread
	// Keep our reserved slot and only unreserve if we find the key already exists
retryWithSlot:
	h2 := atomicLoadHeader(&pb.Header)

	if h2.getBinState() != BinNoTransfer {
		if h2.getBinState() == BinDoneTransfer {
			m.unreserveSlot(idx, pb, slotIndex)
			idx = idx.getNextIndex()
			goto retry
		}
		cpu.Yield()
		if atomicLoadHeader(&pb.Header).getBinState() == BinInTransfer {
			runtime.Gosched()
		}
		m.unreserveSlot(idx, pb, slotIndex)
		idx = m.getActiveIndex()
		goto retry
	}

	// Check if key now exists in primary bucket
	bitmap2 := m.matchPrimaryBucketKeys(pb, h2, hash)

	for bitmap2 != 0 {
		slotIdx := bits.TrailingZeros32(bitmap2) >> 1
		candidate := atomicLoadSlotVal(pb.slotAt(slotIdx))
		if candidate != nil && candidate.Key == key {
			// Key was inserted by another thread, unreserve our slot and return existing value
			if h3 := atomicLoadHeader(&pb.Header); h2 == h3 {
				m.unreserveSlot(idx, pb, slotIndex)
				return candidate.Value, false
			}
			goto retryWithSlot
		}
		bitmap2 &= bitmap2 - 1
	}

	// Check if key exists in link buckets
	if entry := m.scanLinksForKey(idx, pb, h2, key, hash); entry != nil {
		if h3 := atomicLoadHeader(&pb.Header); h2 == h3 {
			m.unreserveSlot(idx, pb, slotIndex)
			return entry.Value, false
		}
		goto retryWithSlot
	}

	// Key doesn't exist yet, try to finalize our reserved slot again
	if m.finalizeSlot(pb, h2, slotIndex) {
		var zero V
		return zero, true
	}

	// Still failing, retry the check
	goto retryWithSlot
}

func (m *Map[K, V]) reserveSlot(pb *PrimaryBucket[K, V], oldH, newH Header) bool {
	return atomicCASHeader(&pb.Header, oldH, newH)
}

func (m *Map[K, V]) finalizeSlot(pb *PrimaryBucket[K, V], oldH Header, i int) bool {
	newH := oldH.setSlotStateAndVersion(i, SlotValid)
	return atomicCASHeader(&pb.Header, oldH, newH)
}

func (m *Map[K, V]) unreserveSlot(idx *index[K, V], pb *PrimaryBucket[K, V], i int) bool {
	// Unreserve should retry on CAS failures, but must preserve prior updates to the header
	for {
		h := atomicLoadHeader(&pb.Header)
		if h.getSlotState(i) != SlotTrying || h.getBinState() != BinNoTransfer {
			return false
		}

		// We still exclusively own SlotTrying; clear the value before releasing the slot
		if slot := idx.getSlotByIndex(pb, i); slot != nil {
			slot.Val = nil
		}

		newH := h.setSlotStateAndVersion(i, SlotInvalid)
		if atomicCASHeader(&pb.Header, h, newH) {
			return true
		}
	}
}
