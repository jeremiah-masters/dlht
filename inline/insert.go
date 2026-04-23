package inline

import (
	"hash/maphash"
	"math/bits"
	"runtime"

	"github.com/jeremiah-masters/dlht/internal/cpu"
)

// Insert inserts (key, value) into the map. If the key already exists, returns (existingValue, false).
func (m *Map[V]) Insert(key uint64, value V) (V, bool) {
	hash := maphash.Comparable(m.hashConfig.Seed, key)
	idx := m.getActiveIndex()

retry:
	pb := idx.getBin(hash)

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

	// Check if key already exists
	bitmap := matchPrimaryBucketKeys(pb, h0, key)

	for bitmap != 0 {
		slotIdx := bits.TrailingZeros32(bitmap) >> 1
		candidate := atomicLoadSlotVal(pb.slotAt(slotIdx))
		if h1 := atomicLoadHeader(&pb.Header); h0 == h1 {
			return V(candidate), false
		}
		goto retry
	}

	if existing, found := scanLinksForKey(idx, pb, h0, key).get(); found {
		if h1 := atomicLoadHeader(&pb.Header); h0 == h1 {
			return V(existing), false
		}
		goto retry
	}

	var zero V

	// Find free slot
	var slotIndex int
	if im := h0.invalidMask3(); im != 0 {
		slotIndex = bits.TrailingZeros32(im) >> 1
	} else {
		slotIndex = m.chooseInsertSlot(idx, pb)
	}
	if slotIndex < 0 {
		m.triggerResize()
		m.helpResize()
		idx = m.getActiveIndex()
		goto retry
	}

	// Reserve the slot
	h1 := h0.setSlotStateAndVersion(slotIndex, SlotTrying)
	if !m.reserveSlot(pb, h0, h1) {
		goto retry
	}

	// Fill the slot
	slot := idx.getSlotByIndex(pb, slotIndex)
	if slot == nil {
		m.unreserveSlot(idx, pb, slotIndex)
		goto retry
	}

	slot.Key = key
	atomicStoreSlotVal(slot, uint64(value))

	// Finalize
	if m.finalizeSlot(pb, h1, slotIndex) {
		return zero, true
	}

	// Finalization failed, retry deduplication
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

	bitmap2 := matchPrimaryBucketKeys(pb, h2, key)

	for bitmap2 != 0 {
		slotIdx := bits.TrailingZeros32(bitmap2) >> 1
		candidate := atomicLoadSlotVal(pb.slotAt(slotIdx))
		if h3 := atomicLoadHeader(&pb.Header); h2 == h3 {
			m.unreserveSlot(idx, pb, slotIndex)
			return V(candidate), false
		}
		goto retryWithSlot
	}

	if existing, found := scanLinksForKey(idx, pb, h2, key).get(); found {
		if h3 := atomicLoadHeader(&pb.Header); h2 == h3 {
			m.unreserveSlot(idx, pb, slotIndex)
			return V(existing), false
		}
		goto retryWithSlot
	}

	if m.finalizeSlot(pb, h2, slotIndex) {
		return zero, true
	}

	goto retryWithSlot
}

func (m *Map[V]) reserveSlot(pb *PrimaryBucket[V], oldH, newH Header) bool {
	return atomicCASHeader(&pb.Header, oldH, newH)
}

func (m *Map[V]) finalizeSlot(pb *PrimaryBucket[V], oldH Header, i int) bool {
	newH := oldH.setSlotStateAndVersion(i, SlotValid)
	return atomicCASHeader(&pb.Header, oldH, newH)
}

func (m *Map[V]) unreserveSlot(idx *index[V], pb *PrimaryBucket[V], i int) bool {
	for {
		h := atomicLoadHeader(&pb.Header)
		if h.getSlotState(i) != SlotTrying || h.getBinState() != BinNoTransfer {
			return false
		}

		if slot := idx.getSlotByIndex(pb, i); slot != nil {
			slot.Key = 0
			slot.Val = 0
		}

		newH := h.setSlotStateAndVersion(i, SlotInvalid)
		if atomicCASHeader(&pb.Header, h, newH) {
			return true
		}
	}
}
