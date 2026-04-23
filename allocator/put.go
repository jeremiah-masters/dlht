package allocator

import (
	"hash/maphash"
	"math/bits"
	"runtime"
	"unsafe"

	"github.com/jeremiah-masters/dlht/internal/asm"
	"github.com/jeremiah-masters/dlht/internal/cpu"
)

// Put update existing key-value pair in the map.
// Returns the old value and a boolean indicating if the update was successful.
func (m *Map[K, V]) Put(key K, newValue V) (V, bool) {
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

	bitmap := m.matchPrimaryBucketKeys(pb, h0, hash)

	var targetSlot *Slot[K, V]
	var targetIdx int
	var targetEntry *Entry[K, V]
	var found bool

	for bitmap != 0 {
		slotIdx := bits.TrailingZeros32(bitmap) >> 1
		slot := pb.slotAt(slotIdx)
		candidate := atomicLoadSlotVal(slot)
		if candidate != nil && candidate.Key == key {
			targetSlot = slot
			targetIdx = slotIdx
			targetEntry = candidate
			found = true
			break
		}
		bitmap &= bitmap - 1
	}

	if !found {
		targetIdx, targetSlot, targetEntry = m.findKeyInLinks(idx, pb, h0, key, hash)
		found = targetIdx != -1
	}

	h1 := atomicLoadHeader(&pb.Header)
	if h0 != h1 {
		goto retry
	}

	if !found {
		var zero V
		return zero, false
	}

	next := &Entry[K, V]{Key: key, Value: newValue}
	if asm.DWCASPtr(unsafe.Pointer(targetSlot), hash, unsafe.Pointer(targetEntry), hash, unsafe.Pointer(next)) {
		return targetEntry.Value, true
	}

	// Must revalidate slot using seqlock proceedure
	for {
		hLoop := atomicLoadHeader(&pb.Header)
		if hLoop.getSlotState(targetIdx) != SlotValid {
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

		// Required to prevent TOCTOU with Delete+Insert slot recycling.
		// Without this, Put can read an uncommitted Insert's entry that may never be commited.
		if atomicLoadHeader(&pb.Header) != hLoop {
			continue
		}

		// TTAS (Test-and-Test-and-Set) filter, re-check entry to avoid cache coherence
		// overhead when contention is expected.
		if atomicLoadSlotVal(targetSlot) != entry {
			continue
		}

		if asm.DWCASPtr(unsafe.Pointer(targetSlot), curKey, unsafe.Pointer(entry), curKey, unsafe.Pointer(next)) {
			return entry.Value, true
		}
	}
}
