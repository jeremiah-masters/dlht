package inline

import (
	"hash/maphash"
	"math/bits"
	"runtime"
	"unsafe"

	"github.com/jeremiah-masters/dlht/internal/asm"
	"github.com/jeremiah-masters/dlht/internal/cpu"
)

// Put updates an existing key's value. Returns (oldValue, true) on success.
func (m *Map[V]) Put(key uint64, newValue V) (V, bool) {
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

	bitmap := matchPrimaryBucketKeys(pb, h0, key)

	var targetSlot *Slot[V]
	var targetIdx int
	var targetVal uint64
	var found bool

	for bitmap != 0 {
		slotIdx := bits.TrailingZeros32(bitmap) >> 1
		slot := pb.slotAt(slotIdx)
		targetSlot = slot
		targetIdx = slotIdx
		targetVal = atomicLoadSlotVal(slot)
		found = true
		break
	}

	if !found {
		targetIdx, targetSlot, targetVal = findKeyInLinks(idx, pb, h0, key)
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

	newRawValue := uint64(newValue)
	if asm.DWCAS(unsafe.Pointer(targetSlot), key, targetVal, key, newRawValue) {
		return V(targetVal), true
	}

	// Retry loop
	for {
		hLoop := atomicLoadHeader(&pb.Header)
		if hLoop.getSlotState(targetIdx) != SlotValid {
			goto retry
		}

		curKey := targetSlot.Key
		if curKey != key {
			goto retry
		}

		val := atomicLoadSlotVal(targetSlot)
		if atomicLoadHeader(&pb.Header) != hLoop {
			continue
		}

		// TTAS filter
		if atomicLoadSlotVal(targetSlot) != val {
			continue
		}

		if asm.DWCAS(unsafe.Pointer(targetSlot), curKey, val, curKey, newRawValue) {
			return V(val), true
		}
	}
}
