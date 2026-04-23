package inline

import (
	"hash/maphash"
	"math/bits"
	"runtime"

	"github.com/jeremiah-masters/dlht/internal/cpu"
)

// Delete removes a key from the map.
func (m *Map[V]) Delete(key uint64) bool {
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

	var targetSlotIdx int
	var found bool

	bitmap := matchPrimaryBucketKeys(pb, h0, key)

	for bitmap != 0 {
		slotIdx := bits.TrailingZeros32(bitmap) >> 1
		targetSlotIdx = slotIdx
		found = true
		break
	}

	if !found {
		targetSlotIdx, _, _ = findKeyInLinks(idx, pb, h0, key)
		found = targetSlotIdx != -1
	}

	h1 := atomicLoadHeader(&pb.Header)
	if h0 != h1 {
		goto retry
	}

	if !found {
		return false
	}

	newH := h0.setSlotStateAndVersion(targetSlotIdx, SlotInvalid)
	if atomicCASHeader(&pb.Header, h0, newH) {
		return true
	}
	goto retry
}

// findKeyInLinks searches link buckets for a key, returning (slotIndex, slot, val).
func findKeyInLinks[V Integer](idx *index[V], pb *PrimaryBucket[V], h0 Header, key uint64) (int, *Slot[V], uint64) {
	lm := atomicLoadLinkMeta(&pb.LinkMeta)

	s := lm.getSingle()
	if s == NO_LINK {
		return -1, nil, 0
	}

	if vm := h0.validMask4(PRIMARY_SLOTS); vm != 0 {
		if slotIdx, slot, val := findKeyInLinkBucket(idx.getLinkBucket(s), vm, key, PRIMARY_SLOTS); slotIdx != -1 {
			return slotIdx, slot, val
		}
	}

	p := lm.getPairStart()
	if p == NO_LINK {
		return -1, nil, 0
	}

	if vm := h0.validMask4(PRIMARY_SLOTS + LINK_SLOTS); vm != 0 {
		if slotIdx, slot, val := findKeyInLinkBucket(idx.getLinkBucket(p), vm, key, PRIMARY_SLOTS+LINK_SLOTS); slotIdx != -1 {
			return slotIdx, slot, val
		}
	}

	if vm := h0.validMask4(PRIMARY_SLOTS + 2*LINK_SLOTS); vm != 0 {
		if slotIdx, slot, val := findKeyInLinkBucket(idx.getLinkBucket(p+1), vm, key, PRIMARY_SLOTS+2*LINK_SLOTS); slotIdx != -1 {
			return slotIdx, slot, val
		}
	}

	return -1, nil, 0
}

func findKeyInLinkBucket[V Integer](b *LinkBucket[V], vm4 uint32, key uint64, baseSlotIdx int) (int, *Slot[V], uint64) {
	k0 := b.Slots[0].Key
	k1 := b.Slots[1].Key
	k2 := b.Slots[2].Key
	k3 := b.Slots[3].Key

	eq := (b2u32(k0 == key) << 0) |
		(b2u32(k1 == key) << 2) |
		(b2u32(k2 == key) << 4) |
		(b2u32(k3 == key) << 6)
	eq &= vm4

	for eq != 0 {
		slotIdx := bits.TrailingZeros32(eq) >> 1
		slot := b.slotAt(slotIdx)
		return baseSlotIdx + slotIdx, slot, atomicLoadSlotVal(slot)
	}

	return -1, nil, 0
}
