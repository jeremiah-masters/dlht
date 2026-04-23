package inline

import (
	"hash/maphash"
	"math/bits"
	"runtime"

	"github.com/jeremiah-masters/dlht/internal/cpu"
)

// Get retrieves the value for the given key.
func (m *Map[V]) Get(key uint64) (V, bool) {
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

	for bitmap != 0 {
		slotIdx := bits.TrailingZeros32(bitmap) >> 1
		val := atomicLoadSlotVal(pb.slotAt(slotIdx))
		h1 := atomicLoadHeader(&pb.Header)
		if h0 != h1 {
			goto retry
		}
		return V(val), true
	}

	val := scanLinksForKey(idx, pb, h0, key)

	h1 := atomicLoadHeader(&pb.Header)
	if h0 != h1 {
		goto retry
	}

	if val, found := val.get(); found {
		return V(val), true
	}

	var zero V
	return zero, false
}

func (m *Map[V]) Contains(key uint64) bool {
	_, found := m.Get(key)
	return found
}

// matchPrimaryBucketKeys returns a spread bitmap of primary slots that are both valid and match the key.
func matchPrimaryBucketKeys[V Integer](pb *PrimaryBucket[V], h Header, key uint64) uint32 {
	k0 := pb.Slots[0].Key
	k1 := pb.Slots[1].Key
	k2 := pb.Slots[2].Key

	bitmap := (b2u32(k0 == key) << 0) |
		(b2u32(k1 == key) << 2) |
		(b2u32(k2 == key) << 4)

	return bitmap & (uint32(h) >> 1)
}

type rawValue struct {
	raw   uint64
	found bool
}

func (r rawValue) get() (uint64, bool) {
	return r.raw, r.found
}

func scanLinksForKey[V Integer](idx *index[V], pb *PrimaryBucket[V], h Header, key uint64) rawValue {
	lm := atomicLoadLinkMeta(&pb.LinkMeta)

	s := lm.getSingle()
	if s == NO_LINK {
		return rawValue{}
	}

	vm := h.validMask4(PRIMARY_SLOTS)
	if vm != 0 {
		if val := probeLinkBucket(idx.getLinkBucket(s), vm, key); val.found {
			return val
		}
	}

	p := lm.getPairStart()
	if p == NO_LINK {
		return rawValue{}
	}

	vm = h.validMask4(PRIMARY_SLOTS + LINK_SLOTS)
	if vm != 0 {
		if val := probeLinkBucket(idx.getLinkBucket(p), vm, key); val.found {
			return val
		}
	}

	vm = h.validMask4(PRIMARY_SLOTS + 2*LINK_SLOTS)
	if vm != 0 {
		if val := probeLinkBucket(idx.getLinkBucket(p+1), vm, key); val.found {
			return val
		}
	}

	return rawValue{}
}

func probeLinkBucket[V Integer](b *LinkBucket[V], vm4 uint32, key uint64) rawValue {
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
		return rawValue{raw: atomicLoadSlotVal(b.slotAt(slotIdx)), found: true}
	}
	return rawValue{}
}
