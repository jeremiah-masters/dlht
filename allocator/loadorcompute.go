package allocator

import (
	"hash/maphash"
	"math/bits"
	"runtime"

	"github.com/jeremiah-masters/dlht/internal/cpu"
)

// LoadOrCompute returns the existing value and true without calling fn.
// If key is absent, it calls fn once and returns the result and false,
// inserting the value only if fn returns save=true.
//
// Calls to fn for the same key never overlap. A saved value is always inserted;
// concurrent Insert and LoadOrCompute calls for that key wait for the claim.
// Until then, Get, Put, Delete, and Range treat the key as absent. With no
// intervening Delete, later callers load the current value without running fn.
//
// Claims are matched by their 64-bit hash, so hash collisions also serialize
// callers. Keep fn short: waiters spin and yield. fn must not call Insert or
// LoadOrCompute on this map for the same key or a key with the same hash.
// Calls for other keys, including calls that trigger resize, are supported,
// but cyclic dependencies between callbacks deadlock. Get, Put, and Delete
// are safe to call from fn for any key.
//
// fn must return normally. A panic or runtime.Goexit leaves the claim held,
// blocking later Insert and LoadOrCompute calls with the same hash. To abort
// on panic, recover inside fn and return save=false:
//
//	m.LoadOrCompute(k, func() (v V, save bool) {
//		defer func() { recover() }() // named returns stay (zero, false): abort
//		return riskyCompute()
//	})
func (m *Map[K, V]) LoadOrCompute(key K, fn func() (value V, save bool)) (V, bool) {
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

	// Step 2: Run the Get algorithm - if the key already exists, return it without calling fn
	bitmap := m.matchPrimaryBucketKeys(pb, h0.validMask3(), hash)

	// Check all hash matches in the primary bucket
	for bitmap != 0 {
		slotIdx := bits.TrailingZeros32(bitmap) >> 1
		candidate := atomicLoadSlotVal(pb.slotAt(slotIdx))
		if candidate != nil && candidate.Key == key {
			// Key exists, validate (seqlock) header and return existing value with true
			if h1 := atomicLoadHeader(&pb.Header); h0 == h1 {
				return candidate.Value, true
			}
			goto retry
		}
		bitmap &= bitmap - 1
	}

	if entry := m.scanLinksForKey(idx, pb, h0, key, hash); entry != nil {
		if h1 := atomicLoadHeader(&pb.Header); h0 == h1 {
			return entry.Value, true
		}
		goto retry
	}

	// Wait for a pending compute with the same hash before reserving a slot.
	if h0.shadowMaskAll() != 0 {
		if si, _ := m.findShadowSlot(idx, pb, h0, hash); si >= 0 {
			m.waitForClaim(pb, si)
			goto retry
		}
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

	// Step 4: Reserve the slot (transition Invalid to Trying)
	h1 := h0.setSlotStateAndVersion(slotIndex, SlotTrying)
	if !m.reserveSlot(pb, h0, h1) {
		cpu.Yield() // back off after a failed reserve
		goto retry
	}

	// The reservation owns the slot until it publishes or releases it.
	slot := idx.getSlotByIndex(pb, slotIndex)
	slot.Key = hash

	// Step 5: Publish the claim (Trying to Shadow) before calling fn.
	if m.publishClaim(pb, h1, slotIndex) {
		return m.computeAndResolve(idx, pb, slot, slotIndex, hash, key, fn)
	}

	// Keep the reservation on a failed publish, but rescan before retrying.
retryWithSlot:
	cpu.Yield() // back off before rescanning
	h2 := atomicLoadHeader(&pb.Header)

	if h2.getBinState() != BinNoTransfer {
		if h2.getBinState() == BinDoneTransfer {
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

	// Check if key now exists in primary bucket
	bitmap2 := m.matchPrimaryBucketKeys(pb, h2.validMask3(), hash)

	for bitmap2 != 0 {
		slotIdx := bits.TrailingZeros32(bitmap2) >> 1
		candidate := atomicLoadSlotVal(pb.slotAt(slotIdx))
		if candidate != nil && candidate.Key == key {
			// Key was inserted by another thread, unreserve our slot and return existing value
			if h3 := atomicLoadHeader(&pb.Header); h2 == h3 {
				m.unreserveSlot(pb, nil, slotIndex)
				return candidate.Value, true
			}
			goto retryWithSlot
		}
		bitmap2 &= bitmap2 - 1
	}

	// Check if key exists in link buckets
	if entry := m.scanLinksForKey(idx, pb, h2, key, hash); entry != nil {
		if h3 := atomicLoadHeader(&pb.Header); h2 == h3 {
			m.unreserveSlot(pb, nil, slotIndex)
			return entry.Value, true
		}
		goto retryWithSlot
	}

	// A claim may have appeared since we reserved. Do not publish past it.
	if h2.shadowMaskAll() != 0 {
		if si, _ := m.findShadowSlot(idx, pb, h2, hash); si >= 0 {
			m.unreserveSlot(pb, nil, slotIndex)
			goto retry
		}
	}

	// Key doesn't exist and nobody claims it, try to publish our reserved slot again
	if m.publishClaim(pb, h2, slotIndex) {
		return m.computeAndResolve(idx, pb, slot, slotIndex, hash, key, fn)
	}

	// Still failing, retry the check
	goto retryWithSlot
}

func (m *Map[K, V]) publishClaim(pb *PrimaryBucket[K, V], oldH Header, i int) bool {
	newH := oldH.setSlotStateAndVersion(i, SlotShadow)
	return atomicCASHeader(&pb.Header, oldH, newH)
}

// waitForClaim spins briefly, then yields if the observed slot is still Shadow.
func (m *Map[K, V]) waitForClaim(pb *PrimaryBucket[K, V], i int) {
	cpu.Yield()
	if atomicLoadHeader(&pb.Header).getSlotState(i) == SlotShadow {
		runtime.Gosched()
	}
}

// computeAndResolve keeps the callback and allocation off the load path.
func (m *Map[K, V]) computeAndResolve(idx *index[K, V], pb *PrimaryBucket[K, V], slot *Slot[K, V], slotIndex int, hash uint64, key K, fn func() (V, bool)) (V, bool) {
	value, save := fn()

	var entry *Entry[K, V]
	// The owned Shadow slot changes by +1 on commit or -2 on abort.
	stateDelta := int64(SlotInvalid - SlotShadow)
	if save {
		entry = &Entry[K, V]{Key: key, Value: value}
		// Publish the entry before making the slot Valid, as in Insert.
		atomicStoreSlotVal(slot, entry)
		stateDelta = SlotValid - SlotShadow
	}

	relocated := false
	delta := Header((stateDelta << (uint(slotIndex*2) & 63)) + 1<<versionShift)
	for {
		hLoop := atomicLoadHeader(&pb.Header)
		switch hLoop.getBinState() {
		case BinNoTransfer:
			if relocated {
				// Transfer must carry every claim before marking the bin done.
				slotIndex, slot = m.findShadowSlot(idx, pb, hLoop, hash)
				if slot == nil {
					panic("dlht: LoadOrCompute claim lost during resize relocation")
				}
				relocated = false
				delta = Header((stateDelta << (uint(slotIndex*2) & 63)) + 1<<versionShift)
				if entry != nil {
					atomicStoreSlotVal(slot, entry)
				}
			}
			// Resolve only against NoTransfer. Committing during transfer
			// could lose the value; after transfer we must find the carried claim.
			if atomicCASHeader(&pb.Header, hLoop, hLoop+delta) {
				return value, false
			}
			cpu.Yield() // back off after a failed resolve
		case BinInTransfer:
			cpu.Yield()
			if atomicLoadHeader(&pb.Header).getBinState() == BinInTransfer {
				runtime.Gosched()
			}
		case BinDoneTransfer:
			// Follow one generation at a time; the next bin may also be moving.
			idx = idx.getNextIndex()
			pb = idx.getBin(hash)
			relocated = true
		}
	}
}

// findShadowSlot returns the claim matching hash, or (-1, nil).
// A bin has at most one claim per hash. A subsequent publication must compare
// against h; the snapshot may be stale after a slot is released or reused.
func (m *Map[K, V]) findShadowSlot(idx *index[K, V], pb *PrimaryBucket[K, V], h Header, hash uint64) (int, *Slot[K, V]) {
	sm := h.shadowMask3()
	for sm != 0 {
		slotIdx := bits.TrailingZeros32(sm) >> 1
		if slot := pb.slotAt(slotIdx); slot.Key == hash {
			return slotIdx, slot
		}
		sm &= sm - 1
	}

	// A link is attached before any claim in it is published.
	lm := atomicLoadLinkMeta(&pb.LinkMeta)

	s := lm.getSingle()
	if s == NO_LINK {
		return -1, nil
	}

	if sm := h.shadowMask4(PRIMARY_SLOTS); sm != 0 {
		if i, slot := probeShadowSlot(idx.getLinkBucket(s), sm, hash); slot != nil {
			return PRIMARY_SLOTS + i, slot
		}
	}

	p := lm.getPairStart()
	if p == NO_LINK {
		return -1, nil
	}

	if sm := h.shadowMask4(PRIMARY_SLOTS + LINK_SLOTS); sm != 0 {
		if i, slot := probeShadowSlot(idx.getLinkBucket(p), sm, hash); slot != nil {
			return PRIMARY_SLOTS + LINK_SLOTS + i, slot
		}
	}

	if sm := h.shadowMask4(PRIMARY_SLOTS + 2*LINK_SLOTS); sm != 0 {
		if i, slot := probeShadowSlot(idx.getLinkBucket(p+1), sm, hash); slot != nil {
			return PRIMARY_SLOTS + 2*LINK_SLOTS + i, slot
		}
	}

	return -1, nil
}

// probeShadowSlot returns the in-bucket index and address of the slot holding
// hash among the Shadow slots indicated by the spread bitmask sm4, or (-1, nil).
func probeShadowSlot[K Key, V any](b *LinkBucket[K, V], sm4 uint32, hash uint64) (int, *Slot[K, V]) {
	for sm4 != 0 {
		slotIdx := bits.TrailingZeros32(sm4) >> 1
		if slot := b.slotAt(slotIdx); slot.Key == hash {
			return slotIdx, slot
		}
		sm4 &= sm4 - 1
	}
	return -1, nil
}
