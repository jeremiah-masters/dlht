package allocator

import (
	"hash/maphash"
	"math/bits"
	"runtime"

	"github.com/jeremiah-masters/dlht/internal/cpu"
)

// LoadOrCompute returns the existing value for key if present (loaded=true)
// without calling fn. Otherwise it calls fn exactly once: if fn returns
// save=true the computed value is inserted and returned (loaded=false); if fn
// returns save=false nothing is inserted and the computed value is still
// returned (loaded=false).
//
// The compute path holds a claim on the key while fn runs (a Shadow slot,
// spec/protocol.qnt LC1-LC10), which gives two guarantees a Get-then-Insert
// composition cannot:
//   - fn executions for one key never overlap, and while a computed value
//     remains in the map no further fn runs for that key. With save=true and
//     no intervening Delete, fn runs exactly once per key across all racing
//     callers; the others wait and load its value.
//   - a save=true value is always inserted: while fn runs nothing can make the
//     key visible, so the computed value can never be discarded because a
//     racing writer got there first. A concurrent Put or Delete of the key
//     linearizes after the insertion.
//
// While fn runs, Get/Put/Delete/Range treat the key as absent, and Insert and
// LoadOrCompute of the same key spin (cpu.Yield, then Gosched) until the
// claim resolves, so keep fn short. fn must not Insert or LoadOrCompute this
// key on this map, nor a key whose 64-bit maphash collides with it: a claim
// is identified by hash (its entry does not exist yet) and only resolves when
// fn returns, so such a call inside fn waits on its own claim forever.
// Operations on other keys, including ones that trigger a resize, and
// Get/Put/Delete of any key are safe inside fn; as with locks, two fns that
// each LoadOrCompute the other's key deadlock.
//
// fn must return normally. There is no built-in panic containment (a defer
// would tax every compute): if fn panics or calls runtime.Goexit, the claim
// it holds is never released, and every later Insert or LoadOrCompute of the
// key (or of a hash-colliding one) spins forever. Readers are unaffected, the
// key simply stays absent. If fn can panic, recover inside fn and return with
// save=false to abort cleanly:
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

	// Step 2.1: A Shadow slot holding this hash is another goroutine's pending
	// claim on this key, meaning its fn is running. Wait for it to resolve and
	// rescan: the claim commits (we return its value) or aborts (we compute).
	// A claim is never abandoned in a live bin (its holder always commits,
	// aborts, or relocates), so the wait terminates. When no claim exists
	// anywhere in the bin, the common case, this is a three-instruction test.
	if h0.shadowMaskAll() != 0 {
		if si, _ := m.findShadowSlot(idx, pb, h0, hash); si >= 0 {
			m.waitForClaim(idx, pb, hash)
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

	// Step 4: Reserve the slot (transition Invalid to TryInsert)
	h1 := h0.setSlotStateAndVersion(slotIndex, SlotTrying)
	if !m.reserveSlot(pb, h0, h1) {
		goto retry
	}

	// Step 4.1: Write the claim's key. The entry pointer stays nil until the
	// commit; a non-Valid slot's Val is owner-exclusive.
	slot := idx.getSlotByIndex(pb, slotIndex)
	if slot == nil {
		m.unreserveSlot(idx, pb, slotIndex)
		goto retry
	}

	slot.Key = hash

	// Step 5: Publish the claim (transition TryInsert to Shadow). From here the
	// claim blocks same-key writers while staying invisible to readers.
	if m.publishClaim(pb, h1, slotIndex) {
		return m.computeAndResolve(idx, pb, slot, slotIndex, hash, key, fn)
	}

	// Publish failed - header changed. fn has not run. Keep our reserved slot and
	// re-check under a fresh header, exactly like Insert's finalize retry: unreserve
	// only if the key now exists or another goroutine claimed it first.
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
	bitmap2 := m.matchPrimaryBucketKeys(pb, h2.validMask3(), hash)

	for bitmap2 != 0 {
		slotIdx := bits.TrailingZeros32(bitmap2) >> 1
		candidate := atomicLoadSlotVal(pb.slotAt(slotIdx))
		if candidate != nil && candidate.Key == key {
			// Key was inserted by another thread, unreserve our slot and return existing value
			if h3 := atomicLoadHeader(&pb.Header); h2 == h3 {
				m.unreserveSlot(idx, pb, slotIndex)
				return candidate.Value, true
			}
			goto retryWithSlot
		}
		bitmap2 &= bitmap2 - 1
	}

	// Check if key exists in link buckets
	if entry := m.scanLinksForKey(idx, pb, h2, key, hash); entry != nil {
		if h3 := atomicLoadHeader(&pb.Header); h2 == h3 {
			m.unreserveSlot(idx, pb, slotIndex)
			return entry.Value, true
		}
		goto retryWithSlot
	}

	// Another goroutine published a claim on this hash inside our reserve->publish
	// window: publishing ours too would put two claims on the key. Give up the
	// slot and wait for the holder from the top.
	if h2.shadowMaskAll() != 0 {
		if si, _ := m.findShadowSlot(idx, pb, h2, hash); si >= 0 {
			m.unreserveSlot(idx, pb, slotIndex)
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

// waitForClaim backs off while another goroutine's claim on hash is pending in
// pb: a short spin first (most fns are quick), then a scheduler yield if the
// claim is still there. Callers rescan afterwards.
func (m *Map[K, V]) waitForClaim(idx *index[K, V], pb *PrimaryBucket[K, V], hash uint64) {
	cpu.Yield()
	if h := atomicLoadHeader(&pb.Header); h.shadowMaskAll() != 0 {
		if si, _ := m.findShadowSlot(idx, pb, h, hash); si >= 0 {
			runtime.Gosched()
		}
	}
}

// computeAndResolve runs fn exactly once under the claim held at
// (idx, pb, slot, slotIndex) and resolves it: commit fn's value (save=true) or
// abort (save=false). Kept out of line so LoadOrCompute's frame stays small
// for the load path. No recover/defer around fn (a defer would cost every
// compute); an fn that panics or Goexits orphans the claim, see
// LoadOrCompute's contract.
func (m *Map[K, V]) computeAndResolve(idx *index[K, V], pb *PrimaryBucket[K, V], slot *Slot[K, V], slotIndex int, hash uint64, key K, fn func() (V, bool)) (V, bool) {
	value, save := fn()

	var entry *Entry[K, V]
	if save {
		entry = &Entry[K, V]{Key: key, Value: value}
	}
	m.resolveClaim(idx, pb, slot, slotIndex, hash, entry)
	return value, false
}

// resolveClaim resolves the held claim at (idx, pb, slot, slotIndex): commit
// (entry != nil; release-store the entry, then CAS Shadow -> Valid, the
// linearization point of a save=true compute) or abort (entry == nil; CAS
// Shadow -> Invalid; nothing was stored, so there is nothing to clear, and the
// version bump keeps header history ABA-free).
//
// The holder rule (spec LC6/LC7/LC10): resolve only against a NoTransfer
// header. While the bin transfers, spin: never restart (that would orphan the
// claim the transfer is carrying) and never CAS (a CAS built from an
// InTransfer header would turn the slot Valid behind the transfer's back and
// the entry would be dropped at DoneTransfer). Once the bin is dead, relocate:
// the transfer carried the claim (key only, entry nil) into the next index's
// bin for this hash, where it is the unique Shadow slot holding this hash.
// The computed entry travels in our locals and is stored again after each
// relocation.
func (m *Map[K, V]) resolveClaim(idx *index[K, V], pb *PrimaryBucket[K, V], slot *Slot[K, V], slotIndex int, hash uint64, entry *Entry[K, V]) {
	target := uint8(SlotInvalid)
	if entry != nil {
		// Same publication pattern as Insert: release-store the entry, then
		// the header CAS makes the slot Valid. The store is owner-exclusive
		// while the slot is Shadow.
		atomicStoreSlotVal(slot, entry)
		target = SlotValid
	}

	for {
		hLoop := atomicLoadHeader(&pb.Header)
		switch hLoop.getBinState() {
		case BinNoTransfer:
			if slot == nil {
				// Just relocated: adopt the carried claim. Its existence is
				// guaranteed (spec carriedClaimExists/rsCompleteness: a bin
				// only goes DoneTransfer after every snapshot Shadow was
				// carried); absence means the table is corrupted.
				slotIndex, slot = m.findShadowSlot(idx, pb, hLoop, hash)
				if slot == nil {
					panic("dlht: LoadOrCompute claim lost during resize relocation")
				}
				if entry != nil {
					atomicStoreSlotVal(slot, entry)
				}
			}
			// Nothing but the holder resolves a claim in a live bin, so any
			// other state here is corruption; fail loudly rather than stamp
			// a state over it.
			if hLoop.getSlotState(slotIndex) != SlotShadow {
				panic("dlht: LoadOrCompute claim slot not Shadow at resolve")
			}
			if atomicCASHeader(&pb.Header, hLoop, hLoop.setSlotStateAndVersion(slotIndex, target)) {
				return
			}
			// Another slot's operation bumped the header, or a transfer
			// began; reload and reclassify.
		case BinInTransfer:
			cpu.Yield()
			if atomicLoadHeader(&pb.Header).getBinState() == BinInTransfer {
				runtime.Gosched()
			}
		case BinDoneTransfer:
			// Follow the carried claim one generation forward. The child may
			// itself be transferring or dead; the loop reclassifies.
			idx = idx.getNextIndex()
			pb = idx.getBin(hash)
			slot = nil
		}
	}
}

// findShadowSlot returns the index and address of the Shadow slot in pb that
// holds hash, or (-1, nil). Per (bin, hash) at most one Shadow claim can
// exist: publishing a claim requires that no same-hash claim is present (the
// Shadow back-off arm), and a resize carries a claim into exactly one child
// bin.
//
// The key reads are plain but coherent: a slot listed as Shadow in h had its
// key written before the publish CAS that h's load observed, and a Shadow
// slot's key is never rewritten (the transfer does not sentinel Shadow
// slots). A header change after h was loaded is caught by the caller's CAS or
// seqlock re-check, exactly as for the valid-candidate scans.
func (m *Map[K, V]) findShadowSlot(idx *index[K, V], pb *PrimaryBucket[K, V], h Header, hash uint64) (int, *Slot[K, V]) {
	sm := h.shadowMask3()
	for sm != 0 {
		slotIdx := bits.TrailingZeros32(sm) >> 1
		if slot := pb.slotAt(slotIdx); slot.Key == hash {
			return slotIdx, slot
		}
		sm &= sm - 1
	}

	// A Shadow state bit in a link slot implies the link was attached before
	// that claim's publish CAS, so LinkMeta resolves it (same argument as
	// scanLinksForKey).
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
