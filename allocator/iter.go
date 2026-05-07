package allocator

import (
	"iter"
	"math/bits"
	"runtime"

	"github.com/jeremiah-masters/dlht/internal/cpu"
)

// Range yields every key/value the map holds at some moment during the call.
//
// Range is per-entry atomic, not snapshot. Each emitted (k, v) reflects the
// value paired with that key at some point during the call; different keys
// may reflect different points. A key continuously present across the whole
// call is yielded exactly once. Concurrent inserts and deletes may or may not
// be observed. Iteration order is unspecified.
//
// Returning false from yield stops iteration. Range is safe to call alongside
// Get/Insert/Delete/Put, resize, and other Range callers.
func (m *Map[K, V]) Range(yield func(K, V) bool) {
	idx := m.getActiveIndex()
	m.rangeFrom(idx, yield)
}

// rangeFrom walks every bin of idx, following resize chains via scanShards when bins are DoneTransfer.
func (m *Map[K, V]) rangeFrom(idx *index[K, V], yield func(K, V) bool) {
	var buf [MAX_SLOTS_PER_BIN]*Entry[K, V]
	for binIdx := uint64(0); binIdx < uint64(len(idx.bins)); binIdx++ {
		if !m.scanBin(idx, binIdx, buf[:], yield) {
			return
		}
	}
}

// scanBin snapshots one bin into buf via the seqlock and yields each entry.
// Returns false to halt the outer loop when yield asks to stop.
func (m *Map[K, V]) scanBin(idx *index[K, V], binIdx uint64, buf []*Entry[K, V], yield func(K, V) bool) bool {
	pb := idx.getBinByIndex(binIdx)

retry:
	h0 := atomicLoadHeader(&pb.Header)

	switch h0.getBinState() {
	case BinInTransfer:
		cpu.Yield()
		if atomicLoadHeader(&pb.Header).getBinState() == BinInTransfer {
			runtime.Gosched()
		}
		goto retry
	case BinDoneTransfer:
		return m.scanShards(idx, binIdx, buf, yield)
	}

	vmPrim := h0.validMask3()
	vmS := h0.validMask4(PRIMARY_SLOTS)
	vmP0 := h0.validMask4(PRIMARY_SLOTS + LINK_SLOTS)
	vmP1 := h0.validMask4(PRIMARY_SLOTS + 2*LINK_SLOTS)

	if (vmPrim | vmS | vmP0 | vmP1) == 0 {
		return true
	}

	count := readPrimarySlots(pb, vmPrim, buf)

	if (vmS | vmP0 | vmP1) != 0 {
		lm := atomicLoadLinkMeta(&pb.LinkMeta)
		if vmS != 0 {
			count += readLinkSlots(idx.getLinkBucket(lm.getSingle()), vmS, buf[count:])
		}
		if (vmP0 | vmP1) != 0 {
			ps := lm.getPairStart()
			if vmP0 != 0 {
				count += readLinkSlots(idx.getLinkBucket(ps), vmP0, buf[count:])
			}
			if vmP1 != 0 {
				count += readLinkSlots(idx.getLinkBucket(ps+1), vmP1, buf[count:])
			}
		}
	}

	h1 := atomicLoadHeader(&pb.Header)
	if h1 != h0 {
		goto retry
	}

	for i := range count {
		e := buf[i]
		if e == nil {
			continue
		}
		if !yield(e.Key, e.Value) {
			return false
		}
	}
	return true
}

// scanShards walks the descendant bins of a DoneTransfer bin in the next
// index. Entries from old bin i live in new bins (k << oldBits) | i for
// k in [0, growth). The new bin position is the old position extended by
// the next (newBits - oldBits) bits of the hash.
func (m *Map[K, V]) scanShards(oldIdx *index[K, V], oldBinIdx uint64, buf []*Entry[K, V], yield func(K, V) bool) bool {
	// indexNext is published before any bin flips to DoneTransfer, so observing DoneTransfer guarantees a successor.
	newIdx := oldIdx.getNextIndex()

	oldBits := uint(bits.TrailingZeros64(oldIdx.mask + 1))
	newBits := uint(bits.TrailingZeros64(newIdx.mask + 1))
	growth := uint64(1) << (newBits - oldBits)

	for k := range growth {
		newBinIdx := (k << oldBits) | oldBinIdx
		if !m.scanBin(newIdx, newBinIdx, buf, yield) {
			return false
		}
	}
	return true
}

// readPrimarySlots copies entry pointers for primary slots set in vm. vm is
// the spread bitmap from validMask3: bits 0, 2, 4 cover slots 0, 1, 2.
func readPrimarySlots[K Key, V any](pb *PrimaryBucket[K, V], vm uint32, buf []*Entry[K, V]) int {
	n := 0
	if vm&0x01 != 0 {
		buf[n] = atomicLoadSlotVal(pb.slotAt(0))
		n++
	}
	if vm&0x04 != 0 {
		buf[n] = atomicLoadSlotVal(pb.slotAt(1))
		n++
	}
	if vm&0x10 != 0 {
		buf[n] = atomicLoadSlotVal(pb.slotAt(2))
		n++
	}
	return n
}

// readLinkSlots copies entry pointers for link-bucket slots set in vm. vm is
// the spread bitmap from validMask4: bits 0, 2, 4, 6 cover the 4 slots.
func readLinkSlots[K Key, V any](b *LinkBucket[K, V], vm uint32, buf []*Entry[K, V]) int {
	n := 0
	if vm&0x01 != 0 {
		buf[n] = atomicLoadSlotVal(b.slotAt(0))
		n++
	}
	if vm&0x04 != 0 {
		buf[n] = atomicLoadSlotVal(b.slotAt(1))
		n++
	}
	if vm&0x10 != 0 {
		buf[n] = atomicLoadSlotVal(b.slotAt(2))
		n++
	}
	if vm&0x40 != 0 {
		buf[n] = atomicLoadSlotVal(b.slotAt(3))
		n++
	}
	return n
}

// All returns a range-over-func iterator over every (key, value); same contract as Range.
func (m *Map[K, V]) All() iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		m.Range(yield)
	}
}

// Keys returns a range-over-func iterator over every key; same contract as Range.
func (m *Map[K, V]) Keys() iter.Seq[K] {
	return func(yield func(K) bool) {
		m.Range(func(k K, _ V) bool { return yield(k) })
	}
}

// Values returns a range-over-func iterator over every value; same contract as Range.
func (m *Map[K, V]) Values() iter.Seq[V] {
	return func(yield func(V) bool) {
		m.Range(func(_ K, v V) bool { return yield(v) })
	}
}
