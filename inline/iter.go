package inline

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
func (m *Map[V]) Range(yield func(uint64, V) bool) {
	idx := m.getActiveIndex()
	m.rangeFrom(idx, yield)
}

// rangeFrom walks every bin of idx, following resize chains via scanShards when bins are DoneTransfer.
func (m *Map[V]) rangeFrom(idx *index[V], yield func(uint64, V) bool) {
	var buf [MAX_SLOTS_PER_BIN]Slot[V]
	for binIdx := uint64(0); binIdx < uint64(len(idx.bins)); binIdx++ {
		if !m.scanBin(idx, binIdx, buf[:], yield) {
			return
		}
	}
}

// scanBin snapshots one bin into buf via the seqlock and yields each entry.
// Returns false to halt the outer loop when yield asks to stop.
func (m *Map[V]) scanBin(idx *index[V], binIdx uint64, buf []Slot[V], yield func(uint64, V) bool) bool {
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

	count := readPrimarySlotsInline(pb, vmPrim, buf)

	if (vmS | vmP0 | vmP1) != 0 {
		lm := atomicLoadLinkMeta(&pb.LinkMeta)
		if vmS != 0 {
			count += readLinkSlotsInline(idx.getLinkBucket(lm.getSingle()), vmS, buf[count:])
		}
		if (vmP0 | vmP1) != 0 {
			ps := lm.getPairStart()
			if vmP0 != 0 {
				count += readLinkSlotsInline(idx.getLinkBucket(ps), vmP0, buf[count:])
			}
			if vmP1 != 0 {
				count += readLinkSlotsInline(idx.getLinkBucket(ps+1), vmP1, buf[count:])
			}
		}
	}

	h1 := atomicLoadHeader(&pb.Header)
	if h1 != h0 {
		goto retry
	}

	for i := range count {
		s := buf[i]
		if !yield(s.Key, V(s.Val)) {
			return false
		}
	}
	return true
}

// scanShards walks the descendant bins of a DoneTransfer bin in the next
// index. Entries from old bin i live in new bins (k << oldBits) | i for
// k in [0, growth). The new bin position is the old position extended by
// the next (newBits - oldBits) bits of the hash.
func (m *Map[V]) scanShards(oldIdx *index[V], oldBinIdx uint64, buf []Slot[V], yield func(uint64, V) bool) bool {
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

func readPrimarySlotsInline[V Integer](pb *PrimaryBucket[V], vm uint32, buf []Slot[V]) int {
	n := 0
	if vm&0x01 != 0 {
		s := pb.slotAt(0)
		buf[n] = Slot[V]{Key: s.Key, Val: atomicLoadSlotVal(s)}
		n++
	}
	if vm&0x04 != 0 {
		s := pb.slotAt(1)
		buf[n] = Slot[V]{Key: s.Key, Val: atomicLoadSlotVal(s)}
		n++
	}
	if vm&0x10 != 0 {
		s := pb.slotAt(2)
		buf[n] = Slot[V]{Key: s.Key, Val: atomicLoadSlotVal(s)}
		n++
	}
	return n
}

func readLinkSlotsInline[V Integer](b *LinkBucket[V], vm uint32, buf []Slot[V]) int {
	n := 0
	if vm&0x01 != 0 {
		s := b.slotAt(0)
		buf[n] = Slot[V]{Key: s.Key, Val: atomicLoadSlotVal(s)}
		n++
	}
	if vm&0x04 != 0 {
		s := b.slotAt(1)
		buf[n] = Slot[V]{Key: s.Key, Val: atomicLoadSlotVal(s)}
		n++
	}
	if vm&0x10 != 0 {
		s := b.slotAt(2)
		buf[n] = Slot[V]{Key: s.Key, Val: atomicLoadSlotVal(s)}
		n++
	}
	if vm&0x40 != 0 {
		s := b.slotAt(3)
		buf[n] = Slot[V]{Key: s.Key, Val: atomicLoadSlotVal(s)}
		n++
	}
	return n
}

// All returns a range-over-func iterator over every (key, value); same contract as Range.
func (m *Map[V]) All() iter.Seq2[uint64, V] {
	return func(yield func(uint64, V) bool) {
		m.Range(yield)
	}
}

// Keys returns a range-over-func iterator over every key; same contract as Range.
func (m *Map[V]) Keys() iter.Seq[uint64] {
	return func(yield func(uint64) bool) {
		m.Range(func(k uint64, _ V) bool { return yield(k) })
	}
}

// Values returns a range-over-func iterator over every value; same contract as Range.
func (m *Map[V]) Values() iter.Seq[V] {
	return func(yield func(V) bool) {
		m.Range(func(_ uint64, v V) bool { return yield(v) })
	}
}
