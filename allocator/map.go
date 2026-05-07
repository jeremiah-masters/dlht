package allocator

import (
	"math/bits"
	"sync/atomic"
)

func New[K Key, V any](opts Options) *Map[K, V] {
	if opts.InitialSize == 0 {
		opts.InitialSize = 16
	}
	size := nextPowerOf2(opts.InitialSize)
	m := &Map[K, V]{
		hashConfig: initHashConfig[K](),
	}
	m.active.Store(newIndex[K, V](size))
	return m
}

// Stats is an approximate snapshot of the table's size and load.
type Stats struct {
	Bins         uint64  // primary bins
	Links        uint64  // link buckets allocated
	LinkCapacity uint64  // total link buckets
	Size         uint64  // entries in the map
	Capacity     uint64  // max entries before forced resize
	LoadFactor   float64 // Size / Capacity
	Resizing     bool    // a resize is in progress
}

// Stats returns an approximate snapshot of the table's size and load.
//
// The walk is per-bin atomic, not per-table atomic and not snapshot.
// Each bin's slot states are observed atomically via a single header
// load, but two bins are observed at different moments. Concurrent
// Insert/Delete/Put/resize may or may not be reflected in Size.
//
// Stats reads the index that was active when the call began. If a
// resize completes mid-call, the returned counts reflect that
// now-stale index.
func (m *Map[K, V]) Stats() Stats {
	idx := m.getActiveIndex()

	bins := uint64(len(idx.bins))
	arenaLen := uint64(len(idx.links))
	bottomCap := uint64(atomic.LoadUint32(&idx.linkCapacity))
	linkNext := uint64(atomic.LoadUint32(&idx.linkNext))

	// Resize completion sets linkCapacity = linkNextResize-1 to fence normal
	// bottom-up allocs off the top-down resize tail; the lowest resize-allocated
	// link index is linkCapacity+1. So the transfer-time allocation count is
	// arenaLen-linkCapacity-1 post-resize, and 0 for fresh indexes where
	// linkCapacity == arenaLen.
	var transferLinks uint64
	if bottomCap < arenaLen {
		transferLinks = arenaLen - bottomCap - 1
	}

	size := idx.validSlotCount()

	capacity := bins * MAX_SLOTS_PER_BIN
	var lf float64
	if capacity > 0 {
		lf = float64(size) / float64(capacity)
	}

	return Stats{
		Bins:         bins,
		Links:        linkNext + transferLinks,
		LinkCapacity: arenaLen,
		Size:         size,
		Capacity:     capacity,
		LoadFactor:   lf,
		Resizing:     m.resizeCtx.Load() != nil,
	}
}

// Size returns the approximate number of valid entries in the map.
//
// Per-bin atomic, not snapshot: each bin's slot states load atomically,
// but bins are observed at different moments, so concurrent
// Insert/Delete/Put/resize may or may not be reflected. Reads the index
// that was active when the call began; if a resize completes mid-call,
// the returned count reflects that now-stale index.
//
// O(Bins): one atomic header load per bin.
func (m *Map[K, V]) Size() uint64 {
	return m.getActiveIndex().validSlotCount()
}

// validSlotCount sums Valid slots across all primary bins via one
// atomic header load per bin.
//
// Slot states pack as 15 * 2 bits in [29:0]: Valid=0b10, Trying=0b01,
// Invalid=0b00. The mask 0x2AAAAAAA has the high bit of each pair set
// (positions 1, 3, ..., 29); popcount of h & mask counts valid slots.
func (idx *index[K, V]) validSlotCount() uint64 {
	const validMask uint32 = 0x2AAAAAAA
	var n uint64
	for i := range idx.bins {
		h := atomicLoadHeader(&idx.bins[i].Header)
		n += uint64(bits.OnesCount32(uint32(h) & validMask))
	}
	return n
}
