package inline

import (
	"math/bits"
	"sync/atomic"
)

func New[V Integer](opts Options) *Map[V] {
	if opts.InitialSize == 0 {
		opts.InitialSize = 16
	}
	size := nextPowerOf2(opts.InitialSize)
	m := &Map[V]{
		hashConfig: initHashConfig(),
	}
	m.active.Store(newIndex[V](size))
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
func (m *Map[V]) Stats() Stats {
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

	// Slot states pack as 15 * 2 bits in [29:0]: Valid=0b10, Trying=0b01,
	// Invalid=0b00. The mask 0x2AAAAAAA has the high bit of each pair set
	// (positions 1, 3, ..., 29); popcount of h & mask counts valid slots.
	const validMask uint32 = 0x2AAAAAAA
	var size uint64
	for i := range idx.bins {
		h := atomicLoadHeader(&idx.bins[i].Header)
		size += uint64(bits.OnesCount32(uint32(h) & validMask))
	}

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
