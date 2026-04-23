package allocator

import (
	"fmt"
	"strings"
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

type Stats struct {
	NumBins      uint64  // Number of primary bins
	NumLinks     uint64  // Number of allocated link buckets
	NumSlots     uint64  // Total number of slots (primary + link)
	LoadFactor   float64 // Estimated load factor
	ChainLengths []int   // Distribution of chain lengths
}

// Stats returns current statistics about the hash table
func (m *Map[K, V]) Stats() Stats {
	idx := m.getActiveIndex()

	stats := Stats{
		NumBins:      uint64(len(idx.bins)),
		NumLinks:     uint64(atomic.LoadUint32(&idx.linkNext)),
		NumSlots:     uint64(len(idx.bins))*PRIMARY_SLOTS + uint64(atomic.LoadUint32(&idx.linkNext))*LINK_SLOTS,
		ChainLengths: make([]int, MAX_LINKS+1),
	}

	// Count occupied slots and chain lengths
	var occupiedSlots uint64
	for i := range idx.bins {
		pb := &idx.bins[i]
		h := atomicLoadHeader(&pb.Header)

		// Count occupied slots in this bin
		var binOccupied int
		for j := 0; j < MAX_SLOTS_PER_BIN; j++ {
			if h.getSlotState(j) == SlotValid {
				binOccupied++
				occupiedSlots++
			}
		}

		// Count attached links for chain length distribution
		lm := atomicLoadLinkMeta(&pb.LinkMeta)
		chainLength := lm.getAttachedLinkCount()
		if chainLength < len(stats.ChainLengths) {
			stats.ChainLengths[chainLength]++
		}
	}

	if stats.NumSlots > 0 {
		stats.LoadFactor = float64(occupiedSlots) / float64(stats.NumSlots)
	}

	return stats
}

// String returns a formatted string representation of the statistics
func (s Stats) String() string {
	var sb strings.Builder

	sb.WriteString("Hash Table Statistics:\n")
	sb.WriteString(fmt.Sprintf("  Bins: %d\n", s.NumBins))
	sb.WriteString(fmt.Sprintf("  Links: %d\n", s.NumLinks))
	sb.WriteString(fmt.Sprintf("  Total Slots: %d\n", s.NumSlots))
	sb.WriteString(fmt.Sprintf("  Load Factor: %.3f\n", s.LoadFactor))

	sb.WriteString("  Chain Length Distribution:\n")
	for i, count := range s.ChainLengths {
		if count > 0 {
			percentage := float64(count) / float64(s.NumBins) * 100
			sb.WriteString(fmt.Sprintf("    %d links: %d bins (%.1f%%)\n", i, count, percentage))
		}
	}

	return sb.String()
}
