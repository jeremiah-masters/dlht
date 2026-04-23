package inline

import (
	"hash/maphash"
	"math"
	"math/bits"
)

type HashConfig struct {
	Seed                maphash.Seed
	SentinelForEvenBins uint64 // key whose hash is odd; written into even bins during transfer
	SentinelForOddBins  uint64 // key whose hash is even; written into odd bins during transfer
}

func initHashConfig() HashConfig {
	seed := maphash.MakeSeed()
	even, odd := findSentinels(seed)
	return HashConfig{
		Seed:                seed,
		SentinelForEvenBins: even,
		SentinelForOddBins:  odd,
	}
}

// findSentinels searches for two uint64 keys: one whose hash has odd parity
// (sentinel for even bins) and one whose hash has even parity (sentinel for odd bins).
// Since hash(key) & 1 == binIndex & 1 for all real keys in a bin, a sentinel with
// opposite hash parity can never collide with any real key in that bin.
func findSentinels(seed maphash.Seed) (sentinelForEvenBins, sentinelForOddBins uint64) {
	foundForEven, foundForOdd := false, false
	for k := uint64(math.MaxUint64); ; k-- {
		h := maphash.Comparable(seed, k)
		if !foundForEven && h&1 == 1 {
			sentinelForEvenBins = k
			foundForEven = true
		}
		if !foundForOdd && h&1 == 0 {
			sentinelForOddBins = k
			foundForOdd = true
		}
		if foundForEven && foundForOdd {
			return
		}
		if uint64(math.MaxUint64)-k >= 1000 {
			panic("inline: failed to find transfer sentinels after 1000 iterations")
		}
	}
}

func nextPowerOf2(n uint64) uint64 {
	if n <= 1 {
		return 1
	}
	return 1 << (64 - bits.LeadingZeros64(n-1))
}

func (m *Map[V]) GetTransferSentinel(binIndex uint64) uint64 {
	if binIndex&1 == 0 {
		return m.hashConfig.SentinelForEvenBins
	}
	return m.hashConfig.SentinelForOddBins
}

func (m *Map[V]) IsTransferSentinelForBucket(key uint64, binIndex uint64) bool {
	return key == m.GetTransferSentinel(binIndex)
}
