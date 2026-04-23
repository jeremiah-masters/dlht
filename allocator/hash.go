package allocator

import (
	"hash/maphash"
	"math/bits"
)

type Hasher interface {
	Hash(key uint64) uint64
}

type HashConfig struct {
	Seed maphash.Seed
}

func initHashConfig[K Key]() HashConfig {
	return HashConfig{Seed: maphash.MakeSeed()}
}

// Transfer Sentinels are used as markers to prevent DWCAS updates to a slot during transfer
// Since the key in a slot is the hash of the entries key, even-indexed buckets will contain even hashes
// and odd-indexed buckets will contain odd hashes, so to avoid a collision with a valid entry key hash,
// we use an even value for the odd sentinel and vice-versa
const (
	EvenTransferSentinel = 0xFFFFFFFFFFFFFFFE // Even hash for odd buckets
	OddTransferSentinel  = 0xFFFFFFFFFFFFFFFF // Odd hash for even buckets
)

func nextPowerOf2(n uint64) uint64 {
	if n <= 1 {
		return 1
	}
	return 1 << (64 - bits.LeadingZeros64(n-1))
}

func (m *Map[K, V]) GetTransferSentinel(binIndex uint64) uint64 {
	if binIndex&1 == 0 {
		return OddTransferSentinel
	}
	return EvenTransferSentinel
}

func (m *Map[K, V]) IsTransferSentinelForBucket(key uint64, binIndex uint64) bool {
	expectedSentinel := m.GetTransferSentinel(binIndex)
	return key == expectedSentinel
}
