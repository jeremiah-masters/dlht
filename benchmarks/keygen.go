package benchmarks

import (
	"fmt"
	"math/rand/v2"
	"strings"
)

// --- Key Generators ---
// All generators produce unique keys. The access pattern (uniform vs zipfian)
// is controlled separately via the distribution sampler in the workload engine.

// GenerateUint64Keys produces n unique random uint64 keys.
func GenerateUint64Keys(n int, seed uint64) []uint64 {
	rng := rand.New(rand.NewPCG(seed, 0))
	seen := make(map[uint64]bool, n)
	keys := make([]uint64, 0, n)
	for len(keys) < n {
		k := rng.Uint64()
		if !seen[k] {
			seen[k] = true
			keys = append(keys, k)
		}
	}
	return keys
}

// GenerateStringHexKeys produces n unique 16-character hex strings.
func GenerateStringHexKeys(n int, seed uint64) []string {
	rng := rand.New(rand.NewPCG(seed, 0))
	seen := make(map[string]bool, n)
	keys := make([]string, 0, n)
	for len(keys) < n {
		k := fmt.Sprintf("%016x", rng.Uint64())
		if !seen[k] {
			seen[k] = true
			keys = append(keys, k)
		}
	}
	return keys
}

// GenerateTrigramKeys produces n unique strings built from English trigram chains.
// Each string is 3-10 trigrams (9-30 characters) with realistic character distribution.
func GenerateTrigramKeys(n int, seed uint64) []string {
	rng := rand.New(rand.NewPCG(seed, 0))
	sampler := newTrigramSampler()
	seen := make(map[string]bool, n)
	keys := make([]string, 0, n)
	for len(keys) < n {
		numTrigrams := 3 + rng.IntN(8) // 3-10 trigrams = 9-30 chars
		var b strings.Builder
		b.Grow(numTrigrams * 3)
		for j := 0; j < numTrigrams; j++ {
			b.WriteString(sampler.sample(rng))
		}
		k := b.String()
		if !seen[k] {
			seen[k] = true
			keys = append(keys, k)
		}
	}
	return keys
}

// --- Distribution Samplers ---

// NewZipfianSampler creates a reusable Zipfian sampler for a given keyspace size.
// Uses s=1.01 (close to YCSB's θ=0.99; Go's rand/v2.NewZipf requires s > 1).
// Lower indices are exponentially more likely.
func NewZipfianSampler(rng *rand.Rand, n int) *rand.Zipf {
	return rand.NewZipf(rng, 1.01, 1.0, uint64(n-1))
}

// ZipfianSample returns a random index in [0, n) following Zipfian distribution (s=1.01).
// Note: creates a new Zipf generator per call. For hot loops, use NewZipfianSampler instead.
func ZipfianSample(rng *rand.Rand, n int) int {
	z := rand.NewZipf(rng, 1.01, 1.0, uint64(n-1))
	return int(z.Uint64())
}

// KeyGenConfig describes a key generator + access distribution combination.
type KeyGenConfig struct {
	Name     string
	KeyType  string                       // "uint64" or "string"
	Generate func(n int, seed uint64) any // returns []uint64 or []string
	// SampleIndex returns an index in [0, n) according to the access distribution.
	SampleIndex func(rng *rand.Rand, n int) int
}

// Uint64KeyGens returns all key generator configs for uint64 keys.
func Uint64KeyGens() []KeyGenConfig {
	return []KeyGenConfig{
		{
			Name:        "Uniform",
			KeyType:     "uint64",
			Generate:    func(n int, seed uint64) any { return GenerateUint64Keys(n, seed) },
			SampleIndex: func(rng *rand.Rand, n int) int { return rng.IntN(n) },
		},
		{
			Name:        "Zipfian",
			KeyType:     "uint64",
			Generate:    func(n int, seed uint64) any { return GenerateUint64Keys(n, seed) },
			SampleIndex: ZipfianSample,
		},
	}
}

// StringKeyGens returns all key generator configs for string keys.
func StringKeyGens() []KeyGenConfig {
	return []KeyGenConfig{
		{
			Name:        "Uniform",
			KeyType:     "string",
			Generate:    func(n int, seed uint64) any { return GenerateStringHexKeys(n, seed) },
			SampleIndex: func(rng *rand.Rand, n int) int { return rng.IntN(n) },
		},
		{
			Name:        "Zipfian",
			KeyType:     "string",
			Generate:    func(n int, seed uint64) any { return GenerateStringHexKeys(n, seed) },
			SampleIndex: ZipfianSample,
		},
		{
			Name:        "Trigram",
			KeyType:     "string",
			Generate:    func(n int, seed uint64) any { return GenerateTrigramKeys(n, seed) },
			SampleIndex: func(rng *rand.Rand, n int) int { return rng.IntN(n) },
		},
	}
}
