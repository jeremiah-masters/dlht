package allocator

import (
	"math/rand/v2"
	"testing"
)

// Focused single-threaded micro-benchmarks for the core operation paths.
// Single-threaded on purpose: they isolate instruction-level costs in the
// scan and claim paths (mask arithmetic, branch structure) that the
// contended throughput suite in benchmarks/ would drown in coherence noise.

const benchKeyspace = 1 << 16

func benchKeys() []uint64 {
	rng := rand.New(rand.NewPCG(1, 2))
	keys := make([]uint64, benchKeyspace)
	for i := range keys {
		keys[i] = rng.Uint64()
	}
	return keys
}

func prefilledMap(keys []uint64) *Map[uint64, uint64] {
	m := New[uint64, uint64](Options{InitialSize: benchKeyspace})
	for _, k := range keys {
		m.Insert(k, k)
	}
	return m
}

func BenchmarkGetHit(b *testing.B) {
	keys := benchKeys()
	m := prefilledMap(keys)
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		if _, ok := m.Get(keys[i&(benchKeyspace-1)]); !ok {
			b.Fatal("miss")
		}
	}
}

func BenchmarkGetMiss(b *testing.B) {
	keys := benchKeys()
	m := prefilledMap(keys)
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		if _, ok := m.Get(uint64(i) | 1<<63); ok {
			b.Fatal("unexpected hit")
		}
	}
}

// BenchmarkInsertDelete cycles one key through insert-new and delete,
// exercising the free-slot pick, the claim scan guard, reserve/fill/finalize,
// and the delete DWCAS + cleanup.
func BenchmarkInsertDelete(b *testing.B) {
	keys := benchKeys()
	m := prefilledMap(keys)
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		k := uint64(i) | 1<<62 | 1<<63
		if _, ok := m.Insert(k, k); !ok {
			b.Fatal("insert failed")
		}
		if _, ok := m.Delete(k); !ok {
			b.Fatal("delete failed")
		}
	}
}

// BenchmarkInsertExisting measures the duplicate-key fast path (scan + return).
func BenchmarkInsertExisting(b *testing.B) {
	keys := benchKeys()
	m := prefilledMap(keys)
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		if _, ok := m.Insert(keys[i&(benchKeyspace-1)], 0); ok {
			b.Fatal("unexpected insert")
		}
	}
}

func BenchmarkPutExisting(b *testing.B) {
	keys := benchKeys()
	m := prefilledMap(keys)
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		if _, ok := m.Put(keys[i&(benchKeyspace-1)], uint64(i)); !ok {
			b.Fatal("put missed")
		}
	}
}
