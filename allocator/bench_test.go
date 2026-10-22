package allocator

import (
	"math/rand/v2"
	"sync/atomic"
	"testing"
)

// Single-threaded benchmarks complement the contention suite in benchmarks/.
// The Contended and SameBin benchmarks run on every P and time the CAS retry
// paths; vary the contention with -cpu.

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
	for i := 0; b.Loop(); i++ {
		if _, ok := m.Get(keys[i&(benchKeyspace-1)]); !ok {
			b.Fatal("miss")
		}
	}
}

func BenchmarkGetMiss(b *testing.B) {
	keys := benchKeys()
	m := prefilledMap(keys)
	for i := 0; b.Loop(); i++ {
		if _, ok := m.Get(uint64(i) | 1<<63); ok {
			b.Fatal("unexpected hit")
		}
	}
}

// BenchmarkInsertDelete inserts and deletes a missing key on each iteration.
func BenchmarkInsertDelete(b *testing.B) {
	keys := benchKeys()
	m := prefilledMap(keys)
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

// BenchmarkInsertExisting measures the duplicate-key path.
func BenchmarkInsertExisting(b *testing.B) {
	keys := benchKeys()
	m := prefilledMap(keys)
	for i := 0; b.Loop(); i++ {
		if _, ok := m.Insert(keys[i&(benchKeyspace-1)], 0); ok {
			b.Fatal("unexpected insert")
		}
	}
}

func BenchmarkPutExisting(b *testing.B) {
	keys := benchKeys()
	m := prefilledMap(keys)
	for i := 0; b.Loop(); i++ {
		if _, ok := m.Put(keys[i&(benchKeyspace-1)], uint64(i)); !ok {
			b.Fatal("put missed")
		}
	}
}

// Compare the existing-key path with BenchmarkGetHit.
func BenchmarkLoadOrComputeHit(b *testing.B) {
	keys := benchKeys()
	m := prefilledMap(keys)
	fn := func() (uint64, bool) { b.Fatal("fn ran on hit path"); return 0, false }
	for i := 0; b.Loop(); i++ {
		if _, loaded := m.LoadOrCompute(keys[i&(benchKeyspace-1)], fn); !loaded {
			b.Fatal("miss")
		}
	}
}

// A successful compute reserves, publishes, and commits before deletion.
func BenchmarkLoadOrComputeCompute(b *testing.B) {
	keys := benchKeys()
	m := prefilledMap(keys)
	for i := 0; b.Loop(); i++ {
		k := uint64(i) | 1<<62 | 1<<63
		if _, loaded := m.LoadOrCompute(k, func() (uint64, bool) { return k, true }); loaded {
			b.Fatal("unexpected hit")
		}
		if _, ok := m.Delete(k); !ok {
			b.Fatal("delete failed")
		}
	}
}

// BenchmarkPutContended updates one key from every goroutine.
func BenchmarkPutContended(b *testing.B) {
	m := New[uint64, uint64](Options{InitialSize: 64})
	m.Insert(1, 0)
	b.RunParallel(func(pb *testing.PB) {
		for v := uint64(0); pb.Next(); v++ {
			m.Put(1, v)
		}
	})
}

// BenchmarkInsertDeleteContended inserts and deletes one key from every
// goroutine, so Deletes race each other for the same entry.
func BenchmarkInsertDeleteContended(b *testing.B) {
	m := New[uint64, uint64](Options{InitialSize: 64})
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m.Insert(1, 1)
			m.Delete(1)
		}
	})
}

// sameBinMap returns an empty map and 12 keys that share one of its bins.
func sameBinMap(b *testing.B) (*Map[uint64, uint64], []uint64) {
	keys := keysForBin(testSeed, 63, 0, 12, 1<<24)
	if keys == nil {
		b.Skip("could not synthesize colliding keys")
	}
	return newWithSeed[uint64, uint64](Options{InitialSize: 64}, testSeed), keys
}

// runSameBin gives each goroutine its own key in one bin, so the header CASes
// of different keys collide.
func runSameBin(b *testing.B, op func(m *Map[uint64, uint64], k uint64)) {
	m, keys := sameBinMap(b)
	idx := m.getActiveIndex()
	var id atomic.Int32
	b.RunParallel(func(pb *testing.PB) {
		k := keys[int(id.Add(1)-1)%len(keys)]
		for pb.Next() {
			op(m, k)
		}
	})
	if m.getActiveIndex() != idx {
		b.Fatal("map resized; the keys no longer share a bin")
	}
}

func BenchmarkInsertDeleteSameBin(b *testing.B) {
	runSameBin(b, func(m *Map[uint64, uint64], k uint64) {
		m.Insert(k, k)
		m.Delete(k)
	})
}

func BenchmarkLoadOrComputeSameBin(b *testing.B) {
	runSameBin(b, func(m *Map[uint64, uint64], k uint64) {
		m.LoadOrCompute(k, func() (uint64, bool) { return k, true })
		m.Delete(k)
	})
}
