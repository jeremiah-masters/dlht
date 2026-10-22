package allocator

import (
	"hash/maphash"
	"math/rand/v2"
	"sync"
	"testing"
)

// Every value that enters the map must leave it exactly once: replaced by a
// Put, removed by a Delete, or still there at the end. Run all four write
// operations on one hot key, and on several keys sharing one bin, so most
// CASes fail and the retry paths run.
func TestContendedWritesConserveValues(t *testing.T) {
	sameBin := keysForBin(testSeed, 15, 0, 4, 1<<22)
	if sameBin == nil {
		t.Skip("could not synthesize colliding keys")
	}
	ops := 20000
	if testing.Short() {
		ops = 2000
	}

	for _, tc := range []struct {
		name string
		keys []uint64
	}{
		{"HotKey", sameBin[:1]},
		{"SameBin", sameBin},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := newWithSeed[uint64, uint64](Options{InitialSize: 16}, testSeed)
			type kv struct{ key, val uint64 }
			const workers = 8
			var added, removed [workers][]kv
			gate := newStartGate(workers)
			var wg sync.WaitGroup
			for w := range workers {
				wg.Add(1)
				go func() {
					defer wg.Done()
					rng := rand.New(rand.NewPCG(uint64(w), 0))
					gate.wait()
					for i := range ops {
						k := tc.keys[rng.IntN(len(tc.keys))]
						v := uint64(w)<<32 | uint64(i)
						switch rng.IntN(4) {
						case 0:
							if _, ok := m.Insert(k, v); ok {
								added[w] = append(added[w], kv{k, v})
							}
						case 1:
							if old, ok := m.Put(k, v); ok {
								added[w] = append(added[w], kv{k, v})
								removed[w] = append(removed[w], kv{k, old})
							}
						case 2:
							if old, ok := m.Delete(k); ok {
								removed[w] = append(removed[w], kv{k, old})
							}
						case 3:
							if _, loaded := m.LoadOrCompute(k, func() (uint64, bool) { return v, true }); !loaded {
								added[w] = append(added[w], kv{k, v})
							}
						}
					}
				}()
			}
			gate.release()
			wg.Wait()

			net := map[kv]int{}
			for w := range workers {
				for _, p := range added[w] {
					net[p]++
				}
				for _, p := range removed[w] {
					net[p]--
				}
			}
			for _, k := range tc.keys {
				want := 0
				if v, ok := m.Get(k); ok {
					net[kv{k, v}]--
					want = 1
				}
				if n := countValidSlotsForHash(m, maphash.Comparable(testSeed, k)); n != want {
					t.Errorf("key %d: %d Valid slots, want %d", k, n, want)
				}
			}
			// Values are unique, so each enters at most once.
			for p, n := range net {
				switch {
				case n > 0:
					t.Errorf("key %d value %#x was lost", p.key, p.val)
				case n < 0:
					t.Errorf("key %d value %#x left the map %d more times than it entered", p.key, p.val, -n)
				}
			}
		})
	}
}
