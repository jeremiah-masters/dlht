package tests

import (
	"sync"
	"testing"

	"github.com/jeremiah-masters/dlht"
)

// Concurrent readers on a stable map: each reader sees every key exactly once at the right value.
func TestIter_NoLossWhenStable(t *testing.T) {
	const numKeys, numReaders = 5000, 8

	m := dlht.New[uint64, uint64](dlht.Options{InitialSize: 256})
	for k := uint64(1); k <= numKeys; k++ {
		m.Insert(k, k*10)
	}

	var wg sync.WaitGroup
	for range numReaders {
		wg.Add(1)
		go func() {
			defer wg.Done()
			seen := make(map[uint64]struct{}, numKeys)
			m.Range(func(k, v uint64) bool {
				if _, dup := seen[k]; dup {
					t.Errorf("duplicate key %d", k)
				}
				seen[k] = struct{}{}
				if v != k*10 {
					t.Errorf("key %d: got %d, want %d", k, v, k*10)
				}
				return true
			})
			if len(seen) != numKeys {
				t.Errorf("saw %d keys, want %d", len(seen), numKeys)
			}
		}()
	}
	wg.Wait()
}

// Range across resize: stable prefilled keys remain visible while the writer
// grows the map past at least one resize threshold during the loop.
func TestIter_DuringResize(t *testing.T) {
	const stableSize = 5000

	m := dlht.New[uint64, uint64](dlht.Options{InitialSize: 16})
	for k := uint64(1); k <= stableSize; k++ {
		m.Insert(k, k*10)
	}
	initialBins := m.Stats().Bins

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for k := uint64(stableSize + 1); ; k++ {
			select {
			case <-stop:
				return
			default:
			}
			m.Insert(k, k*10)
		}
	}()

	for i := range 20 {
		seen := make(map[uint64]struct{}, stableSize)
		m.Range(func(k, v uint64) bool {
			if k <= stableSize {
				if _, dup := seen[k]; dup {
					t.Errorf("Range %d: duplicate stable key %d", i, k)
				}
				seen[k] = struct{}{}
				if v != k*10 {
					t.Errorf("Range %d: stable key %d: value %d, want %d", i, k, v, k*10)
				}
			}
			return true
		})
		if len(seen) != stableSize {
			t.Errorf("Range %d: stable keys: saw %d, want %d", i, len(seen), stableSize)
		}
	}

	close(stop)
	wg.Wait()

	if got := m.Stats().Bins; got <= initialBins {
		t.Errorf("expected resize during the loop; bins=%d, initial=%d", got, initialBins)
	}
}

// Stable keys remain visible while writers churn a disjoint range.
func TestIter_StableKeysUnderWriters(t *testing.T) {
	const (
		stableSize  = 2000
		churnStart  = stableSize + 1
		churnEnd    = stableSize + 1000
		numWriters  = 4
		numReaders  = 4
		readerIters = 50
	)

	m := dlht.New[uint64, uint64](dlht.Options{InitialSize: 256})
	for k := uint64(1); k <= stableSize; k++ {
		m.Insert(k, k*10)
	}

	stop := make(chan struct{})
	var wwg sync.WaitGroup
	sliceSize := uint64(churnEnd-churnStart+1) / numWriters
	for w := range numWriters {
		wwg.Add(1)
		go func(seed uint64) {
			defer wwg.Done()
			lo := churnStart + seed*sliceSize
			hi := lo + sliceSize - 1
			if seed == numWriters-1 {
				hi = churnEnd
			}
			for k := lo; ; {
				select {
				case <-stop:
					return
				default:
				}
				m.Insert(k, k*10)
				m.Delete(k)
				if k++; k > hi {
					k = lo
				}
			}
		}(uint64(w))
	}

	var rwg sync.WaitGroup
	for range numReaders {
		rwg.Add(1)
		go func() {
			defer rwg.Done()
			for range readerIters {
				seen := make(map[uint64]struct{}, stableSize)
				m.Range(func(k, v uint64) bool {
					switch {
					case k <= stableSize:
						if _, dup := seen[k]; dup {
							t.Errorf("duplicate stable key %d", k)
						}
						seen[k] = struct{}{}
						if v != k*10 {
							t.Errorf("stable key %d: value %d, want %d", k, v, k*10)
						}
					case k < churnStart || k > churnEnd:
						t.Errorf("phantom key %d", k)
					}
					return true
				})
				if len(seen) != stableSize {
					t.Errorf("stable keys: saw %d, want %d", len(seen), stableSize)
				}
			}
		}()
	}
	rwg.Wait()
	close(stop)
	wwg.Wait()
}

// Per-entry atomicity under concurrent Put: every (k, v) Range emits matches a value the writer recorded for k.
func TestIter_PerEntryValueValidity(t *testing.T) {
	const numKeys = 200

	m := dlht.New[uint64, uint64](dlht.Options{InitialSize: 64})

	var mu sync.Mutex
	history := make(map[uint64]map[uint64]struct{}, numKeys)
	record := func(k, v uint64) {
		mu.Lock()
		defer mu.Unlock()
		s := history[k]
		if s == nil {
			s = map[uint64]struct{}{}
			history[k] = s
		}
		s[v] = struct{}{}
	}
	knew := func(k, v uint64) bool {
		mu.Lock()
		defer mu.Unlock()
		_, ok := history[k][v]
		return ok
	}

	for k := uint64(1); k <= numKeys; k++ {
		v := k * 10
		m.Insert(k, v)
		record(k, v)
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for w := range 4 {
		wg.Add(1)
		go func(seed uint64) {
			defer wg.Done()
			counter := uint64(1_000_000) + seed*100_000
			for {
				select {
				case <-stop:
					return
				default:
				}
				k := 1 + (counter % numKeys)
				// Record before Put.
				record(k, counter)
				m.Put(k, counter)
				counter++
			}
		}(uint64(w))
	}

	for range 50 {
		m.Range(func(k, v uint64) bool {
			if !knew(k, v) {
				t.Errorf("emitted unknown (k=%d, v=%d)", k, v)
			}
			return true
		})
	}

	close(stop)
	wg.Wait()
}
