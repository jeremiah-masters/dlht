package pbt

import (
	"maps"
	"slices"
	"sync"
	"testing"

	"github.com/jeremiah-masters/dlht"

	"pgregory.net/rapid"
)

// Runs the same random op sequence against a dlht.Map and a map[K]int, then
// verifies Range over the dlht.Map yields the same (k, v) set as the map[K]int.
func runRangeSequentialModel[K dlht.Key](t *rapid.T, keyGen *rapid.Generator[K]) {
	opsLen := rapid.IntRange(0, 256).Draw(t, "opsLen")
	valGen := rapid.IntRange(-1_000_000, 1_000_000)
	ops := GenOpSequence(keyGen, valGen, MixBalanced, opsLen, opsLen).Draw(t, "ops")

	m := dlht.New[K, int](dlht.Options{InitialSize: 16})
	model := make(map[K]int)

	for _, o := range ops {
		switch o.Kind {
		case OpGet:
			want, present := model[o.Key]
			got, ok := m.Get(o.Key)
			if ok != present || (present && got != want) {
				t.Fatalf("Get(%v): got (%d, %v), want (%d, %v)", o.Key, got, ok, want, present)
			}
		case OpInsert:
			_, present := model[o.Key]
			_, ok := m.Insert(o.Key, o.Value)
			if ok == present {
				t.Fatalf("Insert(%v) ok=%v but model presence=%v", o.Key, ok, present)
			}
			if !present {
				model[o.Key] = o.Value
			}
		case OpPut:
			_, present := model[o.Key]
			_, ok := m.Put(o.Key, o.Value)
			if ok != present {
				t.Fatalf("Put(%v) ok=%v but model presence=%v", o.Key, ok, present)
			}
			if present {
				model[o.Key] = o.Value
			}
		case OpDelete:
			_, present := model[o.Key]
			_, ok := m.Delete(o.Key)
			if ok != present {
				t.Fatalf("Delete(%v) ok=%v but model presence=%v", o.Key, ok, present)
			}
			delete(model, o.Key)
		}
	}

	got, dups := collectRangeStrict(m)
	if dups > 0 {
		t.Fatalf("Range emitted %d duplicate key(s)", dups)
	}
	if !maps.Equal(got, model) {
		t.Fatalf("Range output != model:\n  got:   %v\n  model: %v", got, model)
	}
}

// collectRangeStrict returns the (k, v) pairs and the count of duplicate-key emissions.
func collectRangeStrict[K dlht.Key](m *dlht.Map[K, int]) (map[K]int, int) {
	out := map[K]int{}
	dups := 0
	m.Range(func(k K, v int) bool {
		if _, seen := out[k]; seen {
			dups++
		}
		out[k] = v
		return true
	})
	return out, dups
}

func TestPBTRangeSequentialModel(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		keyspace := rapid.IntRange(1, 64).Draw(t, "keyspace")
		runRangeSequentialModel(t, GenStringKey(keyspace))
	})
}

func TestPBTRangeSequentialModelUint64(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		keyspace := rapid.IntRange(1, 64).Draw(t, "keyspace")
		runRangeSequentialModel(t, GenUint64Key(keyspace))
	})
}

// Yield returning false on the N-th call yields exactly min(N, total entries).
func TestPBTRangeStopSemantics(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		numKeys := rapid.IntRange(0, 100).Draw(t, "numKeys")
		stopAfter := rapid.IntRange(1, numKeys+1).Draw(t, "stopAfter")

		m := dlht.New[uint64, int](dlht.Options{InitialSize: 16})
		for i := range numKeys {
			m.Insert(uint64(i), i)
		}

		count := 0
		m.Range(func(_ uint64, _ int) bool {
			count++
			return count < stopAfter
		})

		if want := min(stopAfter, numKeys); count != want {
			t.Fatalf("yielded %d, want %d (stopAfter=%d, numKeys=%d)", count, want, stopAfter, numKeys)
		}
	})
}

// After a random op sequence, Range, All, Keys, and Values agree on the
// contents and none of them emit any key more than once.
func TestPBTRangeAllKeysValuesConsistency(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		keyspace := rapid.IntRange(1, 32).Draw(t, "keyspace")
		opsLen := rapid.IntRange(0, 128).Draw(t, "opsLen")
		keyGen := GenStringKey(keyspace)
		valGen := rapid.IntRange(-1_000_000, 1_000_000)
		ops := GenOpSequence(keyGen, valGen, MixBalanced, opsLen, opsLen).Draw(t, "ops")

		m := dlht.New[string, int](dlht.Options{InitialSize: 16})
		for _, o := range ops {
			switch o.Kind {
			case OpInsert:
				m.Insert(o.Key, o.Value)
			case OpPut:
				m.Put(o.Key, o.Value)
			case OpDelete:
				m.Delete(o.Key)
			}
		}

		rangeMap, rangeDups := collectRangeStrict(m)
		if rangeDups > 0 {
			t.Fatalf("Range emitted %d duplicate key(s)", rangeDups)
		}

		allMap, allDups := collectAllStrict(m)
		if allDups > 0 {
			t.Fatalf("All emitted %d duplicate key(s)", allDups)
		}
		if !maps.Equal(allMap, rangeMap) {
			t.Fatalf("All / Range disagree:\n  All:   %v\n  Range: %v", allMap, rangeMap)
		}

		gotKeys := slices.Sorted(m.Keys())
		wantKeys := slices.Sorted(maps.Keys(rangeMap))
		if !slices.Equal(gotKeys, wantKeys) {
			t.Fatalf("Keys / Range disagree:\n  Keys:  %v\n  Range: %v", gotKeys, wantKeys)
		}

		gotValues := slices.Sorted(m.Values())
		wantValues := slices.Sorted(maps.Values(rangeMap))
		if !slices.Equal(gotValues, wantValues) {
			t.Fatalf("Values / Range disagree:\n  Values: %v\n  Range:  %v", gotValues, wantValues)
		}
	})
}

func collectAllStrict(m *dlht.Map[string, int]) (map[string]int, int) {
	out := map[string]int{}
	dups := 0
	for k, v := range m.All() {
		if _, seen := out[k]; seen {
			dups++
		}
		out[k] = v
	}
	return out, dups
}

// Concurrent Put writers vs Range: every (k, v) Range emits matches a value
// the writer recorded for k, and no key appears twice within one Range.
func TestPBTRangePerEntryValidityConcurrentPut(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		numKeys := rapid.IntRange(2, 50).Draw(t, "numKeys")
		numWriters := rapid.IntRange(1, 4).Draw(t, "numWriters")
		numRanges := rapid.IntRange(5, 30).Draw(t, "numRanges")

		m := dlht.New[uint64, uint64](dlht.Options{InitialSize: 16})

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

		for k := range numKeys {
			v := uint64(k) * 1000
			m.Insert(uint64(k), v)
			record(uint64(k), v)
		}

		stop := make(chan struct{})
		var wg sync.WaitGroup
		for w := range numWriters {
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
					k := counter % uint64(numKeys)
					// Record before Put.
					record(k, counter)
					m.Put(k, counter)
					counter++
				}
			}(uint64(w))
		}

		for range numRanges {
			seen := make(map[uint64]struct{}, numKeys)
			m.Range(func(k, v uint64) bool {
				if !knew(k, v) {
					t.Errorf("emitted unknown (k=%d, v=%d)", k, v)
				}
				if _, dup := seen[k]; dup {
					t.Errorf("duplicate key %d in one Range", k)
				}
				seen[k] = struct{}{}
				return true
			})
		}

		close(stop)
		wg.Wait()
	})
}
