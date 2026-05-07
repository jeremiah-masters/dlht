package inline

import (
	"hash/maphash"
	"maps"
	"slices"
	"sync"
	"testing"
)

// testSeed is fresh per process but stable for the lifetime of the test
// binary. Tests that depend on bin placement use getBin() rather than literal
// hashes, so non-determinism across runs is fine.
var testSeed = maphash.MakeSeed()

// newWithSeed mirrors New but injects an explicit maphash.Seed.
func newWithSeed[V Integer](opts Options, seed maphash.Seed) *Map[V] {
	if opts.InitialSize == 0 {
		opts.InitialSize = 16
	}
	even, odd := findSentinels(seed)
	m := &Map[V]{hashConfig: HashConfig{
		Seed:                seed,
		SentinelForEvenBins: even,
		SentinelForOddBins:  odd,
	}}
	m.active.Store(newIndex[V](nextPowerOf2(opts.InitialSize)))
	return m
}

// keysForBin returns count distinct keys that all land in binIdx under seed.
// Probes up to maxProbe candidates; nil if not enough collisions are found.
func keysForBin(seed maphash.Seed, mask, binIdx uint64, count int, maxProbe uint64) []uint64 {
	if binIdx&^mask != 0 {
		panic("keysForBin: binIdx out of range for mask")
	}
	out := make([]uint64, 0, count)
	for k := uint64(1); k < maxProbe && len(out) < count; k++ {
		if maphash.Comparable(seed, k)&mask == binIdx {
			out = append(out, k)
		}
	}
	if len(out) < count {
		return nil
	}
	return out
}

// collectRange runs Range and returns the emitted (k, v) pairs as a map. It
// fails the test if any key is emitted more than once; without that check,
// maps.Equal would happily accept a Range that emits a key twice with the
// final value being the expected one.
func collectRange[V Integer](t *testing.T, m *Map[V]) map[uint64]V {
	t.Helper()
	out := map[uint64]V{}
	m.Range(func(k uint64, v V) bool {
		if _, seen := out[k]; seen {
			t.Fatalf("Range emitted duplicate key %d", k)
		}
		out[k] = v
		return true
	})
	return out
}

// expectMap returns {1:10, 2:20, ..., n:n*10}.
func expectMap(n uint64) map[uint64]uint64 {
	want := make(map[uint64]uint64, n)
	for k := uint64(1); k <= n; k++ {
		want[k] = k * 10
	}
	return want
}

// expectMapKeys returns {keys[i]:keys[i]*10, ...}.
func expectMapKeys(keys []uint64) map[uint64]uint64 {
	want := make(map[uint64]uint64, len(keys))
	for _, k := range keys {
		want[k] = k * 10
	}
	return want
}

// insertSequential inserts keys 1..n with values k*10.
func insertSequential(t *testing.T, m *Map[uint64], n uint64) {
	t.Helper()
	for k := uint64(1); k <= n; k++ {
		if _, ok := m.Insert(k, k*10); !ok {
			t.Fatalf("Insert(%d) failed", k)
		}
	}
}

// insertKeys inserts each given key with value k*10.
func insertKeys(t *testing.T, m *Map[uint64], keys []uint64) {
	t.Helper()
	for _, k := range keys {
		if _, ok := m.Insert(k, k*10); !ok {
			t.Fatalf("Insert(%d) failed", k)
		}
	}
}

func TestRange_EmptyMap(t *testing.T) {
	m := newWithSeed[uint64](Options{InitialSize: 16}, testSeed)
	if got := collectRange(t, m); len(got) != 0 {
		t.Errorf("expected empty, got %v", got)
	}
}

func TestRange_SinglePrimaryEntry(t *testing.T) {
	m := newWithSeed[uint64](Options{InitialSize: 16}, testSeed)
	m.Insert(42, 100)
	if got, want := collectRange(t, m), map[uint64]uint64{42: 100}; !maps.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestRange_FullPrimaryBucket(t *testing.T) {
	m := newWithSeed[uint64](Options{InitialSize: 16}, testSeed)
	keys := keysForBin(testSeed, m.getActiveIndex().mask, 0, 3, 1_000_000)
	if keys == nil {
		t.Fatal("could not find 3 keys hashing to bin 0")
	}
	insertKeys(t, m, keys)
	if got, want := collectRange(t, m), expectMapKeys(keys); !maps.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestRange_LinkBucketEntries(t *testing.T) {
	m := newWithSeed[uint64](Options{InitialSize: 16}, testSeed)
	keys := keysForBin(testSeed, m.getActiveIndex().mask, 0, 7, 5_000_000)
	if keys == nil {
		t.Fatal("could not find 7 keys hashing to bin 0")
	}
	insertKeys(t, m, keys)
	if got, want := collectRange(t, m), expectMapKeys(keys); !maps.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestRange_FullyChainedBucket(t *testing.T) {
	// InitialSize 32 gives linkCapacity 4, which fits 1 single + 1 pair link
	// bucket without resize. Anything smaller resizes mid-insert and the keys
	// rehash to other bins.
	m := newWithSeed[uint64](Options{InitialSize: 32}, testSeed)
	idx := m.getActiveIndex()
	keys := keysForBin(testSeed, idx.mask, 0, 15, 50_000_000)
	if keys == nil {
		t.Fatal("could not find 15 keys hashing to bin 0")
	}
	insertKeys(t, m, keys)
	if m.getActiveIndex() != idx {
		t.Fatal("resize fired during insertion: InitialSize too small")
	}
	if got, want := collectRange(t, m), expectMapKeys(keys); !maps.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestRange_StopOnFalse(t *testing.T) {
	m := newWithSeed[uint64](Options{InitialSize: 16}, testSeed)
	insertSequential(t, m, 10)
	count := 0
	m.Range(func(_, _ uint64) bool {
		count++
		return false
	})
	if count != 1 {
		t.Errorf("expected 1 yield, got %d", count)
	}
}

func TestRange_NoLossWhenStable(t *testing.T) {
	m := newWithSeed[uint64](Options{InitialSize: 64}, testSeed)
	insertSequential(t, m, 1000)
	if got, want := collectRange(t, m), expectMap(1000); !maps.Equal(got, want) {
		t.Errorf("got %d entries, want %d", len(got), len(want))
	}
}

// TestRange_DoneTransferShard pins the pre-resize index, triggers a resize,
// then drives rangeFrom against the old index so scanShards has to follow the
// chain to the new one.
func TestRange_DoneTransferShard(t *testing.T) {
	m := newWithSeed[uint64](Options{InitialSize: 16}, testSeed)
	insertSequential(t, m, 32)
	oldIdx := m.getActiveIndex()
	m.triggerResize()
	if m.getActiveIndex() == oldIdx {
		t.Fatal("expected resize to swap the active index")
	}
	got := map[uint64]uint64{}
	m.rangeFrom(oldIdx, func(k, v uint64) bool {
		got[k] = v
		return true
	})
	if want := expectMap(32); !maps.Equal(got, want) {
		t.Errorf("got %d entries, want %d", len(got), len(want))
	}
}

// TestRange_DoneTransferShardMultiGen covers recursive scanShards across two
// resize generations (oldest, mid, newest).
func TestRange_DoneTransferShardMultiGen(t *testing.T) {
	m := newWithSeed[uint64](Options{InitialSize: 16}, testSeed)
	insertSequential(t, m, 32)
	oldestIdx := m.getActiveIndex()
	m.triggerResize()
	midIdx := m.getActiveIndex()
	if midIdx == oldestIdx {
		t.Fatal("expected first resize to swap the active index")
	}
	m.triggerResize()
	if newest := m.getActiveIndex(); newest == midIdx || newest == oldestIdx {
		t.Fatal("expected second resize to swap the active index")
	}
	got := map[uint64]uint64{}
	m.rangeFrom(oldestIdx, func(k, v uint64) bool {
		got[k] = v
		return true
	})
	if want := expectMap(32); !maps.Equal(got, want) {
		t.Errorf("got %d entries, want %d", len(got), len(want))
	}
}

func TestRange_AllKeysValuesAgree(t *testing.T) {
	m := newWithSeed[uint64](Options{InitialSize: 16}, testSeed)
	insertSequential(t, m, 50)

	rangeKV := collectRange(t, m)
	if all := maps.Collect(m.All()); !maps.Equal(all, rangeKV) {
		t.Errorf("All / Range disagree:\n  All:   %v\n  Range: %v", all, rangeKV)
	}
	if got, want := slices.Sorted(m.Keys()), slices.Sorted(maps.Keys(rangeKV)); !slices.Equal(got, want) {
		t.Errorf("Keys / Range disagree")
	}
	if got, want := slices.Sorted(m.Values()), slices.Sorted(maps.Values(rangeKV)); !slices.Equal(got, want) {
		t.Errorf("Values / Range disagree")
	}
}

// TestRange_AllocsPerRun checks that Range itself never allocates. The
// All/Keys/Values adapters reach zero allocations only when the compiler
// inlines their closures, which doesn't happen under -N -l. Their inlining
// is a build-time property; the make iter-allocs target prints the escape
// analysis output for inspection.
func TestRange_AllocsPerRun(t *testing.T) {
	m := newWithSeed[uint64](Options{InitialSize: 64}, testSeed)
	insertSequential(t, m, 100)

	if a := testing.AllocsPerRun(50, func() {
		m.Range(func(_, _ uint64) bool { return true })
	}); a != 0 {
		t.Errorf("Range: expected 0 allocs, got %.1f", a)
	}
}

// TestRange_PerEntryValidityUnderConcurrentPut runs a writer that alternates
// Put on two same-bucket keys against a reader running Range. Every emitted
// (k, v) must be a value the writer recorded for k.
func TestRange_PerEntryValidityUnderConcurrentPut(t *testing.T) {
	m := newWithSeed[uint64](Options{InitialSize: 16}, testSeed)
	keys := keysForBin(testSeed, m.getActiveIndex().mask, 0, 2, 1_000_000)
	if keys == nil {
		t.Fatal("could not find 2 keys hashing to bin 0")
	}
	kA, kB := keys[0], keys[1]
	m.Insert(kA, 1)
	m.Insert(kB, 1)

	var mu sync.Mutex
	historyA := map[uint64]struct{}{1: {}}
	historyB := map[uint64]struct{}{1: {}}
	knew := func(h map[uint64]struct{}, v uint64) bool {
		mu.Lock()
		defer mu.Unlock()
		_, ok := h[v]
		return ok
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		next := uint64(2)
		for {
			select {
			case <-stop:
				return
			default:
			}
			// Record before Put. Otherwise Range can briefly see a value
			// the writer has published but not yet logged.
			mu.Lock()
			historyA[next] = struct{}{}
			mu.Unlock()
			m.Put(kA, next)
			next++
			mu.Lock()
			historyB[next] = struct{}{}
			mu.Unlock()
			m.Put(kB, next)
			next++
		}
	}()

	for range 1000 {
		m.Range(func(k, v uint64) bool {
			switch k {
			case kA:
				if !knew(historyA, v) {
					t.Errorf("kA emitted unknown value %d", v)
				}
			case kB:
				if !knew(historyB, v) {
					t.Errorf("kB emitted unknown value %d", v)
				}
			}
			return true
		})
	}
	close(stop)
	wg.Wait()
}
