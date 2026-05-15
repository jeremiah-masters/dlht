package tests

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/jeremiah-masters/dlht"
	"github.com/jeremiah-masters/dlht/inline"
)

func TestInlineLoadOrCompute(t *testing.T) {
	t.Run("All", func(t *testing.T) {
		m := dlht.NewInline[uint64](inline.Options{})

		for i := range uint64(128) {
			v, loaded := m.LoadOrCompute(i, func() (uint64, bool) { return i * 10, true })
			if loaded {
				t.Errorf("key %d: expected computed, got loaded", i)
			}
			if v != i*10 {
				t.Errorf("key %d: got %d, want %d", i, v, i*10)
			}
			// Second call should load
			v, loaded = m.LoadOrCompute(i, func() (uint64, bool) { return 999, true })
			if !loaded {
				t.Errorf("key %d: expected loaded, got computed", i)
			}
			if v != i*10 {
				t.Errorf("key %d: loaded %d, want %d", i, v, i*10)
			}
		}
	})

	t.Run("ConcurrentSharedKeys", func(t *testing.T) {
		m := dlht.NewInline[uint64](inline.Options{})

		const numKeys = 8
		var wins [numKeys]atomic.Int32
		gmp := runtime.GOMAXPROCS(0)
		var wg sync.WaitGroup
		for i := range gmp {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				for j := range numKeys {
					_, loaded := m.LoadOrCompute(uint64(j), func() (uint64, bool) { return uint64(id), true })
					if !loaded {
						wins[j].Add(1)
					}
				}
			}(i)
		}
		wg.Wait()

		for i := range numKeys {
			if n := wins[i].Load(); n != 1 {
				t.Errorf("key %d: %d computes, want 1", i, n)
			}
		}
	})
}

func TestInlineLoadOrComputeOnce(t *testing.T) {
	t.Run("All", func(t *testing.T) {
		m := dlht.NewInline[uint64](inline.Options{})

		for i := range uint64(128) {
			v, loaded := m.LoadOrComputeOnce(i, func() (uint64, bool) { return i * 10, true })
			if loaded {
				t.Errorf("key %d: expected computed, got loaded", i)
			}
			if v != i*10 {
				t.Errorf("key %d: got %d, want %d", i, v, i*10)
			}
			v, loaded = m.LoadOrComputeOnce(i, func() (uint64, bool) { return 999, true })
			if !loaded {
				t.Errorf("key %d: expected loaded, got computed", i)
			}
			if v != i*10 {
				t.Errorf("key %d: loaded %d, want %d", i, v, i*10)
			}
		}
	})

	t.Run("ConcurrentSharedKeys", func(t *testing.T) {
		m := dlht.NewInline[uint64](inline.Options{})

		const numKeys = 8
		var wins [numKeys]atomic.Int32
		gmp := runtime.GOMAXPROCS(0)
		var wg sync.WaitGroup
		for i := range gmp {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				for j := range numKeys {
					_, loaded := m.LoadOrComputeOnce(uint64(j), func() (uint64, bool) { return uint64(id), true })
					if !loaded {
						wins[j].Add(1)
					}
				}
			}(i)
		}
		wg.Wait()

		for i := range numKeys {
			if n := wins[i].Load(); n != 1 {
				t.Errorf("key %d: %d computes, want 1", i, n)
			}
		}
	})

	t.Run("FnCalledOnce", func(t *testing.T) {
		m := dlht.NewInline[uint64](inline.Options{})

		const numKeys = 8
		var fnCalls [numKeys]atomic.Int32
		gmp := runtime.GOMAXPROCS(0)
		var wg sync.WaitGroup
		for i := range gmp {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				for j := range numKeys {
					m.LoadOrComputeOnce(uint64(j), func() (uint64, bool) {
						fnCalls[j].Add(1)
						return uint64(id), true
					})
				}
			}(i)
		}
		wg.Wait()

		for i := range numKeys {
			if calls := fnCalls[i].Load(); calls != 1 {
				t.Errorf("key %d: fn called %d times, want exactly 1", i, calls)
			}
		}
	})

	t.Run("ResizeInteraction", func(t *testing.T) {
		m := dlht.NewInline[uint64](inline.Options{InitialSize: 2})

		const keysPerWorker = 500
		var wg sync.WaitGroup
		for w := range 8 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := range keysPerWorker {
					key := uint64(w*keysPerWorker + i)
					m.LoadOrComputeOnce(key, func() (uint64, bool) { return key * 10, true })
				}
			}()
		}
		wg.Wait()

		missing := 0
		for w := range 8 {
			for i := range keysPerWorker {
				key := uint64(w*keysPerWorker + i)
				if _, ok := m.Get(key); !ok {
					missing++
					if missing <= 5 {
						t.Errorf("Get(%d): missing after resize", key)
					}
				}
			}
		}
		if missing > 5 {
			t.Errorf("... and %d more missing keys", missing-5)
		}
	})
}

func TestConcurrentInlineLoadOrComputeOnceFactoryCallCount(t *testing.T) {
	m := dlht.NewInline[uint64](inline.Options{InitialSize: 8})

	const numKeys = 5
	var fnCalls [numKeys]atomic.Int32

	var wg sync.WaitGroup
	for w := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range numKeys {
				m.LoadOrComputeOnce(uint64(i), func() (uint64, bool) {
					fnCalls[i].Add(1)
					return uint64(w*100 + i), true
				})
			}
		}()
	}
	wg.Wait()

	for i := range numKeys {
		calls := fnCalls[i].Load()
		if calls != 1 {
			t.Errorf("key %d: fn called %d times, want exactly 1", i, calls)
		}
		if _, ok := m.Get(uint64(i)); !ok {
			t.Errorf("Get(%d): missing", i)
		}
	}
}

