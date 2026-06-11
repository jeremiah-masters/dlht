package tests

import (
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/jeremiah-masters/dlht"
)

// Smoke tests for concurrent operations. Each test isolates one specific
// operation pattern so a failure immediately identifies what regressed.
// Deep coverage lives in the PBT and linearizability suites.

func TestConcurrentInsertUniqueKeys(t *testing.T) {
	// Each goroutine inserts its own keys. No contention on individual keys,
	// so every insert must succeed and every key must be readable afterward.
	// Failure here means basic insert or resize is broken.
	m := dlht.New[string, int](dlht.Options{InitialSize: 4})

	var wg sync.WaitGroup
	for w := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 500 {
				key := fmt.Sprintf("%d/%d", w, i)
				if _, ok := m.Insert(key, w*500+i); !ok {
					t.Errorf("Insert(%s) failed on unique key", key)
				}
			}
		}()
	}
	wg.Wait()

	for w := range 8 {
		for i := range 500 {
			key := fmt.Sprintf("%d/%d", w, i)
			val, ok := m.Get(key)
			if !ok {
				t.Errorf("Get(%s): missing after insert", key)
			} else if val != w*500+i {
				t.Errorf("Get(%s) = %d, want %d", key, val, w*500+i)
			}
		}
	}
}

func TestConcurrentInsertDuplicate(t *testing.T) {
	// N goroutines race to insert the same keys. Exactly one must win per key.
	// Failure here means the header-CAS reservation has a bug.
	m := dlht.New[string, int](dlht.Options{InitialSize: 8})

	const numKeys = 10
	var wins [numKeys]atomic.Int32

	var wg sync.WaitGroup
	for w := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 1000 {
				idx := rand.IntN(numKeys)
				if _, ok := m.Insert(fmt.Sprintf("k%d", idx), w*1000+i); ok {
					wins[idx].Add(1)
				}
			}
		}()
	}
	wg.Wait()

	for i := range numKeys {
		if n := wins[i].Load(); n != 1 {
			t.Errorf("key k%d: %d winners, want 1", i, n)
		}
	}
}

func TestConcurrentPut(t *testing.T) {
	// All goroutines Put to the same keys. After quiescence, each key's value
	// must be one that some goroutine actually wrote.
	// Failure here means DWCAS (the Put path) is corrupting values.
	m := dlht.New[string, int](dlht.Options{InitialSize: 4})

	const numKeys = 4
	for i := range numKeys {
		m.Insert(fmt.Sprintf("k%d", i), -1) // seed with sentinel
	}

	// Each goroutine writes values in [workerID*1000, workerID*1000+1000).
	var wg sync.WaitGroup
	for w := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 1000 {
				idx := rand.IntN(numKeys)
				m.Put(fmt.Sprintf("k%d", idx), w*1000+i)
			}
		}()
	}
	wg.Wait()

	for i := range numKeys {
		key := fmt.Sprintf("k%d", i)
		val, ok := m.Get(key)
		if !ok {
			t.Errorf("Get(%s): missing", key)
		} else if val < 0 || val >= 8*1000 {
			t.Errorf("Get(%s) = %d, not in any worker's write range [0, 8000)", key, val)
		}
	}
}

func TestConcurrentDeleteThenVerifyAbsent(t *testing.T) {
	// Insert keys, then delete them all concurrently. After quiescence every
	// key must be gone. Failure means Delete's header CAS isn't sticking.
	m := dlht.New[string, int](dlht.Options{InitialSize: 16})

	const numKeys = 200
	for i := range numKeys {
		m.Insert(fmt.Sprintf("k%d", i), i)
	}

	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range numKeys {
				m.Delete(fmt.Sprintf("k%d", i))
			}
		}()
	}
	wg.Wait()

	for i := range numKeys {
		key := fmt.Sprintf("k%d", i)
		if _, ok := m.Get(key); ok {
			t.Errorf("Get(%s): still present after concurrent delete", key)
		}
	}
}

func TestPutDeleteInteraction(t *testing.T) {
	// Put (DWCAS on slot) vs Delete (header CAS) on the same small key set.
	// Inserters replenish so there's always work. If the interplay is wrong
	// this will panic, hang, or leave corrupted values.
	m := dlht.New[string, int](dlht.Options{InitialSize: 4})

	const numKeys = 5
	for i := range numKeys {
		m.Insert(fmt.Sprintf("k%d", i), 0)
	}

	const ops = 2000
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(3)
		go func() {
			defer wg.Done()
			for i := range ops {
				m.Put(fmt.Sprintf("k%d", rand.IntN(numKeys)), i)
			}
		}()
		go func() {
			defer wg.Done()
			for range ops {
				m.Delete(fmt.Sprintf("k%d", rand.IntN(numKeys)))
			}
		}()
		go func() {
			defer wg.Done()
			for i := range ops {
				m.Insert(fmt.Sprintf("k%d", rand.IntN(numKeys)), i)
			}
		}()
	}
	wg.Wait()

	for i := range numKeys {
		key := fmt.Sprintf("k%d", i)
		if val, ok := m.Get(key); ok && (val < 0 || val >= ops) {
			t.Errorf("Get(%s) = %d, outside written range [0, %d)", key, val, ops)
		}
	}
}

func TestConcurrentLoadOrComputeUniqueKeys(t *testing.T) {
	m := dlht.New[string, int](dlht.Options{InitialSize: 4})

	var wg sync.WaitGroup
	for w := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 500 {
				key := fmt.Sprintf("%d/%d", w, i)
				val, loaded := m.LoadOrCompute(key, func() (int, bool) { return w*500 + i, true })
				if loaded {
					t.Errorf("LoadOrCompute(%s) loaded on unique key", key)
				}
				if val != w*500+i {
					t.Errorf("LoadOrCompute(%s) = %d, want %d", key, val, w*500+i)
				}
			}
		}()
	}
	wg.Wait()

	for w := range 8 {
		for i := range 500 {
			key := fmt.Sprintf("%d/%d", w, i)
			val, ok := m.Get(key)
			if !ok {
				t.Errorf("Get(%s): missing after LoadOrCompute", key)
			} else if val != w*500+i {
				t.Errorf("Get(%s) = %d, want %d", key, val, w*500+i)
			}
		}
	}
}

func TestConcurrentLoadOrComputeDuplicate(t *testing.T) {
	m := dlht.New[string, int](dlht.Options{InitialSize: 8})

	const numKeys = 10
	var wins [numKeys]atomic.Int32

	var wg sync.WaitGroup
	for w := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 1000 {
				idx := rand.IntN(numKeys)
				_, loaded := m.LoadOrCompute(fmt.Sprintf("k%d", idx), func() (int, bool) { return w*1000 + i, true })
				if !loaded {
					wins[idx].Add(1)
				}
			}
		}()
	}
	wg.Wait()

	for i := range numKeys {
		if n := wins[i].Load(); n != 1 {
			t.Errorf("key k%d: %d computes, want 1", i, n)
		}
	}
}

func TestConcurrentLoadOrComputeFactoryCallCount(t *testing.T) {
	m := dlht.New[string, int](dlht.Options{InitialSize: 8})

	const numKeys = 5
	var fnCalls [numKeys]atomic.Int32

	var wg sync.WaitGroup
	for w := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range numKeys {
				m.LoadOrCompute(fmt.Sprintf("k%d", i), func() (int, bool) {
					fnCalls[i].Add(1)
					return w*100 + i, true
				})
			}
		}()
	}
	wg.Wait()

	for i := range numKeys {
		calls := fnCalls[i].Load()
		if calls < 1 {
			t.Errorf("key k%d: fn called %d times, want >= 1", i, calls)
		}
		key := fmt.Sprintf("k%d", i)
		if _, ok := m.Get(key); !ok {
			t.Errorf("Get(%s): missing", key)
		}
	}
}

func TestLoadOrComputeResizeInteraction(t *testing.T) {
	m := dlht.New[string, int](dlht.Options{InitialSize: 2})

	const keysPerWorker = 500
	var wg sync.WaitGroup
	for w := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range keysPerWorker {
				m.LoadOrCompute(fmt.Sprintf("%d/%d", w, i), func() (int, bool) { return w*keysPerWorker + i, true })
			}
		}()
	}
	wg.Wait()

	missing := 0
	for w := range 8 {
		for i := range keysPerWorker {
			key := fmt.Sprintf("%d/%d", w, i)
			if _, ok := m.Get(key); !ok {
				missing++
				if missing <= 5 {
					t.Errorf("Get(%s): missing after resize", key)
				}
			}
		}
	}
	if missing > 5 {
		t.Errorf("... and %d more missing keys", missing-5)
	}
}

func TestResizePreservesKeys(t *testing.T) {
	// Start tiny (2 bins) and insert enough unique keys to force several
	// resizes. Every key must survive. Failure means resize dropped entries.
	m := dlht.New[string, int](dlht.Options{InitialSize: 2})

	const keysPerWorker = 500
	var wg sync.WaitGroup
	for w := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range keysPerWorker {
				m.Insert(fmt.Sprintf("%d/%d", w, i), w*keysPerWorker+i)
			}
		}()
	}
	wg.Wait()

	missing := 0
	for w := range 8 {
		for i := range keysPerWorker {
			key := fmt.Sprintf("%d/%d", w, i)
			if _, ok := m.Get(key); !ok {
				missing++
				if missing <= 5 {
					t.Errorf("Get(%s): missing after resize", key)
				}
			}
		}
	}
	if missing > 5 {
		t.Errorf("... and %d more missing keys", missing-5)
	}
}

func TestConcurrentLoadOrComputeOnceUniqueKeys(t *testing.T) {
	m := dlht.New[string, int](dlht.Options{InitialSize: 4})

	var wg sync.WaitGroup
	for w := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 500 {
				key := fmt.Sprintf("%d/%d", w, i)
				val, loaded := m.LoadOrComputeOnce(key, func() (int, bool) { return w*500 + i, true })
				if loaded {
					t.Errorf("LoadOrComputeOnce(%s) loaded on unique key", key)
				}
				if val != w*500+i {
					t.Errorf("LoadOrComputeOnce(%s) = %d, want %d", key, val, w*500+i)
				}
			}
		}()
	}
	wg.Wait()

	for w := range 8 {
		for i := range 500 {
			key := fmt.Sprintf("%d/%d", w, i)
			val, ok := m.Get(key)
			if !ok {
				t.Errorf("Get(%s): missing after LoadOrComputeOnce", key)
			} else if val != w*500+i {
				t.Errorf("Get(%s) = %d, want %d", key, val, w*500+i)
			}
		}
	}
}

func TestConcurrentLoadOrComputeOnceDuplicate(t *testing.T) {
	m := dlht.New[string, int](dlht.Options{InitialSize: 8})

	const numKeys = 10
	var wins [numKeys]atomic.Int32

	var wg sync.WaitGroup
	for w := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 1000 {
				idx := rand.IntN(numKeys)
				_, loaded := m.LoadOrComputeOnce(fmt.Sprintf("k%d", idx), func() (int, bool) { return w*1000 + i, true })
				if !loaded {
					wins[idx].Add(1)
				}
			}
		}()
	}
	wg.Wait()

	for i := range numKeys {
		if n := wins[i].Load(); n != 1 {
			t.Errorf("key k%d: %d computes, want 1", i, n)
		}
	}
}

func TestConcurrentLoadOrComputeOnceFactoryCallCount(t *testing.T) {
	m := dlht.New[string, int](dlht.Options{InitialSize: 8})

	const numKeys = 5
	var fnCalls [numKeys]atomic.Int32

	var wg sync.WaitGroup
	for w := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range numKeys {
				m.LoadOrComputeOnce(fmt.Sprintf("k%d", i), func() (int, bool) {
					fnCalls[i].Add(1)
					return w*100 + i, true
				})
			}
		}()
	}
	wg.Wait()

	for i := range numKeys {
		calls := fnCalls[i].Load()
		if calls != 1 {
			t.Errorf("key k%d: fn called %d times, want exactly 1", i, calls)
		}
		key := fmt.Sprintf("k%d", i)
		if _, ok := m.Get(key); !ok {
			t.Errorf("Get(%s): missing", key)
		}
	}
}

func TestLoadOrComputeSaveFalse(t *testing.T) {
	m := dlht.New[string, int](dlht.Options{})

	// save=false should never store
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 100 {
				key := fmt.Sprintf("k%d", i)
				v, loaded := m.LoadOrCompute(key, func() (int, bool) { return 999, false })
				if loaded {
					t.Errorf("LoadOrCompute(%s) loaded=true, want false", key)
				}
				if v != 999 {
					t.Errorf("LoadOrCompute(%s) = %d, want 999", key, v)
				}
			}
		}()
	}
	wg.Wait()

	for i := range 100 {
		if m.Contains(fmt.Sprintf("k%d", i)) {
			t.Errorf("key k%d should not exist after save=false", i)
		}
	}
}

func TestLoadOrComputeOnceSaveFalseRetry(t *testing.T) {
	// First N-1 callers return save=false. The last one returns save=true.
	// Verify the value eventually appears.
	m := dlht.New[string, int](dlht.Options{})

	var calls atomic.Int32
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			m.LoadOrComputeOnce("key", func() (int, bool) {
				n := calls.Add(1)
				if n < 8 {
					return 0, false // decline to save
				}
				return 42, true // last caller saves
			})
		}()
	}
	wg.Wait()

	v, ok := m.Get("key")
	if !ok {
		t.Fatal("key should exist after one caller returned save=true")
	}
	if v != 42 {
		t.Errorf("got %d, want 42", v)
	}
}

func TestLoadOrComputeOncePanicRecovery(t *testing.T) {
	// When the winner's fn panics, waiters must not hang — they should
	// retry and eventually become the winner themselves.
	m := dlht.New[string, int](dlht.Options{})

	var attempts atomic.Int32
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { recover() }()
			m.LoadOrComputeOnce("key", func() (int, bool) {
				attempts.Add(1)
				panic("boom")
			})
		}()
	}
	wg.Wait()

	// Every goroutine should have become a winner (and panicked).
	// None should have hung waiting on a dead channel.
	if n := attempts.Load(); n != 8 {
		t.Errorf("fn called %d times, want 8 (all goroutines should retry and win)", n)
	}
}

func TestLoadOrComputeOnceDeleteDuringCompute(t *testing.T) {
	// If the computed value is deleted before a waiter reads it,
	// the waiter must retry and re-compute — not return zero.
	m := dlht.New[string, int](dlht.Options{})

	started := make(chan struct{})
	deleted := make(chan struct{})

	// Goroutine 1: compute, signal, wait for delete, then let LoadOrComputeOnce return
	go func() {
		m.LoadOrComputeOnce("key", func() (int, bool) {
			close(started) // signal that we're computing
			<-deleted      // wait for the delete to happen
			return 42, true
		})
	}()

	<-started // winner is inside fn(), value not yet inserted

	// Goroutine 2: will become a waiter since the winner holds the compute slot
	var result int
	var loaded bool
	done := make(chan struct{})
	go func() {
		defer close(done)
		result, loaded = m.LoadOrComputeOnce("key", func() (int, bool) { return 99, true })
	}()

	// Let the winner finish — it inserts 42, closes channel, deletes from compute map
	close(deleted)

	// Now delete the key from the main map
	// There's a race: the waiter might read before or after the delete.
	m.Delete("key")

	<-done

	// The waiter should have either:
	// a) Read 42 before the delete (loaded=true, result=42), or
	// b) Retried after delete and computed 99 (loaded=false, result=99)
	// It must NOT return (0, false) — that would mean it silently returned zero.
	if result == 0 {
		t.Errorf("got (0, %v), waiter should have retried or read the value", loaded)
	}
}

func TestLoadOrComputeOnceResizeInteraction(t *testing.T) {
	m := dlht.New[string, int](dlht.Options{InitialSize: 2})

	const keysPerWorker = 500
	var wg sync.WaitGroup
	for w := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range keysPerWorker {
				m.LoadOrComputeOnce(fmt.Sprintf("%d/%d", w, i), func() (int, bool) { return w*keysPerWorker + i, true })
			}
		}()
	}
	wg.Wait()

	missing := 0
	for w := range 8 {
		for i := range keysPerWorker {
			key := fmt.Sprintf("%d/%d", w, i)
			if _, ok := m.Get(key); !ok {
				missing++
				if missing <= 5 {
					t.Errorf("Get(%s): missing after resize", key)
				}
			}
		}
	}
	if missing > 5 {
		t.Errorf("... and %d more missing keys", missing-5)
	}
}
