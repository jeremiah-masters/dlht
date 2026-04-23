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
