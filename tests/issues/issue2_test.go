package dlht_issues_tests

/*
Issue 2: Put/Delete linearizability gap.

Put swaps slot content with DWCASPtr, while Delete linearizes through header
CAS. Those touch different memory, so both can succeed in an order that has no
valid linearization.

Observed failure paths:
- Delete returns a stale old value because Put does not bump header version.
- Delete cleanup can follow slot reuse and return a value written later.
*/

import (
	"runtime"
	"sync"
	"testing"

	"github.com/jeremiah-masters/dlht"
)

func resetKeyToOldValue(m *dlht.Map[string, int], key string, oldValue int) bool {
	if _, ok := m.Put(key, oldValue); ok {
		return true
	}
	if _, ok := m.Insert(key, oldValue); ok {
		return true
	}
	_, ok := m.Put(key, oldValue)
	return ok
}

func runPutDeleteInsertRace(m *dlht.Map[string, int], key string, putValue int, insertValue int) (int, bool, int, bool) {
	start := make(chan struct{})
	var wg sync.WaitGroup

	var putOld, delOld int
	var putUpdated, delFound bool

	wg.Add(3)

	go func() {
		defer wg.Done()
		<-start
		putOld, putUpdated = m.Put(key, putValue)
	}()

	go func() {
		defer wg.Done()
		<-start
		delOld, delFound = m.Delete(key)
	}()

	// Keep reinserting while Delete cleanup runs to widen the race window.
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 32; i++ {
			if _, ok := m.Insert(key, insertValue); ok {
				return
			}
			runtime.Gosched()
		}
	}()

	close(start)
	wg.Wait()
	return putOld, putUpdated, delOld, delFound
}

// Starting from key=oldValue, this outcome is impossible in a linearizable map:
//
//	Put(key, new)    -> (oldValue, updated=true)
//	Delete(key)      -> (oldValue, found=true)
//
// If Put linearizes first, Delete must return new.
// If Delete linearizes first, Put must report updated=false.
// Fail fast if that forbidden outcome appears.
func TestIssue2_PutDeleteCannotBothReturnSameOld(t *testing.T) {
	attempts := 3
	rounds := 100000
	if testing.Short() {
		attempts = 1
		rounds = 10000
	}

	const (
		key      = "hot"
		oldValue = -1
	)

	for attempt := 0; attempt < attempts; attempt++ {
		m := dlht.New[string, int](dlht.Options{InitialSize: 4})
		for round := 0; round < rounds; round++ {
			if !resetKeyToOldValue(m, key, oldValue) {
				t.Fatalf("attempt %d round %d: failed to reset key", attempt, round)
			}

			putValue := round*2 + 1
			insertValue := round*2 + 2

			putOld, putUpdated, delOld, delFound := runPutDeleteInsertRace(m, key, putValue, insertValue)

			if putUpdated && delFound && putOld == oldValue && delOld == oldValue {
				t.Fatalf(
					"attempt %d round %d: forbidden outcome: Put(updated=true, old=%d) and Delete(found=true, old=%d)",
					attempt,
					round,
					putOld,
					delOld,
				)
			}
		}
	}
}
