package dlht_issues_tests

/*
Issue 3: Put TOCTOU can read a value from a failed Insert.

Put validates slot state via header, then later does DWCASPtr on slot content.
If Delete+Insert recycles the slot in between, Put can match slot content in a
Trying slot and return that value even though Insert never finalized.

This test drives heavy Delete/Insert/Put contention on one key to expose that
window.
*/

import (
	"runtime"
	"sync"
	"testing"

	"github.com/jeremiah-masters/dlht"
)

// opResult records one Insert or Put for later phantom-value checks.
type opResult struct {
	op      byte // 'i' or 'p'
	val     int
	ok      bool // Op succeeded/updated
	prevVal int
}

// Each worker loops Delete/Insert/Put on the same key.
// Values are unique per (worker, op).
func hammerDeleteInsertPut(m *dlht.Map[string, int], key string, workers, opsPerWorker, vbase int) [][]opResult {
	results := make([][]opResult, workers)
	gate := make(chan struct{})
	var wg sync.WaitGroup

	for w := range workers {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			<-gate

			buf := make([]opResult, 0, opsPerWorker*2/3)
			off := vbase + id*opsPerWorker + 1

			for i := range opsPerWorker {
				v := off + i
				switch i % 3 {
				case 0:
					m.Delete(key)
				case 1:
					_, ok := m.Insert(key, v)
					buf = append(buf, opResult{'i', v, ok, 0})
				case 2:
					old, ok := m.Put(key, v)
					buf = append(buf, opResult{'p', v, ok, old})
				}
				if i%7 == 0 {
					runtime.Gosched()
				}
			}
			results[id] = buf
		}(w)
	}

	close(gate)
	wg.Wait()
	return results
}

// Returns the first Put old value that no successful Insert/Put wrote.
// If found, it likely came from a transient Trying slot.
func findPhantom(results [][]opResult) (phantom, worker, putVal int, found bool) {
	written := make(map[int]bool)
	for _, rs := range results {
		for _, r := range rs {
			if r.ok {
				written[r.val] = true
			}
		}
	}
	for wid, rs := range results {
		for _, r := range rs {
			if r.op == 'p' && r.ok && !written[r.prevVal] {
				return r.prevVal, wid, r.val, true
			}
		}
	}
	return 0, 0, 0, false
}

// Detect TOCTOU: Put should not return an old value that no successful
// Insert or Put ever wrote.
func TestIssue3_PutReadsPhantomFromFailedInsert(t *testing.T) {
	rounds := 2000
	opsPerWorker := 300
	if testing.Short() {
		rounds = 200
		opsPerWorker = 150
	}

	workers := max(runtime.GOMAXPROCS(0), 4)

	for round := range rounds {
		m := dlht.New[string, int](dlht.Options{InitialSize: 4})

		rs := hammerDeleteInsertPut(m, "k", workers, opsPerWorker, round*workers*opsPerWorker)

		if phantom, wid, pv, ok := findPhantom(rs); ok {
			t.Fatalf("round %d worker %d: Put(\"k\", %d) returned old=%d, "+
				"but no successful Insert or Put ever wrote %d",
				round, wid, pv, phantom, phantom)
		}
	}
}
