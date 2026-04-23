package dlht_issues_tests

/*
Issue 1: stale resize context during chained resizes.

Old behavior: on BinDoneTransfer in index A, operations looked at the global
resize context and jumped to ctx.newIndex.

Race: if A->B finished and B->C had already started, ctx pointed at C, so the
operation skipped B even though data had only moved to B.

Fix: each index points to its direct successor (indexNext), and operations walk
the chain A->B->C instead of consulting global resize state.
*/

import (
	"fmt"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jeremiah-masters/dlht"
	"github.com/jeremiah-masters/dlht/tests"

	"github.com/anishathalye/porcupine"
)

// Force rapid chained resizes on a tiny table and check linearizability.
// A failure means some operation followed the wrong post-transfer index.
func TestIssue1(t *testing.T) {
	var (
		numRounds = 10000
		numOps    = 50
		workers   = runtime.GOMAXPROCS(0)
	)

	violations := 0

	for round := 0; round < numRounds; round++ {
		var startTime time.Time

		m := dlht.New[string, int](dlht.Options{
			InitialSize: 4,
		})
		key := fmt.Sprintf("same_key_%d", round)

		var operations []porcupine.Operation
		var opsMutex sync.Mutex
		var wg sync.WaitGroup

		startTime = time.Now()

		for g := range workers {
			wg.Add(1)
			go func(gid int) {
				defer wg.Done()
				for i := 0; i < numOps; i++ {
					value := gid*1000 + i

					var input tests.DLHTInput
					if i%3 == 0 {
						input = tests.DLHTInput{Op: tests.OpDelete, Key: key, Value: 0}
					} else {
						input = tests.DLHTInput{Op: tests.OpInsert, Key: key, Value: value}
					}

					var callTime int64
					atomic.StoreInt64(&callTime, time.Since(startTime).Nanoseconds())

					output := tests.ExecuteOperation(m, input)

					returnTime := time.Since(startTime).Nanoseconds()

					op := porcupine.Operation{
						ClientId: gid,
						Input:    input,
						Call:     callTime,
						Output:   output,
						Return:   returnTime,
					}

					opsMutex.Lock()
					operations = append(operations, op)
					opsMutex.Unlock()
				}
			}(g)
		}

		wg.Wait()

		ok, failures := tests.CheckPerKeyLinearizability(operations, 5*time.Second)

		if !ok {
			violations++
			if violations <= 3 {
				for _, f := range failures {
					filename := fmt.Sprintf("issue1_concurrent_%d_key_%s_%s.html", round, f.Key, time.Now().Format("150405"))
					file, _ := os.Create(filename)
					if file != nil {
						porcupine.Visualize(tests.DLHTModel, f.Info, file)
						file.Close()
						t.Logf("Round %d: Violation on key %q saved to %s", round, f.Key, filename)
					}
				}
				t.Fatalf("Round %d: Violation detected", round)
			}
		}
	}

	if violations > 0 {
		t.Errorf("Found %d violations in %d rounds (%.2f%%)", violations, numRounds, float64(violations)*100/float64(numRounds))
	} else {
		t.Logf("No violations in %d rounds", numRounds)
	}
}
