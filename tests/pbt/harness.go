package pbt

import (
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/jeremiah-masters/dlht"

	"pgregory.net/rapid"
)

func execOp[K dlht.Key, V any](m *dlht.Map[K, V], op Op[K, V]) OpResult[V] {
	switch op.Kind {
	case OpGet:
		val, ok := m.Get(op.Key)
		return OpResult[V]{Found: ok, Value: val}
	case OpInsert:
		prev, ok := m.Insert(op.Key, op.Value)
		return OpResult[V]{Found: ok, Value: prev}
	case OpPut:
		old, ok := m.Put(op.Key, op.Value)
		return OpResult[V]{Updated: ok, Value: old}
	case OpDelete:
		deleted, ok := m.Delete(op.Key)
		return OpResult[V]{Found: ok, Value: deleted}
	case OpLoadOrCompute, OpLoadOrComputeNoSave:
		save := op.Kind == OpLoadOrCompute
		calls := 0
		val, loaded := m.LoadOrCompute(op.Key, func() (V, bool) {
			calls++
			return op.Value, save
		})
		if calls > 1 {
			panic(fmt.Sprintf("LoadOrCompute called fn %d times", calls))
		}
		return OpResult[V]{Found: loaded, Value: val, Computed: calls == 1}
	default:
		panic(fmt.Sprintf("unknown op kind: %d", op.Kind))
	}
}

func drawOpsByThread[K comparable, V any](t *rapid.T, threads int, opsGen *rapid.Generator[[]Op[K, V]], labelPrefix string) [][]Op[K, V] {
	opsByThread := make([][]Op[K, V], threads)
	for i := 0; i < threads; i++ {
		opsByThread[i] = opsGen.Draw(t, fmt.Sprintf("%s_%d", labelPrefix, i))
	}
	return opsByThread
}

// runConcurrentHistory executes each thread's ops on its own goroutine and
// returns the merged timed history. All rapid draws must happen before the
// call; nothing here touches *rapid.T.
func runConcurrentHistory[K dlht.Key, V any](m *dlht.Map[K, V], opsByThread [][]Op[K, V]) []TimedOp[K, V] {
	var seq SeqCounter
	localHistories := make([][]TimedOp[K, V], len(opsByThread))

	var wg sync.WaitGroup
	for i := range opsByThread {
		wg.Add(1)
		go func(slot int, threadOps []Op[K, V]) {
			defer wg.Done()
			threadHistory := make([]TimedOp[K, V], 0, len(threadOps))
			for _, op := range threadOps {
				start := seq.Start()
				res := execOp(m, op)
				end := seq.End()
				threadHistory = append(threadHistory, TimedOp[K, V]{
					Op:       op,
					Result:   res,
					StartSeq: start,
					EndSeq:   end,
					ThreadId: slot,
				})
			}
			localHistories[slot] = threadHistory
		}(i, opsByThread[i])
	}
	wg.Wait()

	total := 0
	for i := range localHistories {
		total += len(localHistories[i])
	}
	merged := make([]TimedOp[K, V], 0, total)
	for i := range localHistories {
		merged = append(merged, localHistories[i]...)
	}
	return merged
}

func filterTimedOpsByKey[K comparable, V any](ops []TimedOp[K, V], key K) []TimedOp[K, V] {
	keyOps := make([]TimedOp[K, V], 0)
	for _, op := range ops {
		if op.Op.Key == key {
			keyOps = append(keyOps, op)
		}
	}
	return keyOps
}

func runPerKeyLinearizabilityCase[K dlht.Key](t *rapid.T, initialSize uint64, threads int, opsPerThread int, keyGen *rapid.Generator[K]) {
	runPerKeyLinearizabilityCaseMix(t, initialSize, threads, opsPerThread, keyGen, MixChurn)
}

func runPerKeyLinearizabilityCaseMix[K dlht.Key](t *rapid.T, initialSize uint64, threads int, opsPerThread int, keyGen *rapid.Generator[K], mix OpMix) {
	valGen := rapid.IntRange(-1000, 1000)
	opsGen := GenOpSequence(keyGen, valGen, mix, opsPerThread, opsPerThread)
	opsByThread := drawOpsByThread(t, threads, opsGen, "ops")

	m := dlht.New[K, int](dlht.Options{InitialSize: initialSize})
	history := runConcurrentHistory(m, opsByThread)

	if res := ValidatePerKeyLinearizablePorcupineFromInitial(history, nil); !res.Ok {
		t.Fatalf("per-key linearizability failed: %s", res.Reason)
	}
}

// setGOMAXPROCS sets GOMAXPROCS for the duration of one test case and returns
// a restore func. Concurrent property tests sweep this as a grid axis: 1
// exercises the cooperative-yield paths, higher values buy real parallelism.
// Callers must not use t.Parallel.
func setGOMAXPROCS(n int) (restore func()) {
	prev := runtime.GOMAXPROCS(n)
	return func() { runtime.GOMAXPROCS(prev) }
}

// gomaxprocsAxis is the sweep for concurrent grid tests.
func gomaxprocsAxis() []int {
	n := runtime.NumCPU()
	axis := []int{1, 2}
	if n > 2 {
		axis = append(axis, n)
	}
	return axis
}

// errSlots collects at most one error per worker goroutine. testing.T.Fatalf
// must not be called off the test goroutine, so workers record here and the
// test goroutine reports after join.
type errSlots struct {
	errs []error
}

func newErrSlots(n int) *errSlots { return &errSlots{errs: make([]error, n)} }

// set records err for the worker; only the first error per slot is kept.
func (e *errSlots) set(worker int, err error) {
	if e.errs[worker] == nil {
		e.errs[worker] = err
	}
}

// first returns the first recorded error, or nil.
func (e *errSlots) first() error {
	for _, err := range e.errs {
		if err != nil {
			return err
		}
	}
	return nil
}

// waitWithWatchdog waits for wg with a deadline. On timeout it returns all
// goroutine stacks so a hang points at the blocked path instead of the
// package-level test timeout.
func waitWithWatchdog(wg *sync.WaitGroup, budget time.Duration) (stacks string, ok bool) {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return "", true
	case <-time.After(budget):
		buf := make([]byte, 1<<20)
		for {
			n := runtime.Stack(buf, true)
			if n < len(buf) {
				return string(buf[:n]), false
			}
			buf = make([]byte, 2*len(buf))
		}
	}
}
