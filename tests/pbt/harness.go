package pbt

import (
	"fmt"
	"sync"

	"github.com/jeremiah-masters/dlht"

	"pgregory.net/rapid"
)

type fatalfOnly interface {
	Fatalf(format string, args ...any)
}

func execOp[K dlht.Key, V any](tb fatalfOnly, m *dlht.Map[K, V], op Op[K, V]) OpResult[V] {
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
	default:
		tb.Fatalf("unknown op kind: %d", op.Kind)
		var zero V
		return OpResult[V]{Value: zero}
	}
}

func drawOpsByThread[K comparable, V any](t *rapid.T, threads int, opsGen *rapid.Generator[[]Op[K, V]], labelPrefix string) [][]Op[K, V] {
	opsByThread := make([][]Op[K, V], threads)
	for i := 0; i < threads; i++ {
		opsByThread[i] = opsGen.Draw(t, fmt.Sprintf("%s_%d", labelPrefix, i))
	}
	return opsByThread
}

func runConcurrentHistory[K dlht.Key, V any](tb fatalfOnly, m *dlht.Map[K, V], opsByThread [][]Op[K, V]) []TimedOp[K, V] {
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
				res := execOp(tb, m, op)
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
	valGen := rapid.IntRange(-1000, 1000)
	opsGen := GenOpSequence(keyGen, valGen, MixChurn, opsPerThread, opsPerThread)
	opsByThread := drawOpsByThread(t, threads, opsGen, "ops")

	m := dlht.New[K, int](dlht.Options{InitialSize: initialSize})
	history := runConcurrentHistory(t, m, opsByThread)

	ok, reason := ValidatePerKeyLinearizable(history, MaxOracleStates)
	if !ok {
		t.Fatalf("per-key linearizability failed: %s", reason)
	}
}
