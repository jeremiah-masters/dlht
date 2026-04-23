package pbt

import (
	"fmt"
	"testing"

	"github.com/jeremiah-masters/dlht"

	"pgregory.net/rapid"
)

func TestPBTHeaderVersionStress(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		threads := rapid.IntRange(2, 6).Draw(t, "threads")
		maxOpsPerThread := 80 / threads
		if maxOpsPerThread < 4 {
			maxOpsPerThread = 4
		}
		opsPerThread := rapid.IntRange(4, maxOpsPerThread).Draw(t, "opsPerThread")
		initialSize := rapid.Uint64Range(4, 32).Draw(t, "initialSize")

		m := dlht.New[string, int](dlht.Options{InitialSize: initialSize})
		key := "version_key"
		mix := OpMix{Get: 35, Insert: 25, Put: 25, Delete: 15}
		opGen := rapid.Custom(func(t *rapid.T) Op[string, int] {
			kind := DrawOpKind(t, mix)
			var value int
			if kind == OpInsert || kind == OpPut {
				value = rapid.IntRange(-100000, 100000).Draw(t, "val")
			}
			return Op[string, int]{Kind: kind, Key: key, Value: value}
		})
		opsByThread := make([][]Op[string, int], threads)
		for i := 0; i < threads; i++ {
			opsByThread[i] = rapid.SliceOfN(opGen, opsPerThread, opsPerThread).Draw(t, fmt.Sprintf("ops_%d", i))
		}

		history := runConcurrentHistory(t, m, opsByThread)

		ok, reason := ValidatePerKeyLinearizable(history, MaxOracleStates)
		if !ok {
			t.Fatalf("per-key linearizability failed: %s", reason)
		}
	})
}
