package pbt

import (
	"fmt"
	"testing"

	"github.com/jeremiah-masters/dlht"

	"pgregory.net/rapid"
)

func TestPBTResizeConcurrentCorrectness(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		initialSize := rapid.Uint64Range(2, 4).Draw(t, "initialSize")
		keyspace := rapid.IntRange(8, 24).Draw(t, "keyspace")
		threads := rapid.IntRange(2, 5).Draw(t, "threads")
		opsPerThread := rapid.IntRange(10, 30).Draw(t, "opsPerThread")
		prefillCount := rapid.IntRange(4, 6).Draw(t, "prefillCount")
		growthOps := rapid.IntRange(400, 800).Draw(t, "growthOps")

		m := dlht.New[string, int](dlht.Options{InitialSize: initialSize})
		for i := 0; i < prefillCount; i++ {
			key := fmt.Sprintf("prefill_%d", i)
			if _, ok := m.Insert(key, i); !ok {
				t.Fatalf("prefill insert unexpectedly failed for key=%s", key)
			}
		}
		statsAfterPrefill := m.Stats()

		keyGen := GenHotspotStringKey(keyspace, 2, 90)
		valGen := rapid.IntRange(-1000, 1000)
		opsGen := GenOpSequence(keyGen, valGen, MixChurn, opsPerThread, opsPerThread)

		opsByThread := drawOpsByThread(t, threads, opsGen, "ops")
		growthThread := make([]Op[string, int], 0, growthOps)
		for i := 0; i < growthOps; i++ {
			growthThread = append(growthThread, Op[string, int]{
				Kind:  OpInsert,
				Key:   fmt.Sprintf("growth_%d", i),
				Value: i,
			})
		}
		opsByThread = append(opsByThread, growthThread)

		history := runConcurrentHistory(t, m, opsByThread)

		ok, reason := ValidatePerKeyLinearizable(history, MaxOracleStatesLarge)
		if !ok {
			t.Fatalf("per-key linearizability failed: %s", reason)
		}

		statsAfterConcurrent := m.Stats()
		if statsAfterConcurrent.NumBins <= statsAfterPrefill.NumBins {
			t.Fatalf(
				"expected concurrent phase resize growth: bins_after_prefill=%d bins_after_concurrent=%d",
				statsAfterPrefill.NumBins,
				statsAfterConcurrent.NumBins,
			)
		}

		// Growth keys are inserted exactly once and never deleted
		for i := 0; i < growthOps; i++ {
			key := fmt.Sprintf("growth_%d", i)
			val, found := m.Get(key)
			if !found || val != i {
				t.Fatalf("growth key missing or wrong value key=%s found=%v value=%d", key, found, val)
			}
		}

		// Prefill keys are also outside the churn keyspace and should remain intact
		for i := 0; i < prefillCount; i++ {
			key := fmt.Sprintf("prefill_%d", i)
			val, found := m.Get(key)
			if !found || val != i {
				t.Fatalf("prefill key missing or wrong value key=%s found=%v value=%d", key, found, val)
			}
		}
	})
}
