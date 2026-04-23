package pbt

import (
	"fmt"
	"testing"

	"github.com/jeremiah-masters/dlht"

	"pgregory.net/rapid"
)

func TestPBTQuiescentConsistency(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		keyspace := rapid.IntRange(2, 8).Draw(t, "keyspace")
		threads := rapid.IntRange(2, 5).Draw(t, "threads")
		maxOpsPerThread := 40 / threads
		if maxOpsPerThread < 2 {
			maxOpsPerThread = 2
		}
		opsPerThread := rapid.IntRange(1, maxOpsPerThread).Draw(t, "opsPerThread")
		phases := rapid.IntRange(2, 4).Draw(t, "phases")
		initialSize := rapid.Uint64Range(4, 64).Draw(t, "initialSize")
		keyGen := GenStringKey(keyspace)
		valGen := rapid.IntRange(-1000, 1000)
		opsGen := GenOpSequence(keyGen, valGen, MixBalanced, opsPerThread, opsPerThread)

		keys := make([]string, keyspace)
		for i := 0; i < keyspace; i++ {
			keys[i] = fmt.Sprintf("key_%d", i)
		}

		m := dlht.New[string, int](dlht.Options{InitialSize: initialSize})
		knownStates := make(map[string]keyState[int], keyspace)

		for phase := 0; phase < phases; phase++ {
			opsByThread := make([][]Op[string, int], threads)
			for i := 0; i < threads; i++ {
				opsByThread[i] = opsGen.Draw(t, fmt.Sprintf("ops_%d_%d", phase, i))
			}
			phaseHistory := runConcurrentHistory(t, m, opsByThread)

			for _, key := range keys {
				keyOps := filterTimedOpsByKey(phaseHistory, key)
				finals, ok, stats := PossibleEndStatesForKeyFromInitial(keyOps, knownStates[key], MaxOracleStates)
				if !ok {
					t.Fatalf(
						"oracle exploration limit exceeded phase=%d key=%s explored=%d limit=%d ops=%d",
						phase,
						key,
						stats.ExploredStates,
						stats.Limit,
						len(keyOps),
					)
				}
				actualVal, actualFound := m.Get(key)
				matched := false
				for _, st := range finals {
					if st.exists == actualFound {
						if !actualFound || st.value == actualVal {
							matched = true
							break
						}
					}
				}
				if !matched {
					t.Fatalf(
						"quiescent mismatch phase=%d key=%s found=%v value=%d finals=%d",
						phase,
						key,
						actualFound,
						actualVal,
						len(finals),
					)
				}
				if actualFound {
					knownStates[key] = keyState[int]{exists: true, value: actualVal}
				} else {
					delete(knownStates, key)
				}
			}
		}
	})
}
