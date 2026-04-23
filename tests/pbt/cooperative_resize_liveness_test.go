package pbt

import (
	"fmt"
	"sync"
	"testing"

	"github.com/jeremiah-masters/dlht"

	"pgregory.net/rapid"
)

func TestPBTCooperativeResizeLiveness(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		initialSize := rapid.Uint64Range(2, 8).Draw(rt, "initialSize")
		keyspace := rapid.IntRange(4, 32).Draw(rt, "keyspace")
		opsPerThread := rapid.IntRange(50, 150).Draw(rt, "opsPerThread")

		m := dlht.New[string, int](dlht.Options{InitialSize: initialSize})
		keyGen := GenStringKey(keyspace)
		valGen := rapid.IntRange(-1000, 1000)

		statsBefore := m.Stats()

		resizerKeys := make([]string, 0, opsPerThread*2)
		for i := 0; i < opsPerThread*2; i++ {
			// Keep resizer keys unique so every step must make forward progress.
			resizerKeys = append(resizerKeys, fmt.Sprintf("resKey_%d", i))
		}
		opsOther := GenOpSequence(keyGen, valGen, MixChurn, opsPerThread, opsPerThread).Draw(rt, "opsOther")

		var wg sync.WaitGroup
		wg.Add(2)

		go func() {
			defer wg.Done()
			for i := 0; i < len(resizerKeys); i++ {
				if _, ok := m.Insert(resizerKeys[i], i); !ok {
					rt.Fatalf("resizer insert failed key=%s index=%d", resizerKeys[i], i)
					return
				}
			}
		}()

		go func() {
			defer wg.Done()
			for _, op := range opsOther {
				_ = execOp(rt, m, op)
			}
		}()

		wg.Wait()

		statsAfter := m.Stats()
		if statsAfter.NumBins <= statsBefore.NumBins {
			rt.Fatalf(
				"expected resize growth during liveness run: bins_before=%d bins_after=%d",
				statsBefore.NumBins,
				statsAfter.NumBins,
			)
		}
	})
}
