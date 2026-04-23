package pbt

import (
	"fmt"
	"testing"

	"github.com/jeremiah-masters/dlht"

	"pgregory.net/rapid"
)

func TestPBTLinkArenaExhaustionLowCount(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		initialSize := rapid.Uint64Range(2, 4).Draw(t, "initialSize")
		groupCount := rapid.IntRange(8, 24).Draw(t, "groupCount")
		cycles := rapid.IntRange(2, 6).Draw(t, "cycles")
		keysPerGroup := rapid.IntRange(8, 20).Draw(t, "keysPerGroup")

		m := dlht.New[string, int](dlht.Options{InitialSize: initialSize})
		stats0 := m.Stats()

		groupKeys := make([][]string, groupCount)
		for g := 0; g < groupCount; g++ {
			keys := make([]string, keysPerGroup)
			for k := 0; k < keysPerGroup; k++ {
				keys[k] = fmt.Sprintf("group_%d_key_%d", g, k)
			}
			groupKeys[g] = keys
		}

		totalInsertSuccess := 0
		totalDeleteSuccess := 0

		for c := 0; c < cycles; c++ {
			for g := 0; g < groupCount; g++ {
				for k, key := range groupKeys[g] {
					if _, ok := m.Insert(key, c*1_000_000+g*1_000+k); !ok {
						cur, found := m.Get(key)
						t.Fatalf(
							"insert failed unexpectedly cycle=%d group=%d key=%s current_found=%v current_value=%d",
							c,
							g,
							key,
							found,
							cur,
						)
					}
					totalInsertSuccess++
				}
				for _, key := range groupKeys[g] {
					if _, ok := m.Delete(key); !ok {
						t.Fatalf("delete failed unexpectedly cycle=%d group=%d key=%s", c, g, key)
					}
					totalDeleteSuccess++
				}
			}
		}

		if totalInsertSuccess == 0 {
			t.Fatalf("expected successful inserts")
		}
		if totalDeleteSuccess == 0 {
			t.Fatalf("expected successful deletes")
		}

		for g := range groupKeys {
			for _, key := range groupKeys[g] {
				if _, ok := m.Get(key); ok {
					t.Fatalf("key should be absent after churn key=%s", key)
				}
			}
		}

		stats1 := m.Stats()
		if stats1.NumLinks < stats0.NumLinks {
			t.Fatalf("link allocation decreased: before=%d after=%d", stats0.NumLinks, stats1.NumLinks)
		}
		if stats1.NumLinks == stats0.NumLinks && stats1.NumBins == stats0.NumBins {
			t.Fatalf(
				"expected link pressure evidence, got no link growth and no resize: links_before=%d links_after=%d bins_before=%d bins_after=%d",
				stats0.NumLinks,
				stats1.NumLinks,
				stats0.NumBins,
				stats1.NumBins,
			)
		}
	})
}
