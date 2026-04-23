package pbt

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/jeremiah-masters/dlht"

	"pgregory.net/rapid"
)

func TestPBTIndexNextTraversal(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		initialSize := rapid.Uint64Range(2, 4).Draw(t, "initialSize")
		persistentKeyspace := rapid.IntRange(2, 4).Draw(t, "persistentKeyspace")
		persistentMix := MixChurn
		threads := rapid.IntRange(2, 5).Draw(t, "threads")
		opsPerThread := rapid.IntRange(16, 32).Draw(t, "opsPerThread")
		growthOps := rapid.IntRange(240, 420).Draw(t, "growthOps")

		m := dlht.New[string, int](dlht.Options{InitialSize: initialSize})
		statsInitial := m.Stats()
		persistentKeys := make([]string, persistentKeyspace)
		for i := 0; i < persistentKeyspace; i++ {
			key := fmt.Sprintf("persist_%d", i)
			persistentKeys[i] = key
			if _, ok := m.Insert(key, i); !ok {
				t.Fatalf("failed to prefill persistent key=%s", key)
			}
		}

		statsAfterPrefill := m.Stats()
		initialState := make(map[string]keyState[int], persistentKeyspace)
		for i := range persistentKeys {
			initialState[persistentKeys[i]] = keyState[int]{exists: true, value: i}
		}

		keyGen := rapid.SampledFrom(persistentKeys)
		valGen := rapid.IntRange(-1000, 1000)
		opsGen := GenOpSequence(keyGen, valGen, persistentMix, opsPerThread, opsPerThread)
		opsByThread := drawOpsByThread(t, threads, opsGen, "persistentOps")

		growthThread := make([]Op[string, int], 0, growthOps)
		for i := 0; i < growthOps; i++ {
			growthThread = append(growthThread, Op[string, int]{
				Kind:  OpInsert,
				Key:   fmt.Sprintf("grow_chain_%d", i),
				Value: i,
			})
			if i%11 == 0 {
				growthThread = append(growthThread, Op[string, int]{
					Kind: OpGet,
					Key:  persistentKeys[i%len(persistentKeys)],
				})
			}
		}
		opsByThread = append(opsByThread, growthThread)

		history := runConcurrentHistory(t, m, opsByThread)

		ok, reason := ValidatePerKeyLinearizableFromInitial(history, initialState, MaxOracleStatesLarge)
		if !ok {
			pResult := ValidatePerKeyLinearizablePorcupineFromInitial(history, initialState)
			if pResult.Ok {
				t.Fatalf("per-key linearizability failed by custom oracle only: %s\n%s", reason, formatFailedKeyHistory(history, reason))
			}
			filenames := pResult.WriteVisualizations("index_next_traversal")
			t.Fatalf(
				"per-key linearizability failed: custom=%s; porcupine=%s\nvisualizations: %v\n%s",
				reason,
				pResult.Reason,
				filenames,
				formatFailedKeyHistory(history, reason),
			)
		}

		statsAfter := m.Stats()
		if statsAfter.NumBins <= statsAfterPrefill.NumBins {
			t.Fatalf(
				"expected resize growth during concurrent phase: bins_after_prefill=%d bins_after=%d",
				statsAfterPrefill.NumBins,
				statsAfter.NumBins,
			)
		}
		if statsAfter.NumBins < statsInitial.NumBins*64 {
			t.Fatalf(
				"expected multiple resize generations for indexNext traversal: bins_initial=%d bins_after=%d",
				statsInitial.NumBins,
				statsAfter.NumBins,
			)
		}

		// Growth keys are inserted once and never deleted
		for idx := 0; idx < growthOps; idx++ {
			key := fmt.Sprintf("grow_chain_%d", idx)
			val, found := m.Get(key)
			if !found || val != idx {
				t.Fatalf("growth key missing or wrong value key=%s found=%v value=%d", key, found, val)
			}
		}
	})
}

func formatFailedKeyHistory(history []TimedOp[string, int], reason string) string {
	const prefix = "key="
	i := strings.Index(reason, prefix)
	if i < 0 {
		return "failed-key-history: key not found in reason"
	}
	rest := reason[i+len(prefix):]
	j := strings.Index(rest, ":")
	if j < 0 {
		return "failed-key-history: key parse failed"
	}
	key := rest[:j]
	keyOps := filterTimedOpsByKey(history, key)
	if len(keyOps) == 0 {
		return fmt.Sprintf("failed-key-history: key=%s has no recorded ops", key)
	}
	sort.Slice(keyOps, func(i, j int) bool {
		if keyOps[i].StartSeq == keyOps[j].StartSeq {
			return keyOps[i].EndSeq < keyOps[j].EndSeq
		}
		return keyOps[i].StartSeq < keyOps[j].StartSeq
	})
	minimized := minimizeUnsatKeyOps(keyOps)
	var b strings.Builder
	b.WriteString(fmt.Sprintf("failed-key-history key=%s ops=%d\n", key, len(keyOps)))
	for idx := range keyOps {
		op := keyOps[idx]
		b.WriteString(fmt.Sprintf(
			"  [%02d] %s(%q,%d) => found=%v updated=%v value=%d [%d,%d]\n",
			idx,
			op.Op.Kind,
			op.Op.Key,
			op.Op.Value,
			op.Result.Found,
			op.Result.Updated,
			op.Result.Value,
			op.StartSeq,
			op.EndSeq,
		))
	}
	if witness := findIntervalWitness(keyOps); witness != "" {
		b.WriteString(fmt.Sprintf("failed-key-interval-witness: %s\n", witness))
	}
	if len(minimized) > 0 && len(minimized) < len(keyOps) {
		b.WriteString(fmt.Sprintf("failed-key-history-minimized key=%s ops=%d\n", key, len(minimized)))
		for idx := range minimized {
			op := minimized[idx]
			b.WriteString(fmt.Sprintf(
				"  [m%02d] %s(%q,%d) => found=%v updated=%v value=%d [%d,%d]\n",
				idx,
				op.Op.Kind,
				op.Op.Key,
				op.Op.Value,
				op.Result.Found,
				op.Result.Updated,
				op.Result.Value,
				op.StartSeq,
				op.EndSeq,
			))
		}
	}
	return b.String()
}

func minimizeUnsatKeyOps(ops []TimedOp[string, int]) []TimedOp[string, int] {
	if len(ops) <= 1 {
		return nil
	}
	initial := keyState[int]{exists: true, value: parseInitialValueFromKey(ops[0].Op.Key)}
	cur := append([]TimedOp[string, int](nil), ops...)
	changed := true
	for changed {
		changed = false
		for i := 0; i < len(cur); i++ {
			cand := make([]TimedOp[string, int], 0, len(cur)-1)
			cand = append(cand, cur[:i]...)
			cand = append(cand, cur[i+1:]...)
			if len(cand) == 0 {
				continue
			}
			okCustom, _, _ := validateKeyOpsWithInitial(cand, initial, MaxOracleStatesLarge)
			okPorc := validateKeyOpsWithPorcupine(cand, initial)
			if !okCustom && !okPorc {
				cur = cand
				changed = true
				break
			}
		}
	}
	if len(cur) == len(ops) {
		return nil
	}
	return cur
}

func parseInitialValueFromKey(key string) int {
	var idx int
	_, err := fmt.Sscanf(key, "persist_%d", &idx)
	if err != nil {
		return 0
	}
	return idx
}

func findIntervalWitness(ops []TimedOp[string, int]) string {
	// If Put succeeds, key must exist afterward until a successful Delete linearizes.
	// A later successful Insert requires the key to be absent.
	for i := range ops {
		a := ops[i]
		if a.Op.Kind != OpPut || !a.Result.Updated {
			continue
		}
		for j := range ops {
			b := ops[j]
			if b.Op.Kind != OpInsert || !b.Result.Found {
				continue
			}
			if a.EndSeq >= b.StartSeq {
				continue
			}
			bridgeDelete := false
			for k := range ops {
				d := ops[k]
				if d.Op.Kind != OpDelete || !d.Result.Found {
					continue
				}
				if d.StartSeq < b.StartSeq && d.EndSeq > a.EndSeq {
					bridgeDelete = true
					break
				}
			}
			if !bridgeDelete {
				return fmt.Sprintf(
					"Put(updated=true old=%d new=%d)[%d,%d] then Insert(success=true value=%d)[%d,%d] with no successful Delete interval spanning between them",
					a.Result.Value,
					a.Op.Value,
					a.StartSeq,
					a.EndSeq,
					b.Op.Value,
					b.StartSeq,
					b.EndSeq,
				)
			}
		}
	}
	return ""
}
