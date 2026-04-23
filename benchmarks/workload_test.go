package benchmarks

import (
	"math"
	"math/rand/v2"
	"testing"
)

func assertClose(t *testing.T, got, want, tolerance float64, label string) {
	t.Helper()
	if math.Abs(got-want) > tolerance {
		t.Errorf("%s: got %.3f, want %.3f (tolerance %.3f)", label, got, want, tolerance)
	}
}

func TestOpStreamMixRatios(t *testing.T) {
	for _, s := range Scenarios {
		if s.HotKeyMode {
			continue // HotKey has different semantics
		}
		t.Run(s.Name, func(t *testing.T) {
			rng := rand.New(rand.NewPCG(42, 0))
			sampler := func(r *rand.Rand, n int) int { return r.IntN(n) }
			ops := BuildOpStream(s, 100_000, 10_000, rng, sampler)

			counts := [4]int{}
			for _, op := range ops {
				counts[op.Type]++
			}
			total := float64(len(ops))
			assertClose(t, float64(counts[OpGet])/total, s.GetPct, 0.02, "GetPct")
			assertClose(t, float64(counts[OpInsert])/total, s.InsertPct, 0.02, "InsertPct")
			assertClose(t, float64(counts[OpDelete])/total, s.DeletePct, 0.02, "DeletePct")
			assertClose(t, float64(counts[OpPut])/total, s.PutPct, 0.02, "PutPct")
		})
	}
}

func TestOpStreamHotKey(t *testing.T) {
	s := Scenarios[len(Scenarios)-1] // HotKey is last
	if !s.HotKeyMode {
		t.Fatal("expected HotKey scenario to be last")
	}
	rng := rand.New(rand.NewPCG(42, 0))
	sampler := func(r *rand.Rand, n int) int { return r.IntN(n) }
	ops := BuildOpStream(s, 10_000, 1000, rng, sampler)
	for i, op := range ops {
		if op.KeyIdx != 0 {
			t.Fatalf("op %d: HotKey should always use keyIdx=0, got %d", i, op.KeyIdx)
		}
	}
}

func TestScenariosMixSumsToOne(t *testing.T) {
	for _, s := range Scenarios {
		sum := s.GetPct + s.InsertPct + s.DeletePct + s.PutPct
		if math.Abs(sum-1.0) > 0.001 {
			t.Errorf("scenario %s: op percentages sum to %.3f, want 1.0", s.Name, sum)
		}
	}
}
