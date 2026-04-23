package benchmarks

import (
	"math/rand/v2"
	"testing"
)

func TestUniformUint64_Unique(t *testing.T) {
	keys := GenerateUint64Keys(10000, 42)
	if len(keys) != 10000 {
		t.Fatalf("expected 10000 keys, got %d", len(keys))
	}
	seen := make(map[uint64]bool, len(keys))
	for _, k := range keys {
		if seen[k] {
			t.Fatalf("duplicate key: %d", k)
		}
		seen[k] = true
	}
}

func TestUniformStringHex_Unique(t *testing.T) {
	keys := GenerateStringHexKeys(10000, 42)
	if len(keys) != 10000 {
		t.Fatalf("expected 10000 keys, got %d", len(keys))
	}
	seen := make(map[string]bool, len(keys))
	for _, k := range keys {
		if seen[k] {
			t.Fatalf("duplicate key: %s", k)
		}
		seen[k] = true
		if len(k) != 16 {
			t.Fatalf("expected 16-char hex string, got %q (len=%d)", k, len(k))
		}
	}
}

func TestTrigramStrings_Unique(t *testing.T) {
	keys := GenerateTrigramKeys(10000, 42)
	if len(keys) != 10000 {
		t.Fatalf("expected 10000 keys, got %d", len(keys))
	}
	seen := make(map[string]bool, len(keys))
	dupes := 0
	for _, k := range keys {
		if seen[k] {
			dupes++
		}
		seen[k] = true
		if len(k) < 9 || len(k) > 30 {
			t.Fatalf("trigram key length out of range: %q (len=%d)", k, len(k))
		}
	}
	// Some duplication is acceptable for trigram keys since the space is smaller
	if dupeRate := float64(dupes) / float64(len(keys)); dupeRate > 0.01 {
		t.Fatalf("too many duplicate trigram keys: %.1f%%", dupeRate*100)
	}
}

func TestZipfianDistribution_Skewed(t *testing.T) {
	rng := rand.New(rand.NewPCG(42, 0))
	n := 1000
	counts := make([]int, n)
	samples := 100_000
	for i := 0; i < samples; i++ {
		idx := ZipfianSample(rng, n)
		if idx < 0 || idx >= n {
			t.Fatalf("ZipfianSample returned %d, want [0, %d)", idx, n)
		}
		counts[idx]++
	}

	// Top 1% of keys (10 keys) should have > 10% of total accesses
	top1pct := 0
	for i := 0; i < 10; i++ {
		top1pct += counts[i]
	}
	if top1pctRate := float64(top1pct) / float64(samples); top1pctRate < 0.10 {
		t.Fatalf("Zipfian not skewed enough: top 1%% has only %.1f%% of accesses", top1pctRate*100)
	}
}

func TestZipfianDistribution_Deterministic(t *testing.T) {
	rng1 := rand.New(rand.NewPCG(42, 0))
	rng2 := rand.New(rand.NewPCG(42, 0))
	for i := 0; i < 100; i++ {
		a := ZipfianSample(rng1, 1000)
		b := ZipfianSample(rng2, 1000)
		if a != b {
			t.Fatalf("ZipfianSample not deterministic: sample %d got %d vs %d", i, a, b)
		}
	}
}
