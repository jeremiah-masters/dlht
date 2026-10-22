package pbt

import (
	"testing"

	"pgregory.net/rapid"
)

func TestPBTPerKeyLinearizability(t *testing.T) {
	t.Run("String", func(t *testing.T) {
		rapid.Check(t, func(t *rapid.T) {
			keyspace := rapid.IntRange(1, 4).Draw(t, "keyspace")
			threads := rapid.IntRange(2, 4).Draw(t, "threads")
			opsPerThread := rapid.IntRange(1, 10).Draw(t, "opsPerThread")
			initialSize := rapid.Uint64Range(4, 32).Draw(t, "initialSize")
			runPerKeyLinearizabilityCase(t, initialSize, threads, opsPerThread, GenStringKey(keyspace))
		})
	})
	t.Run("Uint64", func(t *testing.T) {
		rapid.Check(t, func(t *rapid.T) {
			keyspace := rapid.IntRange(1, 4).Draw(t, "keyspace")
			threads := rapid.IntRange(2, 4).Draw(t, "threads")
			opsPerThread := rapid.IntRange(1, 10).Draw(t, "opsPerThread")
			initialSize := rapid.Uint64Range(4, 32).Draw(t, "initialSize")
			runPerKeyLinearizabilityCase(t, initialSize, threads, opsPerThread, GenUint64Key(keyspace))
		})
	})
}

func TestPBTPerKeyLinearizabilityHotspot(t *testing.T) {
	t.Run("String", func(t *testing.T) {
		rapid.Check(t, func(t *rapid.T) {
			keyspace := rapid.IntRange(4, 16).Draw(t, "keyspace")
			hotspotSize := rapid.IntRange(1, 3).Draw(t, "hotspotSize")
			hotspotPercent := rapid.IntRange(70, 95).Draw(t, "hotspotPercent")
			threads := rapid.IntRange(2, 4).Draw(t, "threads")
			opsPerThread := rapid.IntRange(2, 12).Draw(t, "opsPerThread")
			initialSize := rapid.Uint64Range(4, 32).Draw(t, "initialSize")
			runPerKeyLinearizabilityCase(t, initialSize, threads, opsPerThread, GenHotspotStringKey(keyspace, hotspotSize, hotspotPercent))
		})
	})
	t.Run("Uint64", func(t *testing.T) {
		rapid.Check(t, func(t *rapid.T) {
			keyspace := rapid.IntRange(4, 16).Draw(t, "keyspace")
			hotspotSize := rapid.IntRange(1, 3).Draw(t, "hotspotSize")
			hotspotPercent := rapid.IntRange(70, 95).Draw(t, "hotspotPercent")
			threads := rapid.IntRange(2, 4).Draw(t, "threads")
			opsPerThread := rapid.IntRange(2, 12).Draw(t, "opsPerThread")
			initialSize := rapid.Uint64Range(4, 32).Draw(t, "initialSize")
			runPerKeyLinearizabilityCase(t, initialSize, threads, opsPerThread, GenHotspotUint64Key(keyspace, hotspotSize, hotspotPercent))
		})
	})
}

func TestPBTPerKeyLinearizabilityCorpus(t *testing.T) {
	t.Run("String", func(t *testing.T) {
		rapid.Check(t, func(t *rapid.T) {
			threads := rapid.IntRange(2, 4).Draw(t, "threads")
			opsPerThread := rapid.IntRange(2, 12).Draw(t, "opsPerThread")
			initialSize := rapid.Uint64Range(4, 32).Draw(t, "initialSize")
			runPerKeyLinearizabilityCase(t, initialSize, threads, opsPerThread, rapid.SampledFrom(AdversarialKeyCorpus()))
		})
	})
	t.Run("Uint64", func(t *testing.T) {
		rapid.Check(t, func(t *rapid.T) {
			threads := rapid.IntRange(2, 4).Draw(t, "threads")
			opsPerThread := rapid.IntRange(2, 12).Draw(t, "opsPerThread")
			initialSize := rapid.Uint64Range(4, 32).Draw(t, "initialSize")
			runPerKeyLinearizabilityCase(t, initialSize, threads, opsPerThread, rapid.SampledFrom(AdversarialUint64Corpus()))
		})
	})
}

// LoadOrCompute interleaved with Insert/Put/Delete/Get on a few hot keys: the
// per-key Porcupine model checks that a call either loads the value present at
// its linearization point or runs fn exactly once and returns the computed
// value, inserting it when fn saves.
func TestPBTPerKeyLinearizabilityCompute(t *testing.T) {
	t.Run("String", func(t *testing.T) {
		rapid.Check(t, func(t *rapid.T) {
			keyspace := rapid.IntRange(1, 4).Draw(t, "keyspace")
			threads := rapid.IntRange(2, 4).Draw(t, "threads")
			opsPerThread := rapid.IntRange(1, 12).Draw(t, "opsPerThread")
			initialSize := rapid.Uint64Range(4, 32).Draw(t, "initialSize")
			runPerKeyLinearizabilityCaseMix(t, initialSize, threads, opsPerThread, GenStringKey(keyspace), MixCompute)
		})
	})
	t.Run("Uint64Hotspot", func(t *testing.T) {
		rapid.Check(t, func(t *rapid.T) {
			keyspace := rapid.IntRange(4, 16).Draw(t, "keyspace")
			hotspotSize := rapid.IntRange(1, 3).Draw(t, "hotspotSize")
			hotspotPercent := rapid.IntRange(70, 95).Draw(t, "hotspotPercent")
			threads := rapid.IntRange(2, 4).Draw(t, "threads")
			opsPerThread := rapid.IntRange(2, 12).Draw(t, "opsPerThread")
			initialSize := rapid.Uint64Range(4, 32).Draw(t, "initialSize")
			runPerKeyLinearizabilityCaseMix(t, initialSize, threads, opsPerThread, GenHotspotUint64Key(keyspace, hotspotSize, hotspotPercent), MixCompute)
		})
	})
	// A one- or two-bin table with a link arena too small for the keyspace
	// resizes under the workload, so claims get carried and holders relocate
	// while the history is being recorded.
	t.Run("Uint64Resizing", func(t *testing.T) {
		rapid.Check(t, func(t *rapid.T) {
			keyspace := rapid.IntRange(8, 32).Draw(t, "keyspace")
			threads := rapid.IntRange(2, 4).Draw(t, "threads")
			opsPerThread := rapid.IntRange(8, 24).Draw(t, "opsPerThread")
			initialSize := rapid.Uint64Range(1, 2).Draw(t, "initialSize")
			runPerKeyLinearizabilityCaseMix(t, initialSize, threads, opsPerThread, GenUint64Key(keyspace), MixCompute)
		})
	})
}
