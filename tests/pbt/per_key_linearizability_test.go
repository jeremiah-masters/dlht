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
