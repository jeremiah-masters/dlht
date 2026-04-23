package benchmarks

import "math/rand/v2"

// OpType represents a map operation.
type OpType uint8

const (
	OpGet OpType = iota
	OpInsert
	OpDelete
	OpPut
)

// Op is a single pre-generated operation in the benchmark stream.
type Op struct {
	Type   OpType
	KeyIdx uint32 // index into the key slice
}

// WorkloadDef defines a benchmark workload scenario.
type WorkloadDef struct {
	Name       string
	GetPct     float64 // fraction of ops that are Get [0.0, 1.0]
	InsertPct  float64 // fraction that are Insert
	DeletePct  float64 // fraction that are Delete
	PutPct     float64 // fraction that are Put
	HotKeyMode bool    // if true, all ops target key index 0
	PrefillPct float64 // fraction of keyspace to prefill before timing [0.0, 1.0]
}

// Scenarios is the curated set of 7 workload scenarios.
var Scenarios = []WorkloadDef{
	{
		Name:       "ReadOnly",
		GetPct:     1.0,
		PrefillPct: 0.75,
	},
	{
		Name:       "ReadMostly",
		GetPct:     0.95,
		InsertPct:  0.025,
		DeletePct:  0.025,
		PrefillPct: 0.75,
	},
	{
		Name:       "HitMiss",
		GetPct:     1.0,
		PrefillPct: 0.50, // half the keyspace is prefilled; Get hits ~50%
	},
	{
		Name:       "Exchange",
		GetPct:     0.10,
		InsertPct:  0.40,
		DeletePct:  0.40,
		PutPct:     0.10,
		PrefillPct: 0.50,
	},
	{
		Name:       "WriteHeavy",
		GetPct:     0.50,
		InsertPct:  0.50,
		PrefillPct: 0.25,
	},
	{
		Name:       "PutHeavy",
		InsertPct:  0.10,
		DeletePct:  0.10,
		PutPct:     0.80,
		PrefillPct: 0.75,
	},
	{
		Name:       "RapidGrow",
		GetPct:     0.05,
		InsertPct:  0.80,
		DeletePct:  0.05,
		PutPct:     0.10,
		PrefillPct: 0.0,
	},
	{
		Name:       "HotKey",
		PutPct:     1.0,
		HotKeyMode: true,
		PrefillPct: 0.75,
	},
}

// BuildOpStream generates a pre-determined operation stream for a workload.
//
// Parameters:
//   - w: workload definition
//   - numOps: total operations to generate
//   - keyspaceSize: total number of unique keys
//   - rng: random number generator (for reproducibility)
//   - sampleIndex: distribution sampler function (uniform or zipfian)
//
// Key index semantics:
//   - Indices [0, prefillCount) are "existing" keys (prefilled before benchmark)
//   - Indices [prefillCount, keyspaceSize) are "new" keys
//   - Get ops sample from the full keyspace (hit/miss depends on whether index < prefillCount)
//   - Insert ops sample from new keys (more likely to succeed)
//   - Delete/Put ops sample from existing keys (more likely to find a target)
func BuildOpStream(w WorkloadDef, numOps int, keyspaceSize int, rng *rand.Rand, sampleIndex func(*rand.Rand, int) int) []Op {
	ops := make([]Op, numOps)
	prefillCount := int(float64(keyspaceSize) * w.PrefillPct)
	if prefillCount < 1 && w.PrefillPct > 0 {
		prefillCount = 1
	}
	newStart := prefillCount
	newCount := keyspaceSize - prefillCount
	if newCount < 1 {
		newCount = 1
		newStart = keyspaceSize - 1
	}

	for i := range ops {
		// Choose operation type via weighted random
		r := rng.Float64()
		var opType OpType
		switch {
		case r < w.GetPct:
			opType = OpGet
		case r < w.GetPct+w.InsertPct:
			opType = OpInsert
		case r < w.GetPct+w.InsertPct+w.DeletePct:
			opType = OpDelete
		default:
			opType = OpPut
		}

		// Choose key index
		var keyIdx int
		if w.HotKeyMode {
			keyIdx = 0
		} else {
			switch opType {
			case OpGet:
				// Sample from full keyspace, hit/miss naturally depends on prefill
				keyIdx = sampleIndex(rng, keyspaceSize)
			case OpInsert:
				// Sample from new keys (beyond prefill), more likely to be absent
				keyIdx = newStart + sampleIndex(rng, newCount)
			case OpDelete, OpPut:
				// Sample from existing keys (within prefill), more likely to be present
				if prefillCount > 0 {
					keyIdx = sampleIndex(rng, prefillCount)
				} else {
					keyIdx = sampleIndex(rng, keyspaceSize)
				}
			}
		}

		ops[i] = Op{Type: opType, KeyIdx: uint32(keyIdx)}
	}
	return ops
}

// PrefillCount returns the number of keys to prefill for a given workload and keyspace size.
func PrefillCount(w WorkloadDef, keyspaceSize int) int {
	n := int(float64(keyspaceSize) * w.PrefillPct)
	if n < 1 && w.PrefillPct > 0 {
		n = 1
	}
	return n
}
