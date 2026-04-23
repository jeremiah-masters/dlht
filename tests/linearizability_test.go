package tests

import (
	"fmt"
	"math/rand/v2"
	"runtime"
	"testing"
	"time"
)

var linearizabilityTestConfigs = []TestConfig{
	{
		Name:            "BasicFunctionality",
		NumClients:      4,
		NumOpsPerClient: 25,
		NumKeys:         8,
		InitialSize:     16,
		OpDistribution:  []float64{0.4, 0.2, 0.2, 0.2}, // Balanced
		ContentionLevel: "low",
		Description:     "Basic functionality test with low contention",
	},
	{
		Name:            "InsertRaceCondition",
		NumClients:      8,
		NumOpsPerClient: 50,
		NumKeys:         2, // High contention
		InitialSize:     4,
		OpDistribution:  []float64{0.1, 0.8, 0.05, 0.05}, // Insert-heavy
		ContentionLevel: "high",
		Description:     "Targets the specific Insert race condition",
	},
	{
		Name:            "HighContentionInsertRace",
		NumClients:      16,
		NumOpsPerClient: 100,
		NumKeys:         4, // High contention on few keys
		InitialSize:     8,
		OpDistribution:  []float64{0.2, 0.6, 0.1, 0.1}, // Insert-heavy
		ContentionLevel: "high",
		Description:     "Tests Insert race conditions under extreme contention",
	},
	{
		Name:            "MixedWorkloadStress",
		NumClients:      12,
		NumOpsPerClient: 200,
		NumKeys:         20,
		InitialSize:     16,
		OpDistribution:  []float64{0.4, 0.25, 0.25, 0.1}, // Balanced workload
		ContentionLevel: "medium",
		Description:     "Realistic mixed workload with balanced operations",
	},
	{
		Name:            "ReadHeavyWithUpdates",
		NumClients:      20,
		NumOpsPerClient: 150,
		NumKeys:         50,
		InitialSize:     32,
		OpDistribution:  []float64{0.7, 0.1, 0.15, 0.05}, // Read-heavy
		ContentionLevel: "low",
		Description:     "Read-heavy workload with occasional updates",
	},
	{
		Name:            "ChurnIntensive",
		NumClients:      8,
		NumOpsPerClient: 300,
		NumKeys:         10,
		InitialSize:     8,
		OpDistribution:  []float64{0.1, 0.3, 0.3, 0.3}, // High churn
		ContentionLevel: "high",
		Description:     "High churn scenario with frequent Insert/Put/Delete cycles",
	},
	{
		Name:            "ScalabilityTest",
		NumClients:      32,
		NumOpsPerClient: 50,
		NumKeys:         100,
		InitialSize:     64,
		OpDistribution:  []float64{0.5, 0.2, 0.2, 0.1}, // Scalability focused
		ContentionLevel: "low",
		Description:     "Tests scalability with many clients and keys",
	},
}

var workloadPatterns = []WorkloadPattern{
	{
		Name:        "SameKeyInsert",
		Description: "Multiple clients inserting the same key simultaneously",
		Generator: func(clientId, opId int, config TestConfig) DLHTInput {
			key := fmt.Sprintf("race_key_%d", opId%max(1, config.NumKeys))
			value := clientId*1000 + opId

			if rand.Float64() < 0.8 {
				return DLHTInput{OpInsert, key, value}
			}
			return DLHTInput{OpGet, key, 0}
		},
	},
	{
		Name:        "UniformPattern",
		Description: "Uniform distribution across all keys",
		Generator: func(clientId, opId int, config TestConfig) DLHTInput {
			key := fmt.Sprintf("key_%d", rand.IntN(config.NumKeys))
			value := clientId*1000 + opId
			return selectOperation(config.OpDistribution, key, value)
		},
	},
	{
		Name:        "HotspotPattern",
		Description: "Creates hotspots where certain keys are accessed much more frequently",
		Generator: func(clientId, opId int, config TestConfig) DLHTInput {
			// 80% of operations target 20% of keys (hotspot)
			var key string
			if rand.Float64() < 0.8 {
				hotspotKeys := max(1, config.NumKeys/5)
				key = fmt.Sprintf("hot_%d", rand.IntN(hotspotKeys))
			} else {
				key = fmt.Sprintf("cold_%d", rand.IntN(config.NumKeys))
			}

			value := clientId*1000 + opId
			return selectOperation(config.OpDistribution, key, value)
		},
	},
	{
		Name:        "TemporalLocalityPattern",
		Description: "Simulates temporal locality where recently accessed keys are more likely to be accessed again",
		Generator: func(clientId, opId int, config TestConfig) DLHTInput {
			// Use a sliding window of recently accessed keys
			windowSize := min(10, config.NumKeys)
			recentOffset := max(0, opId-windowSize)

			var key string
			if rand.Float64() < 0.6 && opId > 0 {
				// Access recently used key
				recentKey := recentOffset + rand.IntN(min(windowSize, opId))
				key = fmt.Sprintf("recent_%d_%d", clientId, recentKey)
			} else {
				// Access random key
				key = fmt.Sprintf("key_%d", rand.IntN(config.NumKeys))
			}

			value := clientId*1000 + opId
			return selectOperation(config.OpDistribution, key, value)
		},
	},
	{
		Name:        "BurstPattern",
		Description: "Creates bursty access patterns with periods of high and low activity",
		Generator: func(clientId, opId int, config TestConfig) DLHTInput {
			// Create burst cycles every 20 operations
			burstCycle := opId % 20
			var key string

			if burstCycle < 5 {
				// Burst phase - high contention on few keys
				key = fmt.Sprintf("burst_%d", rand.IntN(2))
			} else {
				// Normal phase - distributed access
				key = fmt.Sprintf("normal_%d", rand.IntN(config.NumKeys))
			}

			value := clientId*1000 + opId
			return selectOperation(config.OpDistribution, key, value)
		},
	},
}

func TestLinearizability(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping linearizability tests in short mode")
	}

	for _, config := range linearizabilityTestConfigs {
		for _, pattern := range workloadPatterns {
			t.Run(fmt.Sprintf("%s_%s", config.Name, pattern.Name), func(t *testing.T) {
				RunLinearizabilityTest(t, config, pattern)
			})
		}
	}
}

type contentionCombinationSuite struct {
	Name          string
	Rounds        int
	OpsPerWorker  int
	InitialSize   uint64
	Timeout       time.Duration
	Visualization bool
}

type opSequence struct {
	Name string
	Ops  []int
}

var contentionCombinationSuites = []contentionCombinationSuite{
	{
		Name:          "ManyRoundsLowOps",
		Rounds:        256, // issue1 baseline emphasizes many short rounds
		OpsPerWorker:  24,
		InitialSize:   4,
		Timeout:       25 * time.Second,
		Visualization: true,
	},
	{
		Name:          "LowRoundsManyOps",
		Rounds:        32,
		OpsPerWorker:  320,
		InitialSize:   4,
		Timeout:       30 * time.Second,
		Visualization: true,
	},
}

// TestLinearizabilityContentionCombinations runs high-contention combinations
// across small key sets (1–3 keys) using the maximum worker count. The
// sequences are close to the cartesian product of operations, and Insert
// sequences are always paired with Delete to give them a path to succeed.
func TestLinearizabilityContentionCombinations(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping contention combination tests in short mode")
	}

	workers := runtime.GOMAXPROCS(0)
	keyCounts := []int{1, 2, 3}
	sequencePatterns := buildSequencePatterns()

	for _, suite := range contentionCombinationSuites {
		for _, numKeys := range keyCounts {
			for _, pattern := range sequencePatterns {
				config := TestConfig{
					Name:            fmt.Sprintf("%s_Keys%d_%s", suite.Name, numKeys, pattern.Name),
					NumClients:      workers,
					NumOpsPerClient: suite.OpsPerWorker,
					NumKeys:         numKeys,
					InitialSize:     suite.InitialSize,
					ContentionLevel: "high",
					Description:     fmt.Sprintf("High-contention deterministic sequence %s over %d keys", pattern.Name, numKeys),
				}

				t.Run(config.Name, func(t *testing.T) {
					for round := 0; round < suite.Rounds; round++ {
						RunLinearizabilityTest(t, config, pattern)
					}
				})
			}
		}
	}
}

func buildContentionOpSequences() []opSequence {
	base := []struct {
		name string
		op   int
	}{
		{"Get", OpGet},
		{"Put", OpPut},
		{"Delete", OpDelete},
	}

	var sequences []opSequence

	// Near-cartesian product of non-insert operations (pairs).
	for _, first := range base {
		for _, second := range base {
			sequences = append(sequences, opSequence{
				Name: fmt.Sprintf("%sThen%s", first.name, second.name),
				Ops:  []int{first.op, second.op},
			})
		}
	}

	// Insert cases always paired with a Delete so the insert has a chance to succeed.
	for _, tail := range base {
		sequences = append(sequences, opSequence{
			Name: fmt.Sprintf("DeleteInsertThen%s", tail.name),
			Ops:  []int{OpDelete, OpInsert, tail.op},
		})
	}

	return sequences
}

func buildSequencePatterns() []WorkloadPattern {
	var patterns []WorkloadPattern
	for _, seq := range buildContentionOpSequences() {
		seqCopy := seq
		patterns = append(patterns, WorkloadPattern{
			Name:        fmt.Sprintf("Seq%s", seqCopy.Name),
			Description: "Deterministic contention sequence over small keyset",
			Generator: func(clientId, opId int, config TestConfig) DLHTInput {
				chunk := opId / len(seqCopy.Ops)
				key := fmt.Sprintf("key_%d", chunk%max(1, config.NumKeys))
				op := seqCopy.Ops[opId%len(seqCopy.Ops)]
				value := clientId*100000 + opId
				return DLHTInput{Op: op, Key: key, Value: value}
			},
		})
	}
	return patterns
}
