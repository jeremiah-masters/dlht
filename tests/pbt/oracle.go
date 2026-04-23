package pbt

import (
	"fmt"
	"reflect"
	"sync/atomic"
)

const (
	// MaxOracleStates is the default state exploration limit for the per-key
	// linearizability oracle. Tests with small concurrent histories use this.
	MaxOracleStates = 20_000

	// MaxOracleStatesLarge is used by tests that generate larger histories
	// (e.g. resize tests with growth threads) where the search space is wider.
	MaxOracleStatesLarge = 50_000
)

type OpResult[V any] struct {
	Found   bool
	Value   V
	Updated bool
}

type TimedOp[K comparable, V any] struct {
	Op       Op[K, V]
	Result   OpResult[V]
	StartSeq uint64
	EndSeq   uint64
	ThreadId int
}

type SeqCounter struct {
	counter atomic.Uint64
}

func (s *SeqCounter) Start() uint64 {
	return s.counter.Add(1)
}

func (s *SeqCounter) End() uint64 {
	return s.counter.Add(1)
}

type keyState[V any] struct {
	exists bool
	value  V
}

type OracleSearchStats struct {
	ExploredStates int
	HitLimit       bool
	Limit          int
}

func ValidatePerKeyLinearizable[K comparable, V any](ops []TimedOp[K, V], maxStates int) (bool, string) {
	return ValidatePerKeyLinearizableFromInitial(ops, nil, maxStates)
}

func ValidatePerKeyLinearizableFromInitial[K comparable, V any](ops []TimedOp[K, V], initial map[K]keyState[V], maxStates int) (bool, string) {
	perKey := make(map[K][]TimedOp[K, V])
	for _, op := range ops {
		perKey[op.Op.Key] = append(perKey[op.Op.Key], op)
	}
	for key, keyOps := range perKey {
		startState := keyState[V]{}
		if initial != nil {
			if st, ok := initial[key]; ok {
				startState = st
			}
		}
		ok, reason, stats := validateKeyOpsWithInitial(keyOps, startState, maxStates)
		if !ok {
			if stats.HitLimit {
				return false, fmt.Sprintf(
					"key=%v: %s (explored=%d limit=%d ops=%d)",
					key,
					reason,
					stats.ExploredStates,
					stats.Limit,
					len(keyOps),
				)
			}
			return false, fmt.Sprintf("key=%v: %s", key, reason)
		}
	}
	return true, ""
}

// oracleDFS is the core topological-order DFS over a concurrent history's
// happens-before graph. The onComplete callback is invoked each time all ops
// have been placed. Return true from onComplete to stop the search early.
func oracleDFS[K comparable, V any](ops []TimedOp[K, V], initial keyState[V], maxStates int, onComplete func(keyState[V]) bool) OracleSearchStats {
	n := len(ops)
	stats := OracleSearchStats{Limit: maxStates}
	if n == 0 {
		onComplete(initial)
		return stats
	}

	succ := make([][]int, n)
	predCount := make([]int, n)
	for i := 0; i < n; i++ {
		for j := i + 1; j < n; j++ {
			if ops[i].EndSeq < ops[j].StartSeq {
				succ[i] = append(succ[i], j)
				predCount[j]++
			} else if ops[j].EndSeq < ops[i].StartSeq {
				succ[j] = append(succ[j], i)
				predCount[i]++
			}
		}
	}

	used := make([]bool, n)
	usedCount := 0
	done := false

	var dfs func(state keyState[V])
	dfs = func(state keyState[V]) {
		if done || stats.HitLimit {
			return
		}
		stats.ExploredStates++
		if maxStates > 0 && stats.ExploredStates > maxStates {
			stats.HitLimit = true
			return
		}
		if usedCount == n {
			done = onComplete(state)
			return
		}
		for i := 0; i < n; i++ {
			if used[i] || predCount[i] != 0 {
				continue
			}
			exp, next := expectedResult(state, ops[i].Op)
			if !matchResult(exp, ops[i].Result) {
				continue
			}
			used[i] = true
			usedCount++
			for _, s := range succ[i] {
				predCount[s]--
			}
			dfs(next)
			for _, s := range succ[i] {
				predCount[s]++
			}
			usedCount--
			used[i] = false
			if done || stats.HitLimit {
				return
			}
		}
	}

	dfs(initial)
	return stats
}

func PossibleEndStatesForKeyFromInitial[K comparable, V any](ops []TimedOp[K, V], initial keyState[V], maxStates int) ([]keyState[V], bool, OracleSearchStats) {
	var finals []keyState[V]
	foundAny := false

	stats := oracleDFS(ops, initial, maxStates, func(state keyState[V]) bool {
		foundAny = true
		for _, f := range finals {
			if reflect.DeepEqual(f, state) {
				return false
			}
		}
		finals = append(finals, state)
		return false // keep searching for all end states
	})

	if stats.HitLimit {
		return finals, false, stats
	}
	return finals, foundAny, stats
}

func validateKeyOpsWithInitial[K comparable, V any](ops []TimedOp[K, V], initial keyState[V], maxStates int) (bool, string, OracleSearchStats) {
	found := false
	stats := oracleDFS(ops, initial, maxStates, func(_ keyState[V]) bool {
		found = true
		return true // stop on first valid ordering
	})

	if found {
		return true, "", stats
	}
	if stats.HitLimit {
		return false, "state exploration limit exceeded", stats
	}
	return false, "no valid per-key ordering found", stats
}

func expectedResult[K comparable, V any](state keyState[V], op Op[K, V]) (OpResult[V], keyState[V]) {
	var zero V
	switch op.Kind {
	case OpGet:
		if state.exists {
			return OpResult[V]{Found: true, Value: state.value}, state
		}
		return OpResult[V]{Found: false, Value: zero}, state
	case OpInsert:
		if state.exists {
			return OpResult[V]{Found: false, Value: state.value}, state
		}
		return OpResult[V]{Found: true, Value: zero}, keyState[V]{exists: true, value: op.Value}
	case OpPut:
		if state.exists {
			return OpResult[V]{Updated: true, Value: state.value}, keyState[V]{exists: true, value: op.Value}
		}
		return OpResult[V]{Updated: false, Value: zero}, state
	case OpDelete:
		if state.exists {
			return OpResult[V]{Found: true, Value: state.value}, keyState[V]{}
		}
		return OpResult[V]{Found: false, Value: zero}, state
	default:
		panic(fmt.Sprintf("unknown op kind in oracle: %d", op.Kind))
	}
}

func matchResult[V any](expected, observed OpResult[V]) bool {
	if expected.Found != observed.Found || expected.Updated != observed.Updated {
		return false
	}
	return reflect.DeepEqual(expected.Value, observed.Value)
}
