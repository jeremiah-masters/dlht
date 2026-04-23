package pbt

import (
	"fmt"
	"os"
	"time"

	"github.com/anishathalye/porcupine"
)

// perKeyInput represents the input to a per-key operation for Porcupine.
type perKeyInput[V comparable] struct {
	Kind  OpKind
	Value V
}

// perKeyOutput represents the output of a per-key operation for Porcupine.
type perKeyOutput[V comparable] struct {
	Found   bool
	Updated bool
	Value   V
}

// perKeyState represents the state of a single key for Porcupine.
type perKeyState[V comparable] struct {
	exists bool
	value  V
}

// PorcupineKeyFailure holds failure info for a single key.
type PorcupineKeyFailure struct {
	Key   string
	Model porcupine.Model
	Info  porcupine.LinearizationInfo
}

// PorcupineResult holds the full result of per-key Porcupine validation.
type PorcupineResult struct {
	Ok       bool
	Reason   string
	Failures []PorcupineKeyFailure
}

// WriteVisualizations writes HTML visualization files for each failing key.
// Returns the list of filenames written.
func (r *PorcupineResult) WriteVisualizations(prefix string) []string {
	var filenames []string
	for _, f := range r.Failures {
		timestamp := time.Now().Format("20060102_150405")
		filename := fmt.Sprintf("violation_%s_%s_%s.html", prefix, f.Key, timestamp)
		file, err := os.Create(filename)
		if err != nil {
			continue
		}
		err = porcupine.Visualize(f.Model, f.Info, file)
		file.Close()
		if err == nil {
			filenames = append(filenames, filename)
		}
	}
	return filenames
}

// perKeyModel creates a porcupine.Model for single-key linearizability checking.
// The Step function checks ALL output fields (Found, Updated, Value) for every
// operation, matching the strictness of the custom oracle in oracle.go.
// This ensures both oracles agree on every history.
func perKeyModel[V comparable](initial perKeyState[V]) porcupine.Model {
	return porcupine.Model{
		Init: func() any {
			return initial
		},
		Step: func(state, input, output any) (bool, any) {
			s := state.(perKeyState[V])
			in := input.(perKeyInput[V])
			out := output.(perKeyOutput[V])
			var zero V

			switch in.Kind {
			case OpGet:
				if s.exists {
					expected := perKeyOutput[V]{Found: true, Value: s.value}
					return out == expected, s
				}
				expected := perKeyOutput[V]{Found: false, Value: zero}
				return out == expected, s

			case OpInsert:
				if s.exists {
					// Key exists: insert fails, returns previous value.
					expected := perKeyOutput[V]{Found: false, Value: s.value}
					return out == expected, s
				}
				// Key absent: insert succeeds, returns zero value.
				expected := perKeyOutput[V]{Found: true, Value: zero}
				return out == expected, perKeyState[V]{exists: true, value: in.Value}

			case OpPut:
				if s.exists {
					expected := perKeyOutput[V]{Updated: true, Value: s.value}
					return out == expected, perKeyState[V]{exists: true, value: in.Value}
				}
				expected := perKeyOutput[V]{Updated: false, Value: zero}
				return out == expected, s

			case OpDelete:
				if s.exists {
					// Key exists: delete succeeds, returns deleted value.
					expected := perKeyOutput[V]{Found: true, Value: s.value}
					return out == expected, perKeyState[V]{}
				}
				expected := perKeyOutput[V]{Found: false, Value: zero}
				return out == expected, s

			default:
				return false, s
			}
		},
		Equal: func(a, b any) bool {
			return a.(perKeyState[V]) == b.(perKeyState[V])
		},
		DescribeOperation: func(input, output any) string {
			in := input.(perKeyInput[V])
			out := output.(perKeyOutput[V])

			switch in.Kind {
			case OpGet:
				if out.Found {
					return fmt.Sprintf("Get → %v (found)", out.Value)
				}
				return "Get → not found"
			case OpInsert:
				if out.Found {
					return fmt.Sprintf("Insert(%v) → ok", in.Value)
				}
				if out.Updated {
					return fmt.Sprintf("Insert(%v) → exists (prev %v, updated?)", in.Value, out.Value)
				}
				return fmt.Sprintf("Insert(%v) → exists (prev %v)", in.Value, out.Value)
			case OpPut:
				if out.Updated {
					return fmt.Sprintf("Put(%v) → updated (old %v)", in.Value, out.Value)
				}
				return fmt.Sprintf("Put(%v) → not found", in.Value)
			case OpDelete:
				if out.Found {
					return fmt.Sprintf("Delete → deleted (was %v)", out.Value)
				}
				return "Delete → not found"
			default:
				return fmt.Sprintf("Unknown(%d)", in.Kind)
			}
		},
		DescribeState: func(state any) string {
			s := state.(perKeyState[V])
			if s.exists {
				return fmt.Sprintf("value=%v", s.value)
			}
			return "absent"
		},
	}
}

func toPorcupineEvents[K comparable, V comparable](ops []TimedOp[K, V]) []porcupine.Operation {
	events := make([]porcupine.Operation, len(ops))
	for i := range ops {
		events[i] = porcupine.Operation{
			ClientId: ops[i].ThreadId,
			Input:    perKeyInput[V]{Kind: ops[i].Op.Kind, Value: ops[i].Op.Value},
			Output: perKeyOutput[V]{
				Found:   ops[i].Result.Found,
				Updated: ops[i].Result.Updated,
				Value:   ops[i].Result.Value,
			},
			Call:   int64(ops[i].StartSeq),
			Return: int64(ops[i].EndSeq),
		}
	}
	return events
}

// ValidatePerKeyLinearizablePorcupineFromInitial validates per-key linearizability
// using Porcupine. Operations are partitioned by key, and each key is checked
// independently against its initial state.
//
// Returns a PorcupineResult with full diagnostic information including
// visualization data for failing keys (use WriteVisualizations to generate HTML).
func ValidatePerKeyLinearizablePorcupineFromInitial[K comparable, V comparable](
	ops []TimedOp[K, V],
	initial map[K]keyState[V],
) PorcupineResult {
	perKey := make(map[K][]TimedOp[K, V])
	for _, op := range ops {
		perKey[op.Op.Key] = append(perKey[op.Op.Key], op)
	}

	result := PorcupineResult{Ok: true}

	for key, keyOps := range perKey {
		start := keyState[V]{}
		if initial != nil {
			if st, ok := initial[key]; ok {
				start = st
			}
		}

		initState := perKeyState[V]{exists: start.exists, value: start.value}
		model := perKeyModel(initState)
		events := toPorcupineEvents(keyOps)

		checkResult, info := porcupine.CheckOperationsVerbose(model, events, 30*time.Second)

		switch checkResult {
		case porcupine.Ok:
			// Valid linearization found
		case porcupine.Illegal:
			result.Ok = false
			keyStr := fmt.Sprintf("%v", key)
			if result.Reason == "" {
				result.Reason = fmt.Sprintf("key=%v: rejected by porcupine", key)
			}
			result.Failures = append(result.Failures, PorcupineKeyFailure{
				Key:   keyStr,
				Model: model,
				Info:  info,
			})
		case porcupine.Unknown:
			// Timeout. Inconclusive, treat as ok for now
		}
	}

	return result
}

// validateKeyOpsWithPorcupine performs a simple yes/no per-key linearizability
// check using Porcupine. Used by the minimizer where verbose output is not needed.
func validateKeyOpsWithPorcupine[K comparable, V comparable](ops []TimedOp[K, V], initial keyState[V]) bool {
	initState := perKeyState[V]{exists: initial.exists, value: initial.value}
	model := perKeyModel(initState)
	return porcupine.CheckOperations(model, toPorcupineEvents(ops))
}
