package tests

import (
	"fmt"
	"math/rand/v2"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jeremiah-masters/dlht"

	"github.com/anishathalye/porcupine"
)

const (
	OpGet = iota
	OpInsert
	OpPut
	OpDelete
)

type DLHTInput struct {
	Op    int
	Key   string
	Value int
}

type DLHTOutput struct {
	Found   bool
	Value   int
	Updated bool
}

func (input DLHTInput) String() string {
	switch input.Op {
	case OpGet:
		return fmt.Sprintf("Get(%s)", input.Key)
	case OpInsert:
		return fmt.Sprintf("Insert(%s, %d)", input.Key, input.Value)
	case OpPut:
		return fmt.Sprintf("Put(%s, %d)", input.Key, input.Value)
	case OpDelete:
		return fmt.Sprintf("Delete(%s)", input.Key)
	default:
		return fmt.Sprintf("Unknown(%d)", input.Op)
	}
}

// String returns a string representation of the output for debugging
func (output DLHTOutput) String() string {
	return fmt.Sprintf("{Found: %t, Value: %d, Updated: %t}", output.Found, output.Value, output.Updated)
}

// PerKeyState is the state of a single key in the Porcupine model.
type PerKeyState struct {
	Exists bool
	Value  int
}

// DLHTModel is a per-key Porcupine model for DLHT operations. Operations are
// partitioned by key before checking; each partition is checked independently
// against this model. Use CheckPerKeyLinearizability to run the full check.
var DLHTModel = porcupine.Model{
	Init: func() interface{} {
		return PerKeyState{}
	},
	Step: func(state, input, output interface{}) (bool, interface{}) {
		st := state.(PerKeyState)
		inp := input.(DLHTInput)
		out := output.(DLHTOutput)

		switch inp.Op {
		case OpGet:
			if st.Exists {
				return out.Found && out.Value == st.Value, st
			}
			return !out.Found && out.Value == 0, st
		case OpInsert:
			if st.Exists {
				return !out.Found && out.Value == st.Value, st
			}
			if out.Found && out.Value == 0 {
				return true, PerKeyState{Exists: true, Value: inp.Value}
			}
			return false, st
		case OpPut:
			if st.Exists {
				if out.Updated && out.Value == st.Value {
					return true, PerKeyState{Exists: true, Value: inp.Value}
				}
				return false, st
			}
			return !out.Updated && out.Value == 0, st
		case OpDelete:
			if st.Exists {
				if out.Found && out.Value == st.Value {
					return true, PerKeyState{}
				}
				return false, st
			}
			return !out.Found && out.Value == 0, st
		default:
			return false, st
		}
	},
	Equal: func(a, b interface{}) bool {
		return a.(PerKeyState) == b.(PerKeyState)
	},
	DescribeOperation: func(input, output interface{}) string {
		inp := input.(DLHTInput)
		out := output.(DLHTOutput)

		switch inp.Op {
		case OpGet:
			if out.Found {
				return fmt.Sprintf("Get(%s) -> %d (found)", inp.Key, out.Value)
			}
			return fmt.Sprintf("Get(%s) -> not found", inp.Key)
		case OpInsert:
			if out.Found {
				return fmt.Sprintf("Insert(%s, %d) -> ok", inp.Key, inp.Value)
			}
			if out.Value != 0 {
				return fmt.Sprintf("Insert(%s, %d) -> key exists (prev %d)", inp.Key, inp.Value, out.Value)
			}
			return fmt.Sprintf("Insert(%s, %d) -> key exists", inp.Key, inp.Value)
		case OpPut:
			if out.Updated {
				return fmt.Sprintf("Put(%s, %d) -> updated (old %d)", inp.Key, inp.Value, out.Value)
			}
			return fmt.Sprintf("Put(%s, %d) -> key not found", inp.Key, inp.Value)
		case OpDelete:
			if out.Found {
				return fmt.Sprintf("Delete(%s) -> deleted (was %d)", inp.Key, out.Value)
			}
			return fmt.Sprintf("Delete(%s) -> not found", inp.Key)
		default:
			return "Unknown operation"
		}
	},
	DescribeState: func(state interface{}) string {
		st := state.(PerKeyState)
		if st.Exists {
			return fmt.Sprintf("value=%d", st.Value)
		}
		return "absent"
	},
}

// PerKeyFailure holds failure details for a single key.
type PerKeyFailure struct {
	Key  string
	Info porcupine.LinearizationInfo
}

// CheckPerKeyLinearizability partitions operations by key and checks each
// independently using Porcupine. Returns ok=true if all keys are linearizable.
// On failure, returns the list of failing keys with their LinearizationInfo
// (use porcupine.Visualize(DLHTModel, failure.Info, w) to generate HTML).
func CheckPerKeyLinearizability(operations []porcupine.Operation, timeout time.Duration) (bool, []PerKeyFailure) {
	byKey := make(map[string][]porcupine.Operation)
	for _, op := range operations {
		k := op.Input.(DLHTInput).Key
		byKey[k] = append(byKey[k], op)
	}

	var mu sync.Mutex
	var failures []PerKeyFailure
	var wg sync.WaitGroup

	for key, ops := range byKey {
		wg.Add(1)
		go func(key string, ops []porcupine.Operation) {
			defer wg.Done()
			result, info := porcupine.CheckOperationsVerbose(DLHTModel, ops, timeout)
			if result == porcupine.Illegal {
				mu.Lock()
				failures = append(failures, PerKeyFailure{Key: key, Info: info})
				mu.Unlock()
			}
		}(key, ops)
	}
	wg.Wait()

	return len(failures) == 0, failures
}

func ExecuteOperation(m *dlht.Map[string, int], input DLHTInput) DLHTOutput {
	switch input.Op {
	case OpGet:
		value, found := m.Get(input.Key)
		return DLHTOutput{Found: found, Value: value, Updated: false}
	case OpInsert:
		prevValue, success := m.Insert(input.Key, input.Value)
		return DLHTOutput{Found: success, Value: prevValue, Updated: false}
	case OpPut:
		oldValue, updated := m.Put(input.Key, input.Value)
		return DLHTOutput{Found: false, Value: oldValue, Updated: updated}
	case OpDelete:
		deletedValue, deleted := m.Delete(input.Key)
		return DLHTOutput{Found: deleted, Value: deletedValue, Updated: false}
	default:
		return DLHTOutput{Found: false, Value: 0, Updated: false}
	}
}

type TestConfig struct {
	Name            string
	NumClients      int
	NumOpsPerClient int
	NumKeys         int
	InitialSize     uint64
	OpDistribution  []float64
	Duration        time.Duration
	ContentionLevel string
	Description     string
}

type WorkloadPattern struct {
	Name        string
	Description string
	Generator   func(clientId, opId int, config TestConfig) DLHTInput
}

func selectOperation(distribution []float64, key string, value int) DLHTInput {
	r := rand.Float64()
	cumulative := 0.0

	for i, prob := range distribution {
		cumulative += prob
		if r <= cumulative {
			switch i {
			case 0:
				return DLHTInput{OpGet, key, 0}
			case 1:
				return DLHTInput{OpInsert, key, value}
			case 2:
				return DLHTInput{OpPut, key, value}
			case 3:
				return DLHTInput{OpDelete, key, 0}
			}
		}
	}
	return DLHTInput{OpGet, key, 0}
}

func RunLinearizabilityTest(t *testing.T, config TestConfig, pattern WorkloadPattern) {
	m := dlht.New[string, int](dlht.Options{InitialSize: config.InitialSize})

	localOps := make([][]porcupine.Operation, config.NumClients)
	var wg sync.WaitGroup

	startTime := time.Now()

	for clientId := range config.NumClients {
		wg.Add(1)
		go func(cid int) {
			defer wg.Done()
			ops := make([]porcupine.Operation, 0, config.NumOpsPerClient)

			for opId := range config.NumOpsPerClient {
				input := pattern.Generator(cid, opId, config)

				var callTime int64
				atomic.StoreInt64(&callTime, time.Since(startTime).Nanoseconds())

				output := ExecuteOperation(m, input)

				returnTime := time.Since(startTime).Nanoseconds()

				ops = append(ops, porcupine.Operation{
					ClientId: cid,
					Input:    input,
					Call:     callTime,
					Output:   output,
					Return:   returnTime,
				})

				switch config.ContentionLevel {
				case "high":
					if rand.IntN(5) == 0 {
						runtime.Gosched()
					}
				case "medium":
					if rand.IntN(20) == 0 {
						time.Sleep(time.Microsecond)
					}
				case "low":
					if rand.IntN(100) == 0 {
						time.Sleep(time.Microsecond)
					}
				}
			}
			localOps[cid] = ops
		}(clientId)
	}

	wg.Wait()

	total := 0
	for _, ops := range localOps {
		total += len(ops)
	}
	operations := make([]porcupine.Operation, 0, total)
	for _, ops := range localOps {
		operations = append(operations, ops...)
	}

	ok, failures := CheckPerKeyLinearizability(operations, 30*time.Second)
	if !ok {
		for _, f := range failures {
			timestamp := time.Now().Format("20060102_150405")
			filename := fmt.Sprintf("violation_%s_%s_key_%s_%s.html",
				config.Name, pattern.Name, f.Key, timestamp)
			file, err := os.Create(filename)
			if err == nil {
				porcupine.Visualize(DLHTModel, f.Info, file)
				file.Close()
				t.Logf("visualization written to %s", filename)
			}
			t.Errorf("linearizability violation on key %q; see %s", f.Key, filename)
		}
	}
}
