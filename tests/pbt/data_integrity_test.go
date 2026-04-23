package pbt

import (
	"fmt"
	"sync"
	"testing"

	"github.com/jeremiah-masters/dlht"

	"pgregory.net/rapid"
)

type MagicValue struct {
	Magic1    uint64
	WorkerID  int
	Iteration int
	Payload   [8]uint64
	Magic2    uint64
}

const (
	Magic1Const = 0xDEADBEEFCAFEBABE
	Magic2Const = 0xFEEDFACEDEADC0DE
)

func newMagicValue(workerID, iteration int) *MagicValue {
	mv := &MagicValue{
		Magic1:    Magic1Const,
		WorkerID:  workerID,
		Iteration: iteration,
		Magic2:    Magic2Const,
	}
	for i := range mv.Payload {
		mv.Payload[i] = uint64(workerID*10000 + iteration*10 + i)
	}
	return mv
}

func (mv *MagicValue) Validate() bool {
	if mv.Magic1 != Magic1Const || mv.Magic2 != Magic2Const {
		return false
	}
	for i := range mv.Payload {
		expected := uint64(mv.WorkerID*10000 + mv.Iteration*10 + i)
		if mv.Payload[i] != expected {
			return false
		}
	}
	return true
}

type ConsistentValue struct {
	Key       string
	Checksum  int
	Iteration int
}

func (cv *ConsistentValue) Validate() bool {
	return cv.Checksum == len(cv.Key)+cv.Iteration
}

func newConsistentValue(key string, iteration int) *ConsistentValue {
	return &ConsistentValue{
		Key:       key,
		Checksum:  len(key) + iteration,
		Iteration: iteration,
	}
}

func mustUpsert[V any](t *rapid.T, m *dlht.Map[string, V], key string, val V, workerID, iter int) {
	// Put fails on absent keys and subsequent Inserts can fail if another writer inserted first.
	// A final Put handles that race without silently dropping this write.
	if _, ok := m.Put(key, val); ok {
		return
	}
	if _, ok := m.Insert(key, val); ok {
		return
	}
	if _, ok := m.Put(key, val); ok {
		return
	}
	t.Fatalf("writer failed to publish value key=%s worker=%d iter=%d", key, workerID, iter)
}

func TestPBTDataIntegrityMagicValue(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		keyspace := rapid.IntRange(4, 32).Draw(t, "keyspace")
		writers := rapid.IntRange(2, 6).Draw(t, "writers")
		readers := rapid.IntRange(2, 6).Draw(t, "readers")
		opsPerWorker := rapid.IntRange(20, 100).Draw(t, "opsPerWorker")
		initialSize := rapid.Uint64Range(4, 64).Draw(t, "initialSize")

		keys := make([]string, keyspace)
		for i := 0; i < keyspace; i++ {
			keys[i] = fmt.Sprintf("key_%d", i)
		}

		m := dlht.New[string, *MagicValue](dlht.Options{InitialSize: initialSize})
		writerKeys := make([][]int, writers)
		readerKeys := make([][]int, readers)
		for i := 0; i < writers; i++ {
			writerKeys[i] = rapid.SliceOfN(rapid.IntRange(0, keyspace-1), opsPerWorker, opsPerWorker).Draw(t, fmt.Sprintf("writerKeys_%d", i))
		}
		for i := 0; i < readers; i++ {
			readerKeys[i] = rapid.SliceOfN(rapid.IntRange(0, keyspace-1), opsPerWorker, opsPerWorker).Draw(t, fmt.Sprintf("readerKeys_%d", i))
		}

		var wg sync.WaitGroup
		for i := 0; i < writers; i++ {
			wg.Add(1)
			go func(workerID int, indices []int) {
				defer wg.Done()
				for j, idx := range indices {
					key := keys[idx]
					val := newMagicValue(workerID, j)
					mustUpsert(t, m, key, val, workerID, j)
				}
			}(i, writerKeys[i])
		}
		for i := 0; i < readers; i++ {
			wg.Add(1)
			go func(indices []int) {
				defer wg.Done()
				for _, idx := range indices {
					key := keys[idx]
					if v, ok := m.Get(key); ok && v != nil && !v.Validate() {
						t.Fatalf("magic value corruption")
					}
				}
			}(readerKeys[i])
		}
		wg.Wait()
	})
}

func TestPBTDataIntegrityConsistentValue(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		keyspace := rapid.IntRange(4, 32).Draw(t, "keyspace")
		writers := rapid.IntRange(2, 6).Draw(t, "writers")
		readers := rapid.IntRange(2, 6).Draw(t, "readers")
		opsPerWorker := rapid.IntRange(20, 100).Draw(t, "opsPerWorker")
		initialSize := rapid.Uint64Range(4, 64).Draw(t, "initialSize")

		keys := make([]string, keyspace)
		for i := 0; i < keyspace; i++ {
			keys[i] = fmt.Sprintf("key_%d", i)
		}

		m := dlht.New[string, *ConsistentValue](dlht.Options{InitialSize: initialSize})
		writerKeys := make([][]int, writers)
		readerKeys := make([][]int, readers)
		for i := 0; i < writers; i++ {
			writerKeys[i] = rapid.SliceOfN(rapid.IntRange(0, keyspace-1), opsPerWorker, opsPerWorker).Draw(t, fmt.Sprintf("writerKeys_%d", i))
		}
		for i := 0; i < readers; i++ {
			readerKeys[i] = rapid.SliceOfN(rapid.IntRange(0, keyspace-1), opsPerWorker, opsPerWorker).Draw(t, fmt.Sprintf("readerKeys_%d", i))
		}

		var wg sync.WaitGroup
		for i := 0; i < writers; i++ {
			wg.Add(1)
			go func(workerID int, indices []int) {
				defer wg.Done()
				for j, idx := range indices {
					key := keys[idx]
					val := newConsistentValue(key, workerID*opsPerWorker+j)
					mustUpsert(t, m, key, val, workerID, j)
				}
			}(i, writerKeys[i])
		}
		for i := 0; i < readers; i++ {
			wg.Add(1)
			go func(indices []int) {
				defer wg.Done()
				for _, idx := range indices {
					key := keys[idx]
					if v, ok := m.Get(key); ok && v != nil && !v.Validate() {
						t.Fatalf("consistent value corruption")
					}
				}
			}(readerKeys[i])
		}
		wg.Wait()
	})
}
