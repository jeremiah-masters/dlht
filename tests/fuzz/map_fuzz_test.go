package fuzz

import (
	"encoding/binary"
	"testing"

	"github.com/jeremiah-masters/dlht"
)

const (
	opInsert uint8 = iota
	opDelete
	opGet
	opContains
	opPut
	opCount
)

const (
	defaultInitialSize = 16
	fuzzKeySpace       = uint64(256)
	maxFuzzOps         = 512
	hotKeySpace        = uint64(16)
	longSequenceMinOps = 256
	longSequenceJitter = 128
)

func FuzzDLHTNativeOpsParity(f *testing.F) {
	f.Add([]byte{
		opInsert, 1, 0, 0, 0, 10, 0, 0, 0,
		opGet, 1, 0, 0, 0, 0, 0, 0, 0,
		opContains, 1, 0, 0, 0, 0, 0, 0, 0,
		opDelete, 1, 0, 0, 0, 0, 0, 0, 0,
	})
	f.Add([]byte{
		opPut, 2, 0, 0, 0, 8, 0, 0, 0,
		opInsert, 2, 0, 0, 0, 3, 0, 0, 0,
		opPut, 2, 0, 0, 0, 9, 0, 0, 0,
		opGet, 2, 0, 0, 0, 0, 0, 0, 0,
	})
	f.Add([]byte{
		opInsert, 7, 0, 0, 0, 4, 0, 0, 0,
		opInsert, 7, 0, 0, 0, 5, 0, 0, 0,
		opDelete, 7, 0, 0, 0, 0, 0, 0, 0,
		opContains, 7, 0, 0, 0, 0, 0, 0, 0,
	})

	f.Fuzz(func(t *testing.T, data []byte) {
		model := make(map[uint64]uint32)
		m := dlht.New[uint64, uint32](dlht.Options{InitialSize: defaultInitialSize})

		pos := 0
		for step := 0; step < maxFuzzOps && pos < len(data); step++ {
			op := data[pos] % opCount
			pos++

			keyRaw, ok := readU32(data, &pos)
			if !ok {
				return
			}
			val, ok := readU32(data, &pos)
			if !ok {
				return
			}
			key := normalizeKey(uint64(keyRaw))

			checkNativeOp(t, m, model, step, op, key, val)
		}

		assertModelParity(t, m, model)
	})
}

func FuzzDLHTNativeOpsLongSequence(f *testing.F) {
	// Short and medium seeds are intentionally kept; this fuzz target expands
	// any input into a long deterministic operation trace.
	f.Add([]byte{0x00, 0x01, 0x02, 0x03})
	f.Add([]byte{0x10, 0x20, 0x30, 0x40, 0x50, 0x60, 0x70, 0x80})
	f.Add([]byte{0xff, 0x00, 0xaa, 0x55, 0x12, 0x34, 0x56, 0x78, 0x9a})

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) == 0 {
			return
		}

		model := make(map[uint64]uint32)
		m := dlht.New[uint64, uint32](dlht.Options{InitialSize: defaultInitialSize})

		// Keep traces long but bounded for throughput.
		totalOps := longSequenceMinOps + int(data[0]%longSequenceJitter)
		if totalOps > maxFuzzOps {
			totalOps = maxFuzzOps
		}

		pos := 1
		for step := 0; step < totalOps; step++ {
			opByte := data[pos%len(data)]
			pos++

			keyRaw := readU32Circular(data, &pos)
			valRaw := readU32Circular(data, &pos)

			op := selectLongSequenceOp(opByte)
			key := deriveLongSequenceKey(opByte, keyRaw, step)
			val := deriveLongSequenceValue(valRaw, step)

			checkNativeOp(t, m, model, step, op, key, val)
		}

		assertModelParity(t, m, model)
	})
}

func checkNativeOp(t *testing.T, m *dlht.Map[uint64, uint32], model map[uint64]uint32, step int, op uint8, key uint64, val uint32) {
	t.Helper()

	switch op {
	case opInsert:
		expPrev, existed := model[key]
		gotPrev, gotOK := m.Insert(key, val)
		if existed {
			if gotOK || gotPrev != expPrev {
				t.Fatalf("step=%d op=Insert key=%d: expected (prev=%d, ok=false), got (prev=%d, ok=%t)", step, key, expPrev, gotPrev, gotOK)
			}
			return
		}
		if !gotOK || gotPrev != 0 {
			t.Fatalf("step=%d op=Insert key=%d: expected (prev=0, ok=true), got (prev=%d, ok=%t)", step, key, gotPrev, gotOK)
		}
		model[key] = val

	case opDelete:
		expVal, existed := model[key]
		gotVal, gotOK := m.Delete(key)
		if existed {
			if !gotOK || gotVal != expVal {
				t.Fatalf("step=%d op=Delete key=%d: expected (value=%d, ok=true), got (value=%d, ok=%t)", step, key, expVal, gotVal, gotOK)
			}
			delete(model, key)
			return
		}
		if gotOK || gotVal != 0 {
			t.Fatalf("step=%d op=Delete key=%d: expected (value=0, ok=false), got (value=%d, ok=%t)", step, key, gotVal, gotOK)
		}

	case opGet:
		expVal, expOK := model[key]
		gotVal, gotOK := m.Get(key)
		if gotOK != expOK || (gotOK && gotVal != expVal) {
			t.Fatalf("step=%d op=Get key=%d: expected (value=%d, ok=%t), got (value=%d, ok=%t)", step, key, expVal, expOK, gotVal, gotOK)
		}

	case opContains:
		_, expOK := model[key]
		gotOK := m.Contains(key)
		if gotOK != expOK {
			t.Fatalf("step=%d op=Contains key=%d: expected %t, got %t", step, key, expOK, gotOK)
		}

	case opPut:
		expOld, existed := model[key]
		gotOld, gotOK := m.Put(key, val)
		if existed {
			if !gotOK || gotOld != expOld {
				t.Fatalf("step=%d op=Put key=%d: expected (old=%d, ok=true), got (old=%d, ok=%t)", step, key, expOld, gotOld, gotOK)
			}
			model[key] = val
			return
		}
		if gotOK || gotOld != 0 {
			t.Fatalf("step=%d op=Put key=%d: expected (old=0, ok=false), got (old=%d, ok=%t)", step, key, gotOld, gotOK)
		}

	default:
		t.Fatalf("unknown operation: %d", op)
	}
}

func assertModelParity(t *testing.T, m *dlht.Map[uint64, uint32], model map[uint64]uint32) {
	t.Helper()

	for i := uint64(0); i < fuzzKeySpace; i++ {
		expVal, expOK := model[i]
		gotVal, gotOK := m.Get(i)
		if gotOK != expOK || (gotOK && gotVal != expVal) {
			t.Fatalf("final consistency: key=%d expected (value=%d, ok=%t), got (value=%d, ok=%t)", i, expVal, expOK, gotVal, gotOK)
		}

		gotContains := m.Contains(i)
		if gotContains != expOK {
			t.Fatalf("final consistency: key=%d expected contains=%t, got contains=%t", i, expOK, gotContains)
		}
	}
}

func normalizeKey(k uint64) uint64 {
	return k % fuzzKeySpace
}

func selectLongSequenceOp(opByte byte) uint8 {
	switch opByte % 10 {
	case 0, 1:
		return opGet
	case 2, 3, 4:
		return opInsert
	case 5, 6, 7:
		return opPut
	case 8:
		return opDelete
	default:
		return opContains
	}
}

func deriveLongSequenceKey(opByte byte, keyRaw uint32, step int) uint64 {
	mixed := uint64(keyRaw) ^ (uint64(step) * 0x9e3779b97f4a7c15)
	if opByte&0x3 != 0 {
		return mixed % hotKeySpace
	}
	return normalizeKey(mixed)
}

func deriveLongSequenceValue(valueRaw uint32, step int) uint32 {
	return valueRaw ^ uint32(step*0x9e3779b1)
}

func readU32(data []byte, pos *int) (uint32, bool) {
	if *pos+4 > len(data) {
		return 0, false
	}
	v := binary.LittleEndian.Uint32(data[*pos : *pos+4])
	*pos += 4
	return v, true
}

func readU32Circular(data []byte, pos *int) uint32 {
	if len(data) == 0 {
		return 0
	}
	var buf [4]byte
	for i := range 4 {
		buf[i] = data[*pos%len(data)]
		*pos++
	}
	return binary.LittleEndian.Uint32(buf[:])
}
