package pbt

import (
	"testing"

	"github.com/jeremiah-masters/dlht"

	"pgregory.net/rapid"
)

func runSequentialModelCheck[K dlht.Key](t *rapid.T, keyGen *rapid.Generator[K]) {
	opsLen := rapid.IntRange(1, 256).Draw(t, "opsLen")
	valGen := rapid.IntRange(-1_000_000, 1_000_000)
	ops := GenOpSequence(keyGen, valGen, MixBalanced, opsLen, opsLen).Draw(t, "ops")

	m := dlht.New[K, int](dlht.Options{InitialSize: 16})
	model := make(map[K]int)
	var zero int

	for _, o := range ops {
		switch o.Kind {
		case OpGet:
			mv, mok := model[o.Key]
			v, ok := m.Get(o.Key)
			if ok != mok || (ok && v != mv) || (!ok && v != zero) {
				t.Fatalf("get mismatch key=%v model_found=%v map_found=%v model_val=%v map_val=%v",
					o.Key, mok, ok, mv, v)
			}
		case OpInsert:
			_, existed := model[o.Key]
			prev, ok := m.Insert(o.Key, o.Value)
			if existed {
				if ok || prev != model[o.Key] {
					t.Fatalf("insert mismatch key=%v existed=true ok=%v prev=%v model=%v",
						o.Key, ok, prev, model[o.Key])
				}
			} else {
				if !ok || prev != zero {
					t.Fatalf("insert mismatch key=%v existed=false ok=%v prev=%v expected_prev=%v",
						o.Key, ok, prev, zero)
				}
				model[o.Key] = o.Value
			}
		case OpPut:
			_, existed := model[o.Key]
			old, ok := m.Put(o.Key, o.Value)
			if existed {
				if !ok || old != model[o.Key] {
					t.Fatalf("put mismatch key=%v existed=true ok=%v old=%v model=%v",
						o.Key, ok, old, model[o.Key])
				}
				model[o.Key] = o.Value
			} else {
				if ok || old != zero {
					t.Fatalf("put mismatch key=%v existed=false ok=%v old=%v expected_old=%v",
						o.Key, ok, old, zero)
				}
			}
		case OpDelete:
			val, existed := model[o.Key]
			deleted, ok := m.Delete(o.Key)
			if existed {
				if !ok || deleted != val {
					t.Fatalf("delete mismatch key=%v existed=true ok=%v deleted=%v model=%v",
						o.Key, ok, deleted, val)
				}
				delete(model, o.Key)
			} else {
				if ok || deleted != zero {
					t.Fatalf("delete mismatch key=%v existed=false ok=%v deleted=%v expected_deleted=%v",
						o.Key, ok, deleted, zero)
				}
			}
		}
	}
}

func TestPBTSequentialModel(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		keyspace := rapid.IntRange(1, 32).Draw(t, "keyspace")
		runSequentialModelCheck(t, GenStringKey(keyspace))
	})
}

func TestPBTSequentialModelUint64(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		keyspace := rapid.IntRange(1, 32).Draw(t, "keyspace")
		runSequentialModelCheck(t, GenUint64Key(keyspace))
	})
}
