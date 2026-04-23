package tests

import (
	"fmt"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/jeremiah-masters/dlht"
)

func TestMap(t *testing.T) {
	testMap(t, func() *dlht.Map[string, int] {
		return dlht.New[string, int](dlht.Options{})
	})
}

func testMap(t *testing.T, newMap func() *dlht.Map[string, int]) {
	t.Run("GetEmpty", func(t *testing.T) {
		m := newMap()
		for _, s := range mapTestData {
			expectMissing(t, s, 0)(m.Get(s))
			if m.Contains(s) {
				t.Errorf("expected key %q to be missing", s)
			}
		}
	})

	t.Run("Insert", func(t *testing.T) {
		t.Run("All", func(t *testing.T) {
			m := newMap()

			for i, s := range mapTestData {
				expectMissing(t, s, 0)(m.Get(s))
				expectInserted(t, s, 0)(m.Insert(s, i))
				expectPresent(t, s, i)(m.Get(s))
				expectNotInserted(t, s, i)(m.Insert(s, 0))
			}
			for i, s := range mapTestData {
				expectPresent(t, s, i)(m.Get(s))
				expectNotInserted(t, s, i)(m.Insert(s, 0))
			}
		})

		t.Run("ConcurrentUnsharedKeys", func(t *testing.T) {
			m := newMap()

			gmp := runtime.GOMAXPROCS(0)
			var wg sync.WaitGroup
			for i := range gmp {
				wg.Add(1)
				go func(id int) {
					defer wg.Done()

					makeKey := func(s string) string {
						return s + "-" + strconv.Itoa(id)
					}
					for _, s := range mapTestDataSmall {
						key := makeKey(s)
						expectMissing(t, key, 0)(m.Get(key))
						expectInserted(t, key, 0)(m.Insert(key, id))
						expectPresent(t, key, id)(m.Get(key))
						expectNotInserted(t, key, id)(m.Insert(key, 0))
					}
					for _, s := range mapTestDataSmall {
						key := makeKey(s)
						expectPresent(t, key, id)(m.Get(key))
					}
				}(i)
			}
			wg.Wait()
		})

		t.Run("ConcurrentSharedKeys", func(t *testing.T) {
			m := newMap()

			var wins [len(mapTestDataSmall)]atomic.Int32
			gmp := runtime.GOMAXPROCS(0)
			var wg sync.WaitGroup
			for i := range gmp {
				wg.Add(1)
				go func(id int) {
					defer wg.Done()

					for j, s := range mapTestDataSmall {
						previous, inserted := m.Insert(s, id)
						if inserted {
							wins[j].Add(1)
						} else if previous < 0 || previous >= gmp {
							t.Errorf("key %q: got previous=%d, want in [0,%d)", s, previous, gmp)
						}
					}
				}(i)
			}
			wg.Wait()

			for i, s := range mapTestDataSmall {
				if n := wins[i].Load(); n != 1 {
					t.Errorf("key %q had %d successful inserts, want 1", s, n)
				}
				v, ok := m.Get(s)
				if !ok {
					t.Errorf("expected key %q to be present", s)
					continue
				}
				if v < 0 || v >= gmp {
					t.Errorf("key %q has value=%d, want in [0,%d)", s, v, gmp)
				}
			}
		})
	})

	t.Run("Put", func(t *testing.T) {
		t.Run("All", func(t *testing.T) {
			m := newMap()

			for i, s := range mapTestData {
				expectInserted(t, s, 0)(m.Insert(s, i))
			}

			for j := range 3 {
				for i, s := range mapTestData {
					expectPresent(t, s, i+j)(m.Get(s))
					expectUpdated(t, s, i+j, i+j+1)(m.Put(s, i+j+1))
					expectPresent(t, s, i+j+1)(m.Get(s))
				}
			}

			for i, s := range mapTestData {
				expectPresent(t, s, i+3)(m.Get(s))
			}
		})

		t.Run("MissingKey", func(t *testing.T) {
			m := newMap()
			expectNotUpdated(t, "missing", 0, 1)(m.Put("missing", 1))
			expectMissing(t, "missing", 0)(m.Get("missing"))
		})

		t.Run("ConcurrentUnsharedKeys", func(t *testing.T) {
			m := newMap()

			gmp := runtime.GOMAXPROCS(0)
			var wg sync.WaitGroup
			for i := range gmp {
				wg.Add(1)
				go func(id int) {
					defer wg.Done()

					makeKey := func(s string) string {
						return s + "-" + strconv.Itoa(id)
					}
					for _, s := range mapTestDataSmall {
						key := makeKey(s)
						expectInserted(t, key, 0)(m.Insert(key, id))
						expectUpdated(t, key, id, id+1)(m.Put(key, id+1))
						expectPresent(t, key, id+1)(m.Get(key))
					}
					for _, s := range mapTestDataSmall {
						key := makeKey(s)
						expectPresent(t, key, id+1)(m.Get(key))
					}
				}(i)
			}
			wg.Wait()
		})

		t.Run("ConcurrentSharedKeys", func(t *testing.T) {
			m := newMap()

			for i, s := range mapTestData {
				expectInserted(t, s, 0)(m.Insert(s, i))
			}

			gmp := runtime.GOMAXPROCS(0)
			var wg sync.WaitGroup
			for range gmp {
				wg.Add(1)
				go func() {
					defer wg.Done()

					for i, s := range mapTestData {
						old, ok := m.Put(s, i+1)
						if !ok {
							t.Errorf("expected Put(%q, %d) to succeed", s, i+1)
							continue
						}
						if old != i && old != i+1 {
							t.Errorf("Put(%q, %d) old=%d, want %d or %d", s, i+1, old, i, i+1)
						}
					}
				}()
			}
			wg.Wait()

			for i, s := range mapTestData {
				expectPresent(t, s, i+1)(m.Get(s))
			}
		})
	})

	t.Run("Delete", func(t *testing.T) {
		t.Run("All", func(t *testing.T) {
			m := newMap()

			for range 3 {
				for i, s := range mapTestData {
					expectInserted(t, s, 0)(m.Insert(s, i))
					expectPresent(t, s, i)(m.Get(s))
				}
				for i, s := range mapTestData {
					expectPresent(t, s, i)(m.Get(s))
					expectDeleted(t, s, i)(m.Delete(s))
					expectMissing(t, s, 0)(m.Get(s))
					expectNotDeleted(t, s, 0)(m.Delete(s))
				}
				for _, s := range mapTestData {
					expectMissing(t, s, 0)(m.Get(s))
				}
			}
		})

		t.Run("One", func(t *testing.T) {
			m := newMap()

			for i, s := range mapTestData {
				expectInserted(t, s, 0)(m.Insert(s, i))
			}
			expectDeleted(t, mapTestData[15], 15)(m.Delete(mapTestData[15]))
			expectNotDeleted(t, mapTestData[15], 0)(m.Delete(mapTestData[15]))

			for i, s := range mapTestData {
				if i == 15 {
					expectMissing(t, s, 0)(m.Get(s))
				} else {
					expectPresent(t, s, i)(m.Get(s))
				}
			}
		})

		t.Run("Multiple", func(t *testing.T) {
			m := newMap()

			for i, s := range mapTestData {
				expectInserted(t, s, 0)(m.Insert(s, i))
			}
			for _, i := range []int{1, 105, 6, 85} {
				expectDeleted(t, mapTestData[i], i)(m.Delete(mapTestData[i]))
				expectNotDeleted(t, mapTestData[i], 0)(m.Delete(mapTestData[i]))
			}
			for i, s := range mapTestData {
				if i == 1 || i == 105 || i == 6 || i == 85 {
					expectMissing(t, s, 0)(m.Get(s))
				} else {
					expectPresent(t, s, i)(m.Get(s))
				}
			}
		})

		t.Run("ConcurrentUnsharedKeys", func(t *testing.T) {
			m := newMap()

			gmp := runtime.GOMAXPROCS(0)
			var wg sync.WaitGroup
			for i := range gmp {
				wg.Add(1)
				go func(id int) {
					defer wg.Done()

					makeKey := func(s string) string {
						return s + "-" + strconv.Itoa(id)
					}
					for _, s := range mapTestDataSmall {
						key := makeKey(s)
						expectInserted(t, key, 0)(m.Insert(key, id))
					}
					for _, s := range mapTestDataSmall {
						key := makeKey(s)
						expectDeleted(t, key, id)(m.Delete(key))
						expectMissing(t, key, 0)(m.Get(key))
					}
					for _, s := range mapTestDataSmall {
						key := makeKey(s)
						expectMissing(t, key, 0)(m.Get(key))
					}
				}(i)
			}
			wg.Wait()
		})

		t.Run("ConcurrentSharedKeys", func(t *testing.T) {
			m := newMap()

			for i, s := range mapTestData {
				expectInserted(t, s, 0)(m.Insert(s, i))
			}

			gmp := runtime.GOMAXPROCS(0)
			var wg sync.WaitGroup
			for range gmp {
				wg.Add(1)
				go func() {
					defer wg.Done()

					for _, s := range mapTestData {
						m.Delete(s)
						expectMissing(t, s, 0)(m.Get(s))
					}
				}()
			}
			wg.Wait()

			for _, s := range mapTestData {
				expectMissing(t, s, 0)(m.Get(s))
				if m.Contains(s) {
					t.Errorf("expected key %q to be absent", s)
				}
			}
		})
	})
}

func expectPresent[K comparable, V comparable](t *testing.T, key K, want V) func(got V, ok bool) {
	t.Helper()
	return func(got V, ok bool) {
		t.Helper()

		if !ok {
			t.Errorf("expected key %v to be present in map", key)
			return
		}
		if got != want {
			t.Errorf("expected key %v to have value %v, got %v", key, want, got)
		}
	}
}

func expectMissing[K comparable, V comparable](t *testing.T, key K, want V) func(got V, ok bool) {
	t.Helper()
	if want != *new(V) {
		// Keep call-sites uniform to avoid accidental non-zero expectations.
		panic("expectMissing must always have a zero value variable")
	}
	return func(got V, ok bool) {
		t.Helper()

		if ok {
			t.Errorf("expected key %v to be missing from map, got value %v", key, got)
			return
		}
		if got != want {
			t.Errorf("expected missing key %v to be paired with the zero value; got %v", key, got)
		}
	}
}

func expectInserted[K comparable, V comparable](t *testing.T, key K, wantZero V) func(old V, inserted bool) {
	t.Helper()
	if wantZero != *new(V) {
		panic("expectInserted must always have a zero value variable")
	}
	return func(old V, inserted bool) {
		t.Helper()

		if !inserted {
			t.Errorf("expected key %v to be inserted", key)
		}
		if old != wantZero {
			t.Errorf("expected key %v insert old value to be zero; got %v", key, old)
		}
	}
}

func expectNotInserted[K comparable, V comparable](t *testing.T, key K, wantExisting V) func(old V, inserted bool) {
	t.Helper()
	return func(old V, inserted bool) {
		t.Helper()

		if inserted {
			t.Errorf("expected key %v insert to fail because key already exists", key)
			return
		}
		if old != wantExisting {
			t.Errorf("expected key %v to keep existing value %v, got %v", key, wantExisting, old)
		}
	}
}

func expectUpdated[K comparable, V comparable](t *testing.T, key K, wantOld, newV V) func(old V, updated bool) {
	t.Helper()
	return func(old V, updated bool) {
		t.Helper()

		if !updated {
			t.Errorf("expected key %v to be updated to %v", key, newV)
			return
		}
		if old != wantOld {
			t.Errorf("expected key %v old value %v before Put(%v), got %v", key, wantOld, newV, old)
		}
	}
}

func expectNotUpdated[K comparable, V comparable](t *testing.T, key K, wantOld, newV V) func(old V, updated bool) {
	t.Helper()
	return func(old V, updated bool) {
		t.Helper()

		if updated {
			t.Errorf("expected key %v Put(%v) to fail because key is absent", key, newV)
			return
		}
		if old != wantOld {
			t.Errorf("expected key %v missing Put(%v) old value %v, got %v", key, newV, wantOld, old)
		}
	}
}

func expectDeleted[K comparable, V comparable](t *testing.T, key K, wantOld V) func(old V, deleted bool) {
	t.Helper()
	return func(old V, deleted bool) {
		t.Helper()

		if !deleted {
			t.Errorf("expected key %v to be deleted", key)
			return
		}
		if old != wantOld {
			t.Errorf("expected key %v delete old value %v, got %v", key, wantOld, old)
		}
	}
}

func expectNotDeleted[K comparable, V comparable](t *testing.T, key K, wantOld V) func(old V, deleted bool) {
	t.Helper()
	return func(old V, deleted bool) {
		t.Helper()

		if deleted {
			t.Errorf("expected key %v delete to report missing key", key)
			return
		}
		if old != wantOld {
			t.Errorf("expected key %v missing delete old value %v, got %v", key, wantOld, old)
		}
	}
}

var (
	mapTestDataSmall [8]string
	mapTestData      [128]string
)

func init() {
	for i := range mapTestDataSmall {
		mapTestDataSmall[i] = fmt.Sprintf("%b", i)
	}
	for i := range mapTestData {
		mapTestData[i] = fmt.Sprintf("%b", i)
	}
}
