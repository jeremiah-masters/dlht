package benchmarks

import (
	"testing"
)

func testMapAdapter[K comparable, V any](t *testing.T, name string, m MapAdapter[K, V], k1, k2 K, v1, v2 V) {
	t.Helper()

	// Insert new key
	if !m.Insert(k1, v1) {
		t.Fatalf("%s: Insert(%v) should succeed on empty map", name, k1)
	}

	// Insert duplicate should fail
	if m.Insert(k1, v2) {
		t.Fatalf("%s: Insert(%v) should fail on existing key", name, k1)
	}

	// Get existing
	got, ok := m.Get(k1)
	if !ok {
		t.Fatalf("%s: Get(%v) should find key", name, k1)
	}
	if any(got) != any(v1) {
		t.Fatalf("%s: Get(%v) = %v, want %v", name, k1, got, v1)
	}

	// Get missing
	_, ok = m.Get(k2)
	if ok {
		t.Fatalf("%s: Get(%v) should not find missing key", name, k2)
	}

	// Put existing
	if !m.Put(k1, v2) {
		t.Fatalf("%s: Put(%v) should succeed on existing key", name, k1)
	}

	// Verify update
	got, ok = m.Get(k1)
	if !ok || any(got) != any(v2) {
		t.Fatalf("%s: Get after Put = (%v, %v), want (%v, true)", name, got, ok, v2)
	}

	// Delete existing
	if !m.Delete(k1) {
		t.Fatalf("%s: Delete(%v) should succeed on existing key", name, k1)
	}

	// Get deleted
	_, ok = m.Get(k1)
	if ok {
		t.Fatalf("%s: Get(%v) should not find deleted key", name, k1)
	}

	// Delete missing
	if m.Delete(k2) {
		t.Fatalf("%s: Delete(%v) should fail on missing key", name, k2)
	}
}

func TestAdapters_Uint64(t *testing.T) {
	for _, f := range Uint64MapFactories() {
		t.Run(f.Name, func(t *testing.T) {
			m := f.New(16)
			testMapAdapter(t, f.Name, m, uint64(42), uint64(99), uint64(100), uint64(200))
			m.Close()
		})
	}
}

func TestAdapters_String(t *testing.T) {
	for _, f := range StringMapFactories() {
		t.Run(f.Name, func(t *testing.T) {
			m := f.New(16)
			testMapAdapter(t, f.Name, m, "hello", "world", "val1", "val2")
			m.Close()
		})
	}
}
