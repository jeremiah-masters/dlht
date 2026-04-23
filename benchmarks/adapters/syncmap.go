package adapters

import "sync"

type syncMapAdapter[K comparable, V any] struct {
	m sync.Map
}

func NewSyncMapAdapter[K comparable, V any]() MapAdapter[K, V] {
	return &syncMapAdapter[K, V]{}
}

func (a *syncMapAdapter[K, V]) Name() string { return "sync.Map" }

func (a *syncMapAdapter[K, V]) Get(key K) (V, bool) {
	val, ok := a.m.Load(key)
	if !ok {
		var zero V
		return zero, false
	}
	return val.(V), true
}

func (a *syncMapAdapter[K, V]) Insert(key K, val V) bool {
	_, loaded := a.m.LoadOrStore(key, val)
	return !loaded // true if inserted (key was new)
}

func (a *syncMapAdapter[K, V]) Delete(key K) bool {
	_, loaded := a.m.LoadAndDelete(key)
	return loaded // true if deleted (key existed)
}

// Put uses Swap which is upsert (always stores). Returns true if key existed before.
// Note: unlike dlht.Put, this stores even if key didn't exist.
func (a *syncMapAdapter[K, V]) Put(key K, val V) bool {
	_, loaded := a.m.Swap(key, val)
	return loaded
}

func (a *syncMapAdapter[K, V]) Size() int {
	n := 0
	a.m.Range(func(_, _ any) bool { n++; return true })
	return n
}

func (a *syncMapAdapter[K, V]) Close() {}
