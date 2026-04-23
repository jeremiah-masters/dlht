package adapters

import cmap "github.com/orcaman/concurrent-map"

type orcamanAdapter[K ~string, V any] struct {
	m cmap.ConcurrentMap
}

func NewOrcamanAdapter[K ~string, V any](capacity int) MapAdapter[K, V] {
	_ = capacity // orcaman/concurrent-map does not expose capacity tuning.
	return &orcamanAdapter[K, V]{m: cmap.New()}
}

func (a *orcamanAdapter[K, V]) Name() string { return "orcaman" }

func (a *orcamanAdapter[K, V]) Get(key K) (V, bool) {
	val, ok := a.m.Get(string(key))
	if !ok {
		var zero V
		return zero, false
	}
	return val.(V), true
}

func (a *orcamanAdapter[K, V]) Insert(key K, val V) bool {
	return a.m.SetIfAbsent(string(key), val)
}

func (a *orcamanAdapter[K, V]) Delete(key K) bool {
	_, existed := a.m.Pop(string(key))
	return existed
}

// Put approximates update-if-exists via Get+Set.
func (a *orcamanAdapter[K, V]) Put(key K, val V) bool {
	sk := string(key)
	if _, ok := a.m.Get(sk); ok {
		a.m.Set(sk, val)
		return true
	}
	return false
}

func (a *orcamanAdapter[K, V]) Size() int { return a.m.Count() }
func (a *orcamanAdapter[K, V]) Close()    {}
