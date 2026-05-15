package adapters

import (
	"sync"

	cmap "github.com/orcaman/concurrent-map/v2"
)

type orcamanAdapter[K ~string, V any] struct {
	m         cmap.ConcurrentMap[string, V]
	computeMu sync.Mutex
}

func NewOrcamanAdapter[K ~string, V any](capacity int) MapAdapter[K, V] {
	_ = capacity // orcaman/concurrent-map does not expose capacity tuning.
	return &orcamanAdapter[K, V]{m: cmap.New[V]()}
}

func (a *orcamanAdapter[K, V]) Name() string { return "orcaman" }

func (a *orcamanAdapter[K, V]) Get(key K) (V, bool) {
	return a.m.Get(string(key))
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

func (a *orcamanAdapter[K, V]) Range(yield func(K, V) bool) {
	a.m.IterCb(func(key string, v V) {
		yield(K(key), v)
	})
}

func (a *orcamanAdapter[K, V]) LoadOrCompute(key K, fn func() (V, bool)) (V, bool) {
	sk := string(key)
	if v, ok := a.m.Get(sk); ok {
		return v, true
	}
	val, save := fn()
	if !save {
		return val, false
	}
	if a.m.SetIfAbsent(sk, val) {
		return val, false
	}
	v, _ := a.m.Get(sk)
	return v, true
}

func (a *orcamanAdapter[K, V]) LoadOrComputeOnce(key K, fn func() (V, bool)) (V, bool) {
	sk := string(key)
	if v, ok := a.m.Get(sk); ok {
		return v, true
	}
	a.computeMu.Lock()
	defer a.computeMu.Unlock()
	if v, ok := a.m.Get(sk); ok {
		return v, true
	}
	val, save := fn()
	if !save {
		return val, false
	}
	if a.m.SetIfAbsent(sk, val) {
		return val, false
	}
	v, _ := a.m.Get(sk)
	return v, true
}

func (a *orcamanAdapter[K, V]) Size() int { return a.m.Count() }
func (a *orcamanAdapter[K, V]) Close()    {}
