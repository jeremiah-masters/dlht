package adapters

import (
	"cmp"
	"sync"
	"unsafe"

	"github.com/alphadose/haxmap"
)

// haxmapHashable mirrors the unexported hashable constraint from alphadose/haxmap.
type haxmapHashable interface {
	cmp.Ordered | uintptr | ~unsafe.Pointer
}

type haxMapAdapter[K haxmapHashable, V any] struct {
	m         *haxmap.Map[K, V]
	computeMu sync.Mutex
}

func NewHaxMapAdapter[K haxmapHashable, V any](capacity int) MapAdapter[K, V] {
	if capacity > 0 {
		return &haxMapAdapter[K, V]{m: haxmap.New[K, V](uintptr(capacity))}
	}
	return &haxMapAdapter[K, V]{m: haxmap.New[K, V]()}
}

func (a *haxMapAdapter[K, V]) Name() string        { return "haxmap" }
func (a *haxMapAdapter[K, V]) Get(key K) (V, bool) { return a.m.Get(key) }

func (a *haxMapAdapter[K, V]) Insert(key K, val V) bool {
	_, loaded := a.m.GetOrSet(key, val)
	return !loaded
}

func (a *haxMapAdapter[K, V]) Delete(key K) bool {
	_, existed := a.m.GetAndDel(key)
	return existed
}

func (a *haxMapAdapter[K, V]) Put(key K, val V) bool {
	_, swapped := a.m.Swap(key, val)
	return swapped
}

func (a *haxMapAdapter[K, V]) Range(yield func(K, V) bool) {
	a.m.ForEach(func(k K, v V) bool {
		return yield(k, v)
	})
}

func (a *haxMapAdapter[K, V]) LoadOrCompute(key K, fn func() (V, bool)) (V, bool) {
	if v, ok := a.m.Get(key); ok {
		return v, true
	}
	val, save := fn()
	if !save {
		return val, false
	}
	actual, loaded := a.m.GetOrSet(key, val)
	if loaded {
		return actual, true
	}
	return val, false
}

func (a *haxMapAdapter[K, V]) LoadOrComputeOnce(key K, fn func() (V, bool)) (V, bool) {
	if v, ok := a.m.Get(key); ok {
		return v, true
	}
	a.computeMu.Lock()
	defer a.computeMu.Unlock()
	if v, ok := a.m.Get(key); ok {
		return v, true
	}
	val, save := fn()
	if !save {
		return val, false
	}
	actual, loaded := a.m.GetOrSet(key, val)
	if loaded {
		return actual, true
	}
	return val, false
}

func (a *haxMapAdapter[K, V]) Size() int { return int(a.m.Len()) }
func (a *haxMapAdapter[K, V]) Close()    {}
