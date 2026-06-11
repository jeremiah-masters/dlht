package adapters

import (
	"sync"

	"github.com/cornelk/hashmap"
)

// cornelkHashable mirrors the unexported hashable constraint from cornelk/hashmap.
type cornelkHashable interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 | ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~uintptr | ~float32 | ~float64 | ~string
}

type cornelkAdapter[K cornelkHashable, V any] struct {
	m         *hashmap.Map[K, V]
	computeMu sync.Mutex
}

func NewCornelkAdapter[K cornelkHashable, V any](capacity int) MapAdapter[K, V] {
	if capacity > 0 {
		return &cornelkAdapter[K, V]{m: hashmap.NewSized[K, V](uintptr(capacity))}
	}
	return &cornelkAdapter[K, V]{m: hashmap.New[K, V]()}
}

func (a *cornelkAdapter[K, V]) Name() string        { return "cornelk" }
func (a *cornelkAdapter[K, V]) Get(key K) (V, bool) { return a.m.Get(key) }
func (a *cornelkAdapter[K, V]) Insert(key K, val V) bool {
	return a.m.Insert(key, val)
}
func (a *cornelkAdapter[K, V]) Delete(key K) bool { return a.m.Del(key) }

// Put: cornelk has no conditional update. Get+Set to approximate update-if-exists.
func (a *cornelkAdapter[K, V]) Put(key K, val V) bool {
	if _, ok := a.m.Get(key); ok {
		a.m.Set(key, val)
		return true
	}
	return false
}

func (a *cornelkAdapter[K, V]) Range(yield func(K, V) bool) {
	a.m.Range(yield)
}

func (a *cornelkAdapter[K, V]) LoadOrCompute(key K, fn func() (V, bool)) (V, bool) {
	if v, ok := a.m.Get(key); ok {
		return v, true
	}
	val, save := fn()
	if !save {
		return val, false
	}
	if actual, loaded := a.m.GetOrInsert(key, val); loaded {
		return actual, true
	}
	return val, false
}

func (a *cornelkAdapter[K, V]) LoadOrComputeOnce(key K, fn func() (V, bool)) (V, bool) {
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
	if actual, loaded := a.m.GetOrInsert(key, val); loaded {
		return actual, true
	}
	return val, false
}

func (a *cornelkAdapter[K, V]) Size() int { return a.m.Len() }
func (a *cornelkAdapter[K, V]) Close()    {}
