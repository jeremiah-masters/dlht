package adapters

import "github.com/alphadose/haxmap"

// haxmapHashable mirrors the unexported hashable constraint from alphadose/haxmap.
type haxmapHashable interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 | ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~uintptr | ~float32 | ~float64 | ~complex64 | ~complex128 | ~string
}

type haxMapAdapter[K haxmapHashable, V any] struct {
	m *haxmap.Map[K, V]
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

func (a *haxMapAdapter[K, V]) Size() int { return int(a.m.Len()) }
func (a *haxMapAdapter[K, V]) Close()    {}
