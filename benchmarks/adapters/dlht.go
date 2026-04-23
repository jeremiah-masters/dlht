package adapters

import (
	"github.com/jeremiah-masters/dlht/allocator"
	"github.com/jeremiah-masters/dlht/inline"
)

type dlhtAllocatorBenchmarkAdapter[K allocator.Key, V any] struct {
	m *allocator.Map[K, V]
}

type dlhtInlineBenchmarkAdapter struct {
	m *inline.Map[uint64]
}

func NewDLHTAllocatorBenchmarkAdapter[K allocator.Key, V any](capacity int) MapAdapter[K, V] {
	return &dlhtAllocatorBenchmarkAdapter[K, V]{
		m: allocator.New[K, V](allocator.Options{InitialSize: uint64(capacity)}),
	}
}

func NewDLHTInlineBenchmarkAdapter(capacity int) MapAdapter[uint64, uint64] {
	return &dlhtInlineBenchmarkAdapter{
		m: inline.New[uint64](inline.Options{InitialSize: uint64(capacity)}),
	}
}

func (a *dlhtAllocatorBenchmarkAdapter[K, V]) Name() string        { return "dlht-allocator" }
func (a *dlhtAllocatorBenchmarkAdapter[K, V]) Get(key K) (V, bool) { return a.m.Get(key) }
func (a *dlhtAllocatorBenchmarkAdapter[K, V]) Insert(key K, val V) bool {
	_, ok := a.m.Insert(key, val)
	return ok
}
func (a *dlhtAllocatorBenchmarkAdapter[K, V]) Delete(key K) bool {
	_, existed := a.m.Delete(key)
	return existed
}
func (a *dlhtAllocatorBenchmarkAdapter[K, V]) Put(key K, val V) bool {
	_, existed := a.m.Put(key, val)
	return existed
}
func (a *dlhtAllocatorBenchmarkAdapter[K, V]) Size() int { return -1 }
func (a *dlhtAllocatorBenchmarkAdapter[K, V]) Close()    {}

func (a *dlhtInlineBenchmarkAdapter) Name() string { return "dlht-inline" }
func (a *dlhtInlineBenchmarkAdapter) Get(key uint64) (uint64, bool) {
	return a.m.Get(key)
}
func (a *dlhtInlineBenchmarkAdapter) Insert(key, val uint64) bool {
	_, ok := a.m.Insert(key, val)
	return ok
}
func (a *dlhtInlineBenchmarkAdapter) Delete(key uint64) bool {
	return a.m.Delete(key)
}
func (a *dlhtInlineBenchmarkAdapter) Put(key, val uint64) bool {
	_, existed := a.m.Put(key, val)
	return existed
}
func (a *dlhtInlineBenchmarkAdapter) Size() int { return -1 }
func (a *dlhtInlineBenchmarkAdapter) Close()    {}
