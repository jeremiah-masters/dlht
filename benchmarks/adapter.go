package benchmarks

import "dlht-benchmarks/adapters"

// MapAdapter is a thin generic wrapper around concurrent map implementations.
// All methods must be safe for concurrent use.
type MapAdapter[K comparable, V any] interface {
	Name() string
	Get(key K) (V, bool)
	Insert(key K, value V) bool // insert-if-absent; true if inserted (key was new)
	Delete(key K) bool          // true if deleted (key existed)
	Put(key K, value V) bool    // update-if-exists; true if updated (key existed)
	Size() int
	Close()
}

// NamedFactory creates MapAdapter instances for benchmarking.
type NamedFactory[K comparable, V any] struct {
	Name string
	New  func(capacity int) MapAdapter[K, V]
}

// Uint64MapFactories returns benchmark factories for allocator-backed and inline DLHT variants,
// plus the external uint64 map baselines.
func Uint64MapFactories() []NamedFactory[uint64, uint64] {
	return []NamedFactory[uint64, uint64]{
		{Name: "dlht-allocator", New: func(cap int) MapAdapter[uint64, uint64] {
			return adapters.NewDLHTAllocatorBenchmarkAdapter[uint64, uint64](cap)
		}},
		{Name: "dlht-inline", New: func(cap int) MapAdapter[uint64, uint64] { return adapters.NewDLHTInlineBenchmarkAdapter(cap) }},
		{Name: "sync.Map", New: func(cap int) MapAdapter[uint64, uint64] { return adapters.NewSyncMapAdapter[uint64, uint64]() }},
		{Name: "xsync", New: func(cap int) MapAdapter[uint64, uint64] { return adapters.NewXSyncAdapter[uint64, uint64]() }},
		{Name: "cornelk", New: func(cap int) MapAdapter[uint64, uint64] { return adapters.NewCornelkAdapter[uint64, uint64](cap) }},
		{Name: "haxmap", New: func(cap int) MapAdapter[uint64, uint64] { return adapters.NewHaxMapAdapter[uint64, uint64](cap) }},
	}
}

// StringMapFactories returns benchmark factories for the allocator-backed DLHT
// plus the external string-key baselines.
func StringMapFactories() []NamedFactory[string, string] {
	return []NamedFactory[string, string]{
		{Name: "dlht-allocator", New: func(cap int) MapAdapter[string, string] {
			return adapters.NewDLHTAllocatorBenchmarkAdapter[string, string](cap)
		}},
		{Name: "sync.Map", New: func(cap int) MapAdapter[string, string] { return adapters.NewSyncMapAdapter[string, string]() }},
		{Name: "xsync", New: func(cap int) MapAdapter[string, string] { return adapters.NewXSyncAdapter[string, string]() }},
		{Name: "cornelk", New: func(cap int) MapAdapter[string, string] { return adapters.NewCornelkAdapter[string, string](cap) }},
		{Name: "haxmap", New: func(cap int) MapAdapter[string, string] { return adapters.NewHaxMapAdapter[string, string](cap) }},
		{Name: "orcaman", New: func(cap int) MapAdapter[string, string] { return adapters.NewOrcamanAdapter[string, string](cap) }},
	}
}
