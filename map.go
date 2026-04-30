// Package dlht provides DLHT, a high-performance lock-free hash table.
//
// DLHT is designed for high-concurrency scenarios and provides lock-free
// operations for Get, Insert, Delete, and Put. It uses a sophisticated
// memory ordering strategy and cooperative resizing to achieve excellent
// performance under concurrent workloads.
//
// Example usage:
//
//	// Create a new DLHT
//	m := dlht.New[string, int](dlht.Options{InitialSize: 64})
//
//	// Insert key-value pairs
//	m.Insert("key1", 42)
//	m.Insert("key2", 100)
//
//	// Get values
//	if value, found := m.Get("key1"); found {
//		fmt.Printf("Found: key1 = %d\n", value)
//	}
//
//	// Update values atomically
//	if oldValue, updated := m.Put("key1", 84); updated {
//		fmt.Printf("Updated key1: %d -> %d\n", oldValue, 84)
//	}
//
//	// Delete keys
//	if oldValue, deleted := m.Delete("key2"); deleted {
//		fmt.Printf("Deleted key2 (old value: %d)\n", oldValue)
//	}
package dlht

import (
	"github.com/jeremiah-masters/dlht/allocator"
	"github.com/jeremiah-masters/dlht/inline"
)

type Key = allocator.Key

type Map[K Key, V any] = allocator.Map[K, V]

type Options = allocator.Options

type Stats = allocator.Stats

// New creates a new Map[K, V] with the specified options.
//
// If opts.InitialSize is 0, it defaults to 16.
// The actual initial size will be rounded up to the next power of 2.
//
// Example:
//
//	// Create with default size (16)
//	m1 := dlht.New[string, int](dlht.Options{})
//
//	// Create with specific initial size
//	m2 := dlht.New[uint64, string](dlht.Options{InitialSize: 1024})
func New[K Key, V any](opts Options) *Map[K, V] {
	return allocator.New[K, V](opts)
}

func NewInline[V inline.Integer](opts inline.Options) *inline.Map[V] {
	return inline.New[V](opts)
}
