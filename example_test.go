package dlht_test

import (
	"fmt"
	"sync"

	"github.com/jeremiah-masters/dlht"
)

// Example demonstrates basic usage of DLHT
func Example() {
	// Create a new DLHT
	m := dlht.New[string, int](dlht.Options{InitialSize: 64})

	// Insert key-value pairs
	m.Insert("apple", 5)
	m.Insert("banana", 3)

	if oldValue, inserted := m.Insert("cherry", 8); inserted {
		fmt.Println("Inserted cherry")
	} else {
		fmt.Printf("Cherry already existed with value: %d\n", oldValue)
	}

	// Get values
	if value, found := m.Get("apple"); found {
		fmt.Printf("Found: apple = %d\n", value)
	}

	// Update values atomically
	if oldValue, updated := m.Put("apple", 10); updated {
		fmt.Printf("Updated apple: %d -> %d\n", oldValue, 10)
	}

	// Check if key exists
	if value, found := m.Get("apple"); found {
		fmt.Printf("Current value: apple = %d\n", value)
	}

	// Delete a key
	if oldValue, deleted := m.Delete("banana"); deleted {
		fmt.Printf("Deleted banana (old value: %d)\n", oldValue)
	}

	// Approximate entry count
	size := m.Size()
	fmt.Printf("Size: %d\n", size)

	// Print statistics
	stats := m.Stats()
	fmt.Printf("Map has %d bins with load factor %.3f\n", stats.Bins, stats.LoadFactor)

	// Output:
	// Inserted cherry
	// Found: apple = 5
	// Updated apple: 5 -> 10
	// Current value: apple = 10
	// Deleted banana (old value: 3)
	// Size: 2
	// Map has 64 bins with load factor 0.002
}

// ExampleMap_LoadOrCompute demonstrates computing a value only when the key
// is absent. fn runs at most once per key at a time, and a computed
// (save=true) value is always inserted: racing callers wait and load it.
func ExampleMap_LoadOrCompute() {
	m := dlht.New[string, int](dlht.Options{InitialSize: 64})

	calls := 0
	expensive := func() (int, bool) {
		calls++
		return 42, true // save the computed value
	}

	// Absent: fn runs, the value is inserted.
	if v, loaded := m.LoadOrCompute("answer", expensive); !loaded {
		fmt.Printf("computed: %d\n", v)
	}

	// Present: fn does not run, the existing value is returned.
	if v, loaded := m.LoadOrCompute("answer", expensive); loaded {
		fmt.Printf("loaded: %d\n", v)
	}

	fmt.Printf("fn calls: %d\n", calls)

	// A save=false return computes without inserting.
	v, _ := m.LoadOrCompute("probe", func() (int, bool) { return 7, false })
	_, found := m.Get("probe")
	fmt.Printf("probe computed %d, stored: %v\n", v, found)

	// Output:
	// computed: 42
	// loaded: 42
	// fn calls: 1
	// probe computed 7, stored: false
}

// Example_concurrent demonstrates concurrent usage of DLHT
func Example_concurrent() {
	m := dlht.New[uint64, string](dlht.Options{InitialSize: 32})

	var wg sync.WaitGroup
	numWorkers := 4
	itemsPerWorker := 10

	// Concurrent inserts
	for workerID := range numWorkers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range itemsPerWorker {
				key := uint64(workerID*itemsPerWorker + j)
				value := fmt.Sprintf("worker-%d-item-%d", workerID, j)
				_, _ = m.Insert(key, value)
			}
		}()
	}

	wg.Wait()

	// Verify all items were inserted
	totalItems := numWorkers * itemsPerWorker
	foundItems := 0
	for i := range totalItems {
		if _, found := m.Get(uint64(i)); found {
			foundItems++
		}
	}

	fmt.Printf("Concurrent operations completed successfully\n")
	fmt.Printf("Inserted and found %d items\n", foundItems)
}

// ExampleMap_Range walks every entry; yield can return false to stop early.
func ExampleMap_Range() {
	m := dlht.New[string, int](dlht.Options{InitialSize: 16})
	m.Insert("apple", 1)
	m.Insert("banana", 2)
	m.Insert("cherry", 3)

	total := 0
	m.Range(func(_ string, v int) bool {
		total += v
		return true
	})
	fmt.Printf("total: %d\n", total)

	// Output:
	// total: 6
}
