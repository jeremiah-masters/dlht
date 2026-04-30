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

	// Print statistics
	stats := m.Stats()
	fmt.Printf("Map has %d bins with load factor %.3f\n", stats.NumBins, stats.LoadFactor)

	// Output:
	// Inserted cherry
	// Found: apple = 5
	// Updated apple: 5 -> 10
	// Current value: apple = 10
	// Deleted banana (old value: 3)
	// Map has 64 bins with load factor 0.010
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
