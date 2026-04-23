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
	_, _ = m.Insert("apple", 5)
	_, _ = m.Insert("banana", 3)
	_, _ = m.Insert("cherry", 8)

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
	if _, deleted := m.Delete("banana"); deleted {
		fmt.Println("Deleted banana")
	}

	// Print statistics
	stats := m.Stats()
	fmt.Printf("Map has %d bins with load factor %.3f\n", stats.NumBins, stats.LoadFactor)

	// Output:
	// Found: apple = 5
	// Updated apple: 5 -> 10
	// Current value: apple = 10
	// Deleted banana
	// Map has 64 bins with load factor 0.010
}

// Example_concurrent demonstrates concurrent usage of DLHT
func Example_concurrent() {
	m := dlht.New[uint64, string](dlht.Options{InitialSize: 32})

	var wg sync.WaitGroup
	numWorkers := 4
	itemsPerWorker := 10

	// Concurrent inserts
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < itemsPerWorker; j++ {
				key := uint64(workerID*itemsPerWorker + j)
				value := fmt.Sprintf("worker-%d-item-%d", workerID, j)
				_, _ = m.Insert(key, value)
			}
		}(i)
	}

	wg.Wait()

	// Verify all items were inserted
	totalItems := numWorkers * itemsPerWorker
	foundItems := 0
	for i := 0; i < totalItems; i++ {
		if _, found := m.Get(uint64(i)); found {
			foundItems++
		}
	}

	fmt.Printf("Concurrent operations completed successfully\n")
	fmt.Printf("Inserted and found %d items\n", foundItems)
}
