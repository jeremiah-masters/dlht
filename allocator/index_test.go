package allocator

import (
	"fmt"
	"testing"
	"unsafe"

	"github.com/jeremiah-masters/dlht/internal/cpu"
)

// TestMakeAlignedPrimaryBuckets tests that makeAlignedPrimaryBuckets creates properly aligned slices
func TestMakeAlignedPrimaryBuckets(t *testing.T) {
	testCases := []struct {
		name    string
		count   uint64
		keyType string
		valType string
	}{
		{"single_bucket_string_int", 1, "string", "int"},
		{"multiple_buckets_string_int", 10, "string", "int"},
		{"large_count_string_int", 1000, "string", "int"},
		{"power_of_two_string_int", 64, "string", "int"},
		{"odd_count_string_int", 37, "string", "int"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Test with string keys and int values
			buckets := makePrimaryAlignedSlice[string, int](tc.count)

			// Verify slice length
			if uint64(len(buckets)) != tc.count {
				t.Errorf("Expected slice length %d, got %d", tc.count, len(buckets))
			}

			if tc.count == 0 {
				return // Nothing more to test for empty slice
			}

			// Check alignment of the first bucket
			firstBucketAddr := uintptr(unsafe.Pointer(&buckets[0]))
			expectedAlign := cpu.CacheLineSize / 8 // This should be 16 bytes on ARM64 (128/8)

			if firstBucketAddr%uintptr(expectedAlign) != 0 {
				t.Errorf("First bucket not properly aligned: addr=0x%x, expected alignment=%d, actual misalignment=%d",
					firstBucketAddr, expectedAlign, firstBucketAddr%uintptr(expectedAlign))
			}

			// Check that all buckets are properly aligned
			bucketSize := unsafe.Sizeof(PrimaryBucket[string, int]{})
			for i := uint64(0); i < tc.count; i++ {
				bucketAddr := uintptr(unsafe.Pointer(&buckets[i]))

				// Each bucket should be at the expected offset
				expectedAddr := firstBucketAddr + uintptr(i)*bucketSize
				if bucketAddr != expectedAddr {
					t.Errorf("Bucket %d at unexpected address: expected=0x%x, actual=0x%x",
						i, expectedAddr, bucketAddr)
				}

				// Check that slots within each bucket are 16-byte aligned for DWCAS
				slotsAddr := uintptr(unsafe.Pointer(&buckets[i].Slots[0]))
				if slotsAddr%16 != 0 {
					t.Errorf("Bucket %d slots not 16-byte aligned: addr=0x%x, misalignment=%d",
						i, slotsAddr, slotsAddr%16)
				}
			}

			// Verify the slice is backed by the expected memory size
			if tc.count > 1 {
				lastBucketAddr := uintptr(unsafe.Pointer(&buckets[tc.count-1]))
				totalSize := lastBucketAddr - firstBucketAddr + bucketSize
				expectedSize := uintptr(tc.count) * bucketSize
				if totalSize != expectedSize {
					t.Errorf("Unexpected total memory layout: expected size=%d, actual size=%d",
						expectedSize, totalSize)
				}
			}
		})
	}
}

// TestMakeAlignedLinkBuckets tests that makeAlignedLinkBuckets creates properly aligned slices
func TestMakeAlignedLinkBuckets(t *testing.T) {
	testCases := []struct {
		name  string
		count uint64
	}{
		{"single_bucket", 1},
		{"multiple_buckets", 10},
		{"large_count", 1000},
		{"power_of_two", 64},
		{"odd_count", 37},
		{"zero_count", 0},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Test with string keys and int values
			buckets := makeLinkAlignedSlice[string, int](tc.count)

			// Verify slice length
			if uint64(len(buckets)) != tc.count {
				t.Errorf("Expected slice length %d, got %d", tc.count, len(buckets))
			}

			if tc.count == 0 {
				return // Nothing more to test for empty slice
			}

			// Check 16-byte alignment of the first bucket (required for DWCAS)
			firstBucketAddr := uintptr(unsafe.Pointer(&buckets[0]))
			if firstBucketAddr%16 != 0 {
				t.Errorf("First bucket not 16-byte aligned: addr=0x%x, misalignment=%d",
					firstBucketAddr, firstBucketAddr%16)
			}

			// Check that all buckets are properly aligned
			bucketSize := unsafe.Sizeof(LinkBucket[string, int]{})
			for i := uint64(0); i < tc.count; i++ {
				bucketAddr := uintptr(unsafe.Pointer(&buckets[i]))

				// Each bucket should be at the expected offset
				expectedAddr := firstBucketAddr + uintptr(i)*bucketSize
				if bucketAddr != expectedAddr {
					t.Errorf("Bucket %d at unexpected address: expected=0x%x, actual=0x%x",
						i, expectedAddr, bucketAddr)
				}

				// Check that slots within each bucket are 16-byte aligned for DWCAS
				slotsAddr := uintptr(unsafe.Pointer(&buckets[i].Slots[0]))
				if slotsAddr%16 != 0 {
					t.Errorf("Bucket %d slots not 16-byte aligned: addr=0x%x, misalignment=%d",
						i, slotsAddr, slotsAddr%16)
				}
			}
		})
	}
}

// TestAlignmentConsistency tests that the alignment functions produce consistent results
func TestAlignmentConsistency(t *testing.T) {
	const iterations = 100
	const bucketCount = 10

	for i := 0; i < iterations; i++ {
		// Test PrimaryBuckets
		buckets1 := makePrimaryAlignedSlice[string, int](bucketCount)
		buckets2 := makePrimaryAlignedSlice[string, int](bucketCount)

		addr1 := uintptr(unsafe.Pointer(&buckets1[0]))
		addr2 := uintptr(unsafe.Pointer(&buckets2[0]))

		expectedAlign := cpu.CacheLineSize / 8
		if addr1%uintptr(expectedAlign) != 0 {
			t.Errorf("Iteration %d: PrimaryBuckets not aligned: addr=0x%x", i, addr1)
		}
		if addr2%uintptr(expectedAlign) != 0 {
			t.Errorf("Iteration %d: PrimaryBuckets not aligned: addr=0x%x", i, addr2)
		}

		// Test LinkBuckets
		linkBuckets1 := makeLinkAlignedSlice[string, int](bucketCount)
		linkBuckets2 := makeLinkAlignedSlice[string, int](bucketCount)

		linkAddr1 := uintptr(unsafe.Pointer(&linkBuckets1[0]))
		linkAddr2 := uintptr(unsafe.Pointer(&linkBuckets2[0]))

		if linkAddr1%16 != 0 {
			t.Errorf("Iteration %d: LinkBuckets not 16-byte aligned: addr=0x%x", i, linkAddr1)
		}
		if linkAddr2%16 != 0 {
			t.Errorf("Iteration %d: LinkBuckets not 16-byte aligned: addr=0x%x", i, linkAddr2)
		}
	}
}

// TestDifferentKeyValueTypes tests alignment with different key/value type combinations
func TestDifferentKeyValueTypes(t *testing.T) {
	testCases := []struct {
		name string
		test func(t *testing.T)
	}{
		{
			"uint64_string",
			func(t *testing.T) {
				buckets := makePrimaryAlignedSlice[uint64, string](5)
				addr := uintptr(unsafe.Pointer(&buckets[0]))
				expectedAlign := cpu.CacheLineSize / 8
				if addr%uintptr(expectedAlign) != 0 {
					t.Errorf("uint64/string buckets not aligned: addr=0x%x", addr)
				}
			},
		},
		{
			"string_complex_struct",
			func(t *testing.T) {
				type ComplexStruct struct {
					A int64
					B string
					C []byte
				}
				buckets := makePrimaryAlignedSlice[string, ComplexStruct](5)
				addr := uintptr(unsafe.Pointer(&buckets[0]))
				expectedAlign := cpu.CacheLineSize / 8
				if addr%uintptr(expectedAlign) != 0 {
					t.Errorf("string/ComplexStruct buckets not aligned: addr=0x%x", addr)
				}
			},
		},
		{
			"uint64_pointer",
			func(t *testing.T) {
				buckets := makeLinkAlignedSlice[uint64, *int](5)
				addr := uintptr(unsafe.Pointer(&buckets[0]))
				if addr%16 != 0 {
					t.Errorf("uint64/*int link buckets not 16-byte aligned: addr=0x%x", addr)
				}
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, tc.test)
	}
}

// TestSlotAlignmentForDWCAS specifically tests that individual slots are properly aligned for DWCAS operations
func TestSlotAlignmentForDWCAS(t *testing.T) {
	const bucketCount = 10

	// Test PrimaryBuckets
	primaryBuckets := makePrimaryAlignedSlice[string, int](bucketCount)
	for i := 0; i < int(bucketCount); i++ {
		for j := 0; j < PRIMARY_SLOTS; j++ {
			slotAddr := uintptr(unsafe.Pointer(&primaryBuckets[i].Slots[j]))
			if slotAddr%16 != 0 {
				t.Errorf("PrimaryBucket[%d].Slots[%d] not 16-byte aligned for DWCAS: addr=0x%x, misalignment=%d",
					i, j, slotAddr, slotAddr%16)
			}
		}
	}

	// Test LinkBuckets
	linkBuckets := makeLinkAlignedSlice[string, int](bucketCount)
	for i := 0; i < int(bucketCount); i++ {
		for j := 0; j < LINK_SLOTS; j++ {
			slotAddr := uintptr(unsafe.Pointer(&linkBuckets[i].Slots[j]))
			if slotAddr%16 != 0 {
				t.Errorf("LinkBucket[%d].Slots[%d] not 16-byte aligned for DWCAS: addr=0x%x, misalignment=%d",
					i, j, slotAddr, slotAddr%16)
			}
		}
	}
}

// BenchmarkMakeAlignedPrimaryBuckets benchmarks the performance of creating aligned primary buckets
func BenchmarkMakeAlignedPrimaryBuckets(b *testing.B) {
	sizes := []uint64{1, 10, 100, 1000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = makePrimaryAlignedSlice[string, int](size)
			}
		})
	}
}

// BenchmarkMakeAlignedLinkBuckets benchmarks the performance of creating aligned link buckets
func BenchmarkMakeAlignedLinkBuckets(b *testing.B) {
	sizes := []uint64{1, 10, 100, 1000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = makeLinkAlignedSlice[string, int](size)
			}
		})
	}
}

// TestDWCASAlignmentRequirement specifically tests that the alignment functions solve the DWCAS alignment issue
func TestDWCASAlignmentRequirement(t *testing.T) {
	// This test verifies that our alignment functions create slices where every slot
	// is 16-byte aligned, which is required for DWCAS operations on ARM64

	const testIterations = 50
	const bucketsPerTest = 20

	for iteration := 0; iteration < testIterations; iteration++ {
		t.Run(fmt.Sprintf("iteration_%d", iteration), func(t *testing.T) {
			// Test PrimaryBuckets
			primaryBuckets := makePrimaryAlignedSlice[string, int](bucketsPerTest)

			for bucketIdx := 0; bucketIdx < bucketsPerTest; bucketIdx++ {
				bucket := &primaryBuckets[bucketIdx]

				// Check bucket alignment
				bucketAddr := uintptr(unsafe.Pointer(bucket))
				expectedAlign := cpu.CacheLineSize / 8
				if bucketAddr%uintptr(expectedAlign) != 0 {
					t.Errorf("PrimaryBucket[%d] not aligned: addr=0x%x, expected alignment=%d, misalignment=%d",
						bucketIdx, bucketAddr, expectedAlign, bucketAddr%uintptr(expectedAlign))
				}

				// Check each slot for 16-byte alignment (DWCAS requirement)
				for slotIdx := 0; slotIdx < PRIMARY_SLOTS; slotIdx++ {
					slot := &bucket.Slots[slotIdx]
					slotAddr := uintptr(unsafe.Pointer(slot))

					if slotAddr%16 != 0 {
						t.Errorf("PrimaryBucket[%d].Slots[%d] not 16-byte aligned for DWCAS: addr=0x%x, misalignment=%d",
							bucketIdx, slotIdx, slotAddr, slotAddr%16)
					}
				}
			}

			// Test LinkBuckets
			linkBuckets := makeLinkAlignedSlice[string, int](bucketsPerTest)

			for bucketIdx := 0; bucketIdx < bucketsPerTest; bucketIdx++ {
				bucket := &linkBuckets[bucketIdx]

				// Check bucket alignment (should be 16-byte aligned)
				bucketAddr := uintptr(unsafe.Pointer(bucket))
				if bucketAddr%16 != 0 {
					t.Errorf("LinkBucket[%d] not 16-byte aligned: addr=0x%x, misalignment=%d",
						bucketIdx, bucketAddr, bucketAddr%16)
				}

				// Check each slot for 16-byte alignment (DWCAS requirement)
				for slotIdx := 0; slotIdx < LINK_SLOTS; slotIdx++ {
					slot := &bucket.Slots[slotIdx]
					slotAddr := uintptr(unsafe.Pointer(slot))

					if slotAddr%16 != 0 {
						t.Errorf("LinkBucket[%d].Slots[%d] not 16-byte aligned for DWCAS: addr=0x%x, misalignment=%d",
							bucketIdx, slotIdx, slotAddr, slotAddr%16)
					}
				}
			}
		})
	}
}

// TestCompareWithRegularSlices tests that regular Go slices would NOT be properly aligned
// This demonstrates why the alignment functions are necessary
func TestCompareWithRegularSlices(t *testing.T) {
	// Create regular slices (the way it would be done without alignment functions)
	regularPrimaryBuckets := make([]PrimaryBucket[string, int], 10)
	regularLinkBuckets := make([]LinkBucket[string, int], 10)

	// Check if regular slices happen to be aligned (they usually won't be)
	regularPrimaryAddr := uintptr(unsafe.Pointer(&regularPrimaryBuckets[0]))
	regularLinkAddr := uintptr(unsafe.Pointer(&regularLinkBuckets[0]))

	expectedAlign := cpu.CacheLineSize / 8
	primaryAligned := regularPrimaryAddr%uintptr(expectedAlign) == 0
	linkAligned := regularLinkAddr%uintptr(expectedAlign) == 0

	t.Logf("Regular PrimaryBuckets addr=0x%x, aligned=%v (expected alignment=%d)",
		regularPrimaryAddr, primaryAligned, expectedAlign)
	t.Logf("Regular LinkBuckets addr=0x%x, 16-byte aligned=%v",
		regularLinkAddr, linkAligned)

	// Now create aligned slices
	alignedPrimaryBuckets := makePrimaryAlignedSlice[string, int](10)
	alignedLinkBuckets := makeLinkAlignedSlice[string, int](10)

	alignedPrimaryAddr := uintptr(unsafe.Pointer(&alignedPrimaryBuckets[0]))
	alignedLinkAddr := uintptr(unsafe.Pointer(&alignedLinkBuckets[0]))

	// These MUST be aligned
	if alignedPrimaryAddr%uintptr(expectedAlign) != 0 {
		t.Errorf("Aligned PrimaryBuckets not properly aligned: addr=0x%x", alignedPrimaryAddr)
	}
	if alignedLinkAddr%16 != 0 {
		t.Errorf("Aligned LinkBuckets not 16-byte aligned: addr=0x%x", alignedLinkAddr)
	}

	t.Logf("Aligned PrimaryBuckets len=%d addr=0x%x, aligned=%t", len(alignedPrimaryBuckets), alignedPrimaryAddr, alignedPrimaryAddr%uintptr(expectedAlign) == 0)
	t.Logf("Regular PrimaryBuckets len=%d addr=0x%x, aligned=%t", len(regularPrimaryBuckets), regularPrimaryAddr, primaryAligned)
	t.Logf("Aligned LinkBuckets len=%d addr=0x%x, 16-byte aligned=%t", len(alignedLinkBuckets), alignedLinkAddr, alignedLinkAddr%uintptr(expectedAlign) == 0)
	t.Logf("Regular LinkBuckets len=%d addr=0x%x, 16-byte aligned=%t", len(regularLinkBuckets), regularLinkAddr, linkAligned)

	// The key insight: regular slices are often NOT aligned, but our functions guarantee alignment
}
