package allocator

import (
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/jeremiah-masters/dlht/internal/cpu"
)

// Sweeps n across the small/large size-class boundaries mallocgc treats
// specially: (n+2)*64 of 512 (no header), 576 (headered, class is 64-divisible
// but not 128 on arm64), 32768 (last small), and above that (large-object
// path). For each n we check that bucket sizeof is 64 (the skip math relies
// on it), len and cap equal n, the first bucket lands on a cache line, and
// every slot is 16-byte aligned for DWCAS. Subsequent buckets are 64-aligned
// by construction; on arm64 with 128-byte lines, odd-indexed buckets share
// a line with their neighbor by design.
func TestAllocationCorrectness(t *testing.T) {
	sizes := []uint64{0, 1, 6, 7, 8, 14, 30, 62, 126, 254, 510, 511, 1022}

	for _, n := range sizes {
		t.Run(fmt.Sprintf("primary_n=%d", n), func(t *testing.T) {
			buckets := makePrimaryAlignedSlice[uint64, uint64](n)
			checkAlignment(t, "primary", n, len(buckets), cap(buckets), unsafe.SliceData(buckets), func(i uint64) []uintptr {
				addrs := make([]uintptr, PRIMARY_SLOTS)
				for j := range PRIMARY_SLOTS {
					addrs[j] = uintptr(unsafe.Pointer(&buckets[i].Slots[j]))
				}
				return addrs
			})
		})

		t.Run(fmt.Sprintf("link_n=%d", n), func(t *testing.T) {
			buckets := makeLinkAlignedSlice[uint64, uint64](n)
			checkAlignment(t, "link", n, len(buckets), cap(buckets), unsafe.SliceData(buckets), func(i uint64) []uintptr {
				addrs := make([]uintptr, LINK_SLOTS)
				for j := range LINK_SLOTS {
					addrs[j] = uintptr(unsafe.Pointer(&buckets[i].Slots[j]))
				}
				return addrs
			})
		})
	}
}

func checkAlignment[T any](t *testing.T, kind string, n uint64, gotLen, gotCap int, data *T, slotAddrs func(i uint64) []uintptr) {
	t.Helper()
	if sz := unsafe.Sizeof(*data); sz != bucketByteSize {
		t.Errorf("%s bucket sizeof=%d want=%d (the rotation/skip math assumes 64-byte buckets)", kind, sz, bucketByteSize)
	}
	if uint64(gotLen) != n {
		t.Errorf("%s len=%d want=%d", kind, gotLen, n)
	}
	if uint64(gotCap) != n {
		t.Errorf("%s cap=%d want=%d (slack region must not be reachable via append)", kind, gotCap, n)
	}
	if n == 0 {
		return
	}
	firstAddr := uintptr(unsafe.Pointer(data))
	if firstAddr%uintptr(cpu.CacheLineSize) != 0 {
		t.Errorf("%s first bucket addr=0x%x not %d-byte aligned (off by %d)",
			kind, firstAddr, cpu.CacheLineSize, firstAddr%uintptr(cpu.CacheLineSize))
	}
	for i := range n {
		for j, addr := range slotAddrs(i) {
			if addr%16 != 0 {
				t.Errorf("%s bucket[%d].slot[%d] addr=0x%x not 16-byte aligned for DWCAS", kind, i, j, addr)
			}
		}
	}
}

// Cross-checks mallocgcAddsHeader against what mallocgc actually does. A
// scan allocation in the headered range gets an 8-byte type pointer
// prepended, so user data lands at addr%64 == 8; outside that range it
// lands at 0. If a future Go release moves the header range or breaks the
// "every header-range size class is 64-divisible" property, our predictor
// silently mispredicts and we start hitting the slow fallback. This test
// notices first.
func TestMallocgcAddsHeaderMatchesReality(t *testing.T) {
	// Boundaries: 509 is the last n with predictor=true (total=32704, +8=32712).
	// 510 flips it false (total=32768, +8=32776).
	sizes := []uint64{1, 6, 7, 8, 14, 30, 62, 126, 254, 509, 510, 511, 1022}

	const trials = 5
	for _, n := range sizes {
		predicted := mallocgcAddsHeader(n)

		var observed [trials]bool
		var live [trials]any
		for i := range trials {
			buf := make([]PrimaryBucket[uint64, uint64], n+2)
			addr := uintptr(unsafe.Pointer(unsafe.SliceData(buf)))
			observed[i] = addr&63 == 8
			live[i] = buf
		}
		runtime.KeepAlive(live)

		for i, got := range observed {
			if got != predicted {
				t.Errorf("n=%d trial=%d: predictor=%v, observed addr&63==8 -> %v (size=%d bytes)",
					n, i, predicted, got, (n+2)*64)
			}
		}
	}
}

// The rotation trick allocates PrimaryBucketRot8 / LinkBucketRot8 so that
// after the cache-line skip, *Entry pointers in slots end up at the offsets
// the rotated type's GC bitmap marks as pointers. Get the layouts wrong and
// GC won't trace through slot.Val; the entry gets reclaimed under the
// writer.
//
// We plant entries whose only live reference is the slot itself, attach
// finalizers, force GC. A finalizer firing means GC missed the slot
// pointer.
func TestGCSurvivesRotatedLayout(t *testing.T) {
	sizes := []uint64{7, 14, 30, 62, 126, 254}

	for _, n := range sizes {
		if !mallocgcAddsHeader(n) {
			t.Fatalf("test setup error: n=%d should exercise the rotated path", n)
		}

		t.Run(fmt.Sprintf("primary_n=%d", n), func(t *testing.T) {
			var collected atomic.Int32
			buckets := makePrimaryAlignedSlice[uint64, uint64](n)
			for i := range buckets {
				for j := range PRIMARY_SLOTS {
					populateSlot(&buckets[i].Slots[j], uint64(i)*1000+uint64(j), &collected)
				}
			}
			forceGCAndFinalize()
			if c := collected.Load(); c > 0 {
				t.Errorf("n=%d: %d entries collected, PrimaryBucketRot8 bitmap likely wrong", n, c)
			}
			for i := range buckets {
				for j := range PRIMARY_SLOTS {
					verifySlotEntry(t, &buckets[i].Slots[j], uint64(i)*1000+uint64(j), fmt.Sprintf("primary[%d].slot[%d]", i, j))
				}
			}
			runtime.KeepAlive(buckets)
		})

		t.Run(fmt.Sprintf("link_n=%d", n), func(t *testing.T) {
			var collected atomic.Int32
			buckets := makeLinkAlignedSlice[uint64, uint64](n)
			for i := range buckets {
				for j := range LINK_SLOTS {
					populateSlot(&buckets[i].Slots[j], uint64(i)*1000+uint64(j), &collected)
				}
			}
			forceGCAndFinalize()
			if c := collected.Load(); c > 0 {
				t.Errorf("n=%d: %d entries collected, LinkBucketRot8 bitmap likely wrong", n, c)
			}
			for i := range buckets {
				for j := range LINK_SLOTS {
					verifySlotEntry(t, &buckets[i].Slots[j], uint64(i)*1000+uint64(j), fmt.Sprintf("link[%d].slot[%d]", i, j))
				}
			}
			runtime.KeepAlive(buckets)
		})
	}
}

func populateSlot(s *Slot[uint64, uint64], key uint64, collected *atomic.Int32) {
	e := &Entry[uint64, uint64]{Key: key, Value: key ^ 0xCAFEBABE}
	runtime.SetFinalizer(e, func(*Entry[uint64, uint64]) { collected.Add(1) })
	s.Key = key
	s.Val = e
}

func verifySlotEntry(t *testing.T, s *Slot[uint64, uint64], expectedKey uint64, label string) {
	t.Helper()
	e := s.Val
	if e == nil {
		t.Errorf("%s Val is nil; entry was reaped or pointer overwritten", label)
		return
	}
	if e.Key != expectedKey {
		t.Errorf("%s Key=%d want=%d (memory corruption)", label, e.Key, expectedKey)
	}
	if want := expectedKey ^ 0xCAFEBABE; e.Value != want {
		t.Errorf("%s Value=%d want=%d (memory corruption)", label, e.Value, want)
	}
}

// Drains the finalizer queue. Finalizers run on a separate goroutine and
// can lag a GC cycle by tens of milliseconds, so we cycle GC with pauses
// rather than calling it back-to-back. Between cycles we allocate sprays
// of the same Entry type: if the bitmap was wrong and entries got
// reclaimed, the spray is likely to land in the same size-class slots, so
// reading slot.Val.Key in verifySlotEntry returns a 0xDEADBEEF sentinel
// instead of the planted value. Reuse isn't guaranteed but it gives a
// second signal alongside the finalizer count.
func forceGCAndFinalize() {
	for range 20 {
		runtime.GC()
		spray := make([]*Entry[uint64, uint64], 1024)
		for i := range spray {
			spray[i] = &Entry[uint64, uint64]{Key: 0xDEADBEEFDEADBEEF, Value: 0xDEADBEEFDEADBEEF}
		}
		runtime.KeepAlive(spray)
		time.Sleep(10 * time.Millisecond)
	}
}

// The fallback in makePrimary/LinkAlignedSlice fires only when the
// predictor disagrees with the runtime, which doesn't happen on current Go
// (see TestMallocgcAddsHeaderMatchesReality). The path is therefore dead in
// the live helper, so we exercise the same expression directly. If a
// future Go release flips a size-class boundary and the fallback becomes
// reachable, this test will already have covered it.
//
// The fallback inflates the request to at least LargeBufferSize, which
// pushes it past maxSmallSize onto the page-aligned large-object path.
func TestFallbackAllocation(t *testing.T) {
	sizes := []uint64{0, 1, 100, 510, 511, 1022}

	for _, n := range sizes {
		t.Run(fmt.Sprintf("primary_n=%d", n), func(t *testing.T) {
			buf := make([]PrimaryBucket[uint64, uint64], max(n, largeBufferSize))[:n:n]
			checkAlignment(t, "primary-fallback", n, len(buf), cap(buf), unsafe.SliceData(buf), func(i uint64) []uintptr {
				addrs := make([]uintptr, PRIMARY_SLOTS)
				for j := range PRIMARY_SLOTS {
					addrs[j] = uintptr(unsafe.Pointer(&buf[i].Slots[j]))
				}
				return addrs
			})
		})

		t.Run(fmt.Sprintf("link_n=%d", n), func(t *testing.T) {
			buf := make([]LinkBucket[uint64, uint64], max(n, largeBufferSize))[:n:n]
			checkAlignment(t, "link-fallback", n, len(buf), cap(buf), unsafe.SliceData(buf), func(i uint64) []uintptr {
				addrs := make([]uintptr, LINK_SLOTS)
				for j := range LINK_SLOTS {
					addrs[j] = uintptr(unsafe.Pointer(&buf[i].Slots[j]))
				}
				return addrs
			})
		})
	}
}
