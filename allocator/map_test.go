package allocator

import (
	"sync/atomic"
	"testing"
)

func TestStats_EmptyMap(t *testing.T) {
	m := New[uint64, int](Options{InitialSize: 64})
	s := m.Stats()

	if s.Bins != 64 {
		t.Errorf("Bins: got %d, want 64", s.Bins)
	}
	if s.Size != 0 {
		t.Errorf("Size: got %d, want 0", s.Size)
	}
	if s.Capacity != 64*MAX_SLOTS_PER_BIN {
		t.Errorf("Capacity: got %d, want %d", s.Capacity, 64*MAX_SLOTS_PER_BIN)
	}
	if s.LoadFactor != 0 {
		t.Errorf("LoadFactor: got %g, want 0", s.LoadFactor)
	}
	if s.Resizing {
		t.Errorf("Resizing: got true, want false")
	}
	if s.Links != 0 {
		t.Errorf("Links: got %d, want 0 (no allocations yet)", s.Links)
	}
	if s.LinkCapacity == 0 {
		t.Errorf("LinkCapacity: got 0, want > 0")
	}
}

func TestStats_AfterInserts(t *testing.T) {
	m := New[uint64, int](Options{InitialSize: 64})
	const N uint64 = 50
	for i := range N {
		m.Insert(i, int(i))
	}
	s := m.Stats()

	if s.Size != N {
		t.Errorf("Size: got %d, want %d", s.Size, N)
	}
	wantLF := float64(N) / float64(s.Capacity)
	if s.LoadFactor < wantLF*0.999 || s.LoadFactor > wantLF*1.001 {
		t.Errorf("LoadFactor: got %g, want ~%g", s.LoadFactor, wantLF)
	}
}

func TestStats_AfterDeletes(t *testing.T) {
	m := New[uint64, int](Options{InitialSize: 64})
	const N uint64 = 30
	for i := range N {
		m.Insert(i, int(i))
	}
	const D uint64 = 10
	for i := range D {
		m.Delete(i)
	}
	s := m.Stats()

	if s.Size != N-D {
		t.Errorf("Size: got %d, want %d", s.Size, N-D)
	}
}

func TestStats_LinksAllocated(t *testing.T) {
	// InitialSize 64 -> linkCapacity 8, enough headroom for 8 colliding keys
	// (1 single link + 1 pair link = 3 link buckets) without triggering resize.
	m := newWithSeed[uint64, int](Options{InitialSize: 64}, testSeed)
	idx := m.getActiveIndex()
	keys := keysForBin(testSeed, idx.mask, 0, 8, 1<<20)
	if keys == nil {
		t.Skip("could not synthesize 8 colliding keys")
	}
	for i, k := range keys {
		m.Insert(k, i)
	}
	s := m.Stats()

	if s.Resizing {
		t.Fatalf("test setup error: resize triggered, link arena was too small")
	}
	if s.Links == 0 {
		t.Errorf("Links: got 0 with 8 colliding inserts; want > 0")
	}
	if s.LinkCapacity < s.Links {
		t.Errorf("LinkCapacity (%d) < Links (%d)", s.LinkCapacity, s.Links)
	}
	if s.Size != 8 {
		t.Errorf("Size: got %d, want 8", s.Size)
	}
}

func TestStats_ResizingFlag(t *testing.T) {
	m := New[uint64, int](Options{InitialSize: 16})

	ctx := &resizeContext[uint64, int]{active: ResizeActive}
	m.resizeCtx.Store(ctx)
	defer m.resizeCtx.Store(nil)

	if !m.Stats().Resizing {
		t.Errorf("Resizing: got false with non-nil resizeCtx, want true")
	}

	m.resizeCtx.Store(nil)
	if m.Stats().Resizing {
		t.Errorf("Resizing: got true with nil resizeCtx, want false")
	}
}

func TestStats_PostResizeAccuracy(t *testing.T) {
	// Single-threaded scenario that exercises both top-down (resize) and
	// bottom-up (post-resize normal) link allocations on the same index, so
	// Stats() must combine linkNext with the recovered transfer count.
	m := newWithSeed[uint64, int](Options{InitialSize: 16}, testSeed)

	// Keys colliding in bin 0 under both the old (mask=15) and the
	// post-resize (mask=127) indices. mask 127 implies mask 15.
	keys := keysForBin(testSeed, 127, 0, 8, 1<<22)
	if keys == nil {
		t.Skip("could not synthesize 8 colliding keys")
	}

	// 7 inserts: 3 primary slots + 4 single-link slots fill old bin 0.
	// allocateLink runs once, taking linkNext from 0 to 1.
	for i := range 7 {
		m.Insert(keys[i], i)
	}

	s := m.Stats()
	t.Logf("pre-resize: Bins=%d Links=%d LinkCapacity=%d Size=%d Resizing=%v",
		s.Bins, s.Links, s.LinkCapacity, s.Size, s.Resizing)
	if s.Resizing {
		t.Fatalf("resize triggered too early")
	}
	if s.Bins != 16 {
		t.Errorf("Bins pre-resize: got %d, want 16", s.Bins)
	}
	if s.Size != 7 {
		t.Errorf("Size pre-resize: got %d, want 7", s.Size)
	}
	if s.Links != 1 {
		t.Errorf("Links pre-resize: got %d, want 1", s.Links)
	}
	if s.LinkCapacity != 2 {
		t.Errorf("LinkCapacity pre-resize: got %d, want 2", s.LinkCapacity)
	}

	// 8th insert: primary and single both full, pair alloc fails (arena=2),
	// resize fires. Insert helps the resize through and retries on the new
	// index. There the 7 transferred entries already occupy primary+single
	// (the transfer phase top-down-allocated a single link), so the 8th
	// needs a pair via the normal bottom-up allocator.
	m.Insert(keys[7], 7)

	s = m.Stats()
	t.Logf("post-resize: Bins=%d Links=%d LinkCapacity=%d Size=%d Resizing=%v",
		s.Bins, s.Links, s.LinkCapacity, s.Size, s.Resizing)
	if s.Resizing {
		t.Fatalf("resize still in progress")
	}
	if s.Bins != 128 {
		t.Errorf("Bins post-resize: got %d, want 128", s.Bins)
	}
	if s.Size != 8 {
		t.Errorf("Size post-resize: got %d, want 8", s.Size)
	}
	if s.LinkCapacity != 16 {
		t.Errorf("LinkCapacity post-resize: got %d, want 16", s.LinkCapacity)
	}
	// 1 transfer-allocated single + 2 normal-allocated pair = 3 link buckets.
	if s.Links != 3 {
		t.Errorf("Links post-resize: got %d, want 3 (1 transfer single + 2 normal pair)", s.Links)
	}
}

func TestSize_EmptyMap(t *testing.T) {
	m := New[uint64, int](Options{InitialSize: 64})
	if got := m.Size(); got != 0 {
		t.Errorf("Size: got %d, want 0", got)
	}
}

func TestSize_AfterInserts(t *testing.T) {
	m := New[uint64, int](Options{InitialSize: 64})
	const N uint64 = 50
	for i := range N {
		m.Insert(i, int(i))
	}
	if got := m.Size(); got != N {
		t.Errorf("Size: got %d, want %d", got, N)
	}
}

func TestSize_AfterDeletes(t *testing.T) {
	m := New[uint64, int](Options{InitialSize: 64})
	const N uint64 = 30
	for i := range N {
		m.Insert(i, int(i))
	}
	const D uint64 = 10
	for i := range D {
		m.Delete(i)
	}
	if got := m.Size(); got != N-D {
		t.Errorf("Size: got %d, want %d", got, N-D)
	}
}

func TestSize_TryingSlotsNotCounted(t *testing.T) {
	m := newWithSeed[uint64, int](Options{InitialSize: 16}, testSeed)
	idx := m.getActiveIndex()
	pb := idx.getBinByIndex(0)

	h := atomicLoadHeader(&pb.Header)
	atomic.StoreUint64((*uint64)(&pb.Header), uint64(h.setSlotStateAndVersion(0, SlotTrying)))

	if got := m.Size(); got != 0 {
		t.Errorf("Size: got %d, want 0 (Trying slots must not count)", got)
	}
}
