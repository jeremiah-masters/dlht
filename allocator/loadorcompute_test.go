package allocator

import (
	"fmt"
	"hash/maphash"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestLoadOrCompute_ComputesWhenAbsent(t *testing.T) {
	m := New[uint64, uint64](Options{InitialSize: 16})

	calls := 0
	v, loaded := m.LoadOrCompute(1, func() (uint64, bool) {
		calls++
		return 42, true
	})
	if loaded {
		t.Fatalf("loaded: got true for absent key")
	}
	if v != 42 {
		t.Fatalf("value: got %d, want 42", v)
	}
	if calls != 1 {
		t.Fatalf("fn calls: got %d, want 1", calls)
	}
	if got, ok := m.Get(1); !ok || got != 42 {
		t.Fatalf("Get after compute: got (%d, %v), want (42, true)", got, ok)
	}
	if s := m.Size(); s != 1 {
		t.Fatalf("Size: got %d, want 1", s)
	}
}

func TestLoadOrCompute_LoadsExisting(t *testing.T) {
	m := New[uint64, uint64](Options{InitialSize: 16})
	m.Insert(1, 7)

	v, loaded := m.LoadOrCompute(1, func() (uint64, bool) {
		t.Error("fn ran for a present key")
		return 0, true
	})
	if !loaded || v != 7 {
		t.Fatalf("got (%d, %v), want (7, true)", v, loaded)
	}
}

func TestLoadOrCompute_SecondCallLoadsCommitted(t *testing.T) {
	m := New[uint64, uint64](Options{InitialSize: 16})

	calls := 0
	fn := func() (uint64, bool) { calls++; return 11, true }
	if v, loaded := m.LoadOrCompute(9, fn); loaded || v != 11 {
		t.Fatalf("first call: got (%d, %v), want (11, false)", v, loaded)
	}
	if v, loaded := m.LoadOrCompute(9, fn); !loaded || v != 11 {
		t.Fatalf("second call: got (%d, %v), want (11, true)", v, loaded)
	}
	if calls != 1 {
		t.Fatalf("fn calls: got %d, want 1", calls)
	}
}

func TestLoadOrCompute_SaveFalse(t *testing.T) {
	m := New[uint64, uint64](Options{InitialSize: 16})

	v, loaded := m.LoadOrCompute(3, func() (uint64, bool) { return 55, false })
	if loaded || v != 55 {
		t.Fatalf("got (%d, %v), want (55, false)", v, loaded)
	}
	if _, ok := m.Get(3); ok {
		t.Fatal("key visible after save=false")
	}
	if s := m.Size(); s != 0 {
		t.Fatalf("Size after save=false: got %d, want 0", s)
	}

	// The aborted claim's slot is fully released and reusable.
	calls := 0
	if v, loaded := m.LoadOrCompute(3, func() (uint64, bool) { calls++; return 56, true }); loaded || v != 56 {
		t.Fatalf("recompute: got (%d, %v), want (56, false)", v, loaded)
	}
	if calls != 1 {
		t.Fatalf("fn calls after abort: got %d, want 1", calls)
	}
	if got, ok := m.Get(3); !ok || got != 56 {
		t.Fatalf("Get after recompute: got (%d, %v), want (56, true)", got, ok)
	}
}

func TestLoadOrCompute_ZeroValue(t *testing.T) {
	m := New[uint64, int](Options{InitialSize: 16})

	if v, loaded := m.LoadOrCompute(5, func() (int, bool) { return 0, true }); loaded || v != 0 {
		t.Fatalf("got (%d, %v), want (0, false)", v, loaded)
	}
	if v, loaded := m.LoadOrCompute(5, func() (int, bool) {
		t.Error("fn ran for a present key")
		return 1, true
	}); !loaded || v != 0 {
		t.Fatalf("got (%d, %v), want (0, true)", v, loaded)
	}
}

func TestLoadOrCompute_AfterDelete(t *testing.T) {
	m := New[uint64, uint64](Options{InitialSize: 16})

	calls := 0
	fn := func() (uint64, bool) { calls++; return uint64(calls * 100), true }
	m.LoadOrCompute(4, fn)
	if _, ok := m.Delete(4); !ok {
		t.Fatal("Delete failed")
	}
	if v, loaded := m.LoadOrCompute(4, fn); loaded || v != 200 {
		t.Fatalf("recompute after delete: got (%d, %v), want (200, false)", v, loaded)
	}
	if calls != 2 {
		t.Fatalf("fn calls: got %d, want 2", calls)
	}
}

// Fill the primary slots so the claim uses a link slot.
func TestLoadOrCompute_LinkSlotClaim(t *testing.T) {
	m := newWithSeed[uint64, uint64](Options{InitialSize: 16}, testSeed)
	keys := keysForBin(testSeed, 15, 0, 4, 1<<22)
	if keys == nil {
		t.Skip("could not synthesize colliding keys")
	}
	for i := range 3 {
		m.Insert(keys[i], uint64(i))
	}

	hash := maphash.Comparable(testSeed, keys[3])
	started := make(chan struct{})
	release := make(chan struct{})
	var v uint64
	var loaded bool
	done := make(chan struct{})
	go func() {
		defer close(done)
		v, loaded = m.LoadOrCompute(keys[3], func() (uint64, bool) {
			close(started)
			<-release
			return 99, true
		})
	}()
	<-started

	idx := m.getActiveIndex()
	pb := idx.getBin(hash)
	h := atomicLoadHeader(&pb.Header)
	si, _ := m.findShadowSlot(idx, pb, h, hash)
	if si < PRIMARY_SLOTS {
		t.Errorf("claim slot: got %d, want a link slot (>= %d)", si, PRIMARY_SLOTS)
	}

	close(release)
	<-done
	if loaded || v != 99 {
		t.Fatalf("got (%d, %v), want (99, false)", v, loaded)
	}
	if got, ok := m.Get(keys[3]); !ok || got != 99 {
		t.Fatalf("Get: got (%d, %v), want (99, true)", got, ok)
	}
}

// A 16-bin index has room for 3 primary slots and one 4-slot link in this
// bin. The pair allocation fails, forcing the next insertion to resize.
func TestLoadOrCompute_TriggersResizeWhenFull(t *testing.T) {
	m := newWithSeed[uint64, uint64](Options{InitialSize: 16}, testSeed)
	keys := keysForBin(testSeed, 15, 0, 8, 1<<22)
	if keys == nil {
		t.Skip("could not synthesize colliding keys")
	}
	for i := range 7 {
		m.Insert(keys[i], uint64(i))
	}
	oldIdx := m.getActiveIndex()

	v, loaded := m.LoadOrCompute(keys[7], func() (uint64, bool) { return 777, true })
	if loaded || v != 777 {
		t.Fatalf("got (%d, %v), want (777, false)", v, loaded)
	}
	if m.getActiveIndex() == oldIdx {
		t.Fatal("expected the full bin to trigger a resize")
	}
	for i := range 7 {
		if got, ok := m.Get(keys[i]); !ok || got != uint64(i) {
			t.Fatalf("Get(keys[%d]): got (%d, %v), want (%d, true)", i, got, ok, i)
		}
	}
	if got, ok := m.Get(keys[7]); !ok || got != 777 {
		t.Fatalf("Get(keys[7]): got (%d, %v), want (777, true)", got, ok)
	}
}

func TestLoadOrCompute_InvisibleWhileClaimHeld(t *testing.T) {
	m := New[uint64, uint64](Options{InitialSize: 16})
	m.Insert(1000, 1) // unrelated entry so Range has something legitimate

	started := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		m.LoadOrCompute(77, func() (uint64, bool) {
			close(started)
			<-release
			return 5, true
		})
	}()
	<-started

	if _, ok := m.Get(77); ok {
		t.Error("Get sees the key while its claim is pending")
	}
	if m.Contains(77) {
		t.Error("Contains sees the key while its claim is pending")
	}
	if _, ok := m.Put(77, 9); ok {
		t.Error("Put updated a pending claim")
	}
	if _, ok := m.Delete(77); ok {
		t.Error("Delete removed a pending claim")
	}
	if s := m.Size(); s != 1 {
		t.Errorf("Size with pending claim: got %d, want 1", s)
	}
	m.Range(func(k, v uint64) bool {
		if k == 77 {
			t.Error("Range yielded a pending claim")
		}
		return true
	})

	close(release)
	<-done
	if got, ok := m.Get(77); !ok || got != 5 {
		t.Fatalf("Get after commit: got (%d, %v), want (5, true)", got, ok)
	}
}

// startGate releases waiting goroutines together to encourage overlapping
// operations. Waiters yield unless there are enough Ps for all of them to spin.
type startGate struct {
	n     int32
	spin  bool
	ready atomic.Int32
	open  atomic.Bool
}

func newStartGate(n int) *startGate {
	return &startGate{n: int32(n), spin: runtime.GOMAXPROCS(0) > n}
}

// wait blocks the calling goroutine until release.
func (g *startGate) wait() {
	g.ready.Add(1)
	for !g.open.Load() {
		if !g.spin {
			runtime.Gosched()
		}
	}
}

// release waits for all n goroutines to reach the gate, then opens it.
func (g *startGate) release() {
	for g.ready.Load() < g.n {
		runtime.Gosched()
	}
	g.open.Store(true)
}

// countValidSlotsForHash counts matching Valid slots after all writers stop.
func countValidSlotsForHash(m *Map[uint64, uint64], hash uint64) int {
	idx := m.getActiveIndex()
	pb := idx.getBin(hash)
	h := atomicLoadHeader(&pb.Header)
	n := 0
	for i := range MAX_SLOTS_PER_BIN {
		if h.getSlotState(i) != SlotValid {
			continue
		}
		if slot := idx.getSlotByIndex(pb, i); slot != nil && slot.Key == hash {
			n++
		}
	}
	return n
}

// Insert must rescan for claims after a failed finalize CAS. Otherwise a
// claim published after its reservation can lead to duplicate Valid slots.
// Churn in the same bin increases the chance of taking this retry path.
func TestLoadOrCompute_InsertRetryWithSlotMeetsClaim(t *testing.T) {
	m := newWithSeed[uint64, uint64](Options{InitialSize: 16}, testSeed)
	keys := keysForBin(testSeed, 15, 0, 2, 1<<22)
	if keys == nil {
		t.Skip("could not synthesize colliding keys")
	}
	k, churnKey := keys[0], keys[1]
	hash := maphash.Comparable(testSeed, k)

	const rounds = 2000
	for r := 0; r < rounds; r++ {
		var wg sync.WaitGroup
		gate := newStartGate(3)
		wg.Add(3)
		go func() {
			defer wg.Done()
			gate.wait()
			m.Insert(k, 1)
		}()
		go func() {
			defer wg.Done()
			gate.wait()
			m.LoadOrCompute(k, func() (uint64, bool) { return 2, true })
		}()
		go func() {
			defer wg.Done()
			gate.wait()
			// Change the bin header without writing k.
			m.Insert(churnKey, 3)
			m.Delete(churnKey)
		}()
		gate.release()
		wg.Wait()

		if n := countValidSlotsForHash(m, hash); n != 1 {
			t.Fatalf("round %d: %d Valid slots for the key's hash, want exactly 1 (duplicate key or lost insert)", r, n)
		}
		if _, ok := m.Delete(k); !ok {
			t.Fatalf("round %d: reset Delete failed", r)
		}
	}
}

// An entry stored before the commit CAS must remain invisible while its
// slot is Shadow. Construct that intermediate state directly.
func TestLoadOrCompute_ShadowWithEntryInvisibleMidCommit(t *testing.T) {
	m := newWithSeed[uint64, uint64](Options{InitialSize: 16}, testSeed)
	const k = uint64(7)
	hash := maphash.Comparable(testSeed, k)
	m.Insert(1000, 1) // unrelated entry so Range yields something

	idx := m.getActiveIndex()
	pb := idx.getBin(hash)

	// Publish a claim and store its entry without committing.
	slotIndex := m.chooseInsertSlot(idx, pb)
	if slotIndex < 0 {
		t.Fatal("no free slot for claim")
	}
	h0 := atomicLoadHeader(&pb.Header)
	h1 := h0.setSlotStateAndVersion(slotIndex, SlotTrying)
	if !atomicCASHeader(&pb.Header, h0, h1) {
		t.Fatal("reserve CAS failed on a quiescent bin")
	}
	slot := idx.getSlotByIndex(pb, slotIndex)
	slot.Key = hash
	h2 := h1.setSlotStateAndVersion(slotIndex, SlotShadow)
	if !atomicCASHeader(&pb.Header, h1, h2) {
		t.Fatal("publish CAS failed on a quiescent bin")
	}
	atomicStoreSlotVal(slot, &Entry[uint64, uint64]{Key: k, Value: 42})

	if _, ok := m.Get(k); ok {
		t.Fatal("Get surfaced a mid-commit Shadow entry")
	}
	if m.Contains(k) {
		t.Fatal("Contains surfaced a mid-commit Shadow entry")
	}
	if _, ok := m.Put(k, 9); ok {
		t.Fatal("Put updated a mid-commit Shadow entry")
	}
	if _, ok := m.Delete(k); ok {
		t.Fatal("Delete removed a mid-commit Shadow entry")
	}
	if s := m.Size(); s != 1 {
		t.Fatalf("Size counted a mid-commit Shadow entry: got %d, want 1", s)
	}
	m.Range(func(key, _ uint64) bool {
		if key == k {
			t.Fatal("Range yielded a mid-commit Shadow entry")
		}
		return true
	})

	// Commit the entry and check that it is visible.
	if !atomicCASHeader(&pb.Header, h2, h2.setSlotStateAndVersion(slotIndex, SlotValid)) {
		t.Fatal("commit CAS failed on a quiescent bin")
	}
	if got, ok := m.Get(k); !ok || got != 42 {
		t.Fatalf("Get after commit: got (%d, %v), want (42, true)", got, ok)
	}
	if s := m.Size(); s != 2 {
		t.Fatalf("Size after commit: got %d, want 2", s)
	}
}

// With save=true and no deletes, racing callers share one computation.
func TestLoadOrCompute_FnOnceUnderContention(t *testing.T) {
	const goroutines = 32
	m := New[uint64, uint64](Options{InitialSize: 16})

	var fnRuns atomic.Int32
	var wg sync.WaitGroup
	results := make([]uint64, goroutines)
	loadeds := make([]bool, goroutines)
	start := make(chan struct{})
	for i := range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			results[i], loadeds[i] = m.LoadOrCompute(1, func() (uint64, bool) {
				fnRuns.Add(1)
				return uint64(1000 + i), true
			})
		}()
	}
	close(start)
	wg.Wait()

	if n := fnRuns.Load(); n != 1 {
		t.Fatalf("fn executions: got %d, want exactly 1", n)
	}
	committed, ok := m.Get(1)
	if !ok {
		t.Fatal("computed value missing from the map")
	}
	computers := 0
	for i := range goroutines {
		if results[i] != committed {
			t.Errorf("goroutine %d: got %d, want the committed %d", i, results[i], committed)
		}
		if !loadeds[i] {
			computers++
		}
	}
	if computers != 1 {
		t.Errorf("loaded=false count: got %d, want exactly 1", computers)
	}
}

// An Insert racing a pending claim must wait and lose to the commit.
func TestLoadOrCompute_InsertBacksOffDuringClaim(t *testing.T) {
	m := New[uint64, uint64](Options{InitialSize: 16})

	started := make(chan struct{})
	release := make(chan struct{})
	holderDone := make(chan struct{})
	go func() {
		defer close(holderDone)
		m.LoadOrCompute(8, func() (uint64, bool) {
			close(started)
			<-release
			return 100, true
		})
	}()
	<-started

	type insRes struct {
		prev uint64
		ok   bool
	}
	insDone := make(chan insRes, 1)
	go func() {
		prev, ok := m.Insert(8, 200)
		insDone <- insRes{prev, ok}
	}()

	select {
	case r := <-insDone:
		t.Fatalf("Insert returned (%d, %v) while the claim was pending", r.prev, r.ok)
	case <-time.After(100 * time.Millisecond):
		// still blocked, as required
	}

	close(release)
	<-holderDone
	select {
	case r := <-insDone:
		if r.ok || r.prev != 100 {
			t.Fatalf("Insert after commit: got (%d, %v), want (100, false)", r.prev, r.ok)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Insert still blocked after the claim resolved")
	}
}

// After a save=false abort the blocked Insert proceeds and wins.
func TestLoadOrCompute_InsertWinsAfterAbort(t *testing.T) {
	m := New[uint64, uint64](Options{InitialSize: 16})

	started := make(chan struct{})
	release := make(chan struct{})
	go func() {
		m.LoadOrCompute(8, func() (uint64, bool) {
			close(started)
			<-release
			return 100, false
		})
	}()
	<-started

	type insRes struct {
		prev uint64
		ok   bool
	}
	insDone := make(chan insRes, 1)
	go func() {
		prev, ok := m.Insert(8, 200)
		insDone <- insRes{prev, ok}
	}()

	select {
	case r := <-insDone:
		t.Fatalf("Insert returned (%d, %v) while the claim was pending", r.prev, r.ok)
	case <-time.After(100 * time.Millisecond):
	}

	close(release)
	select {
	case r := <-insDone:
		if !r.ok {
			t.Fatalf("Insert after abort: got ok=false, want a successful insert")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Insert still blocked after the claim aborted")
	}
	if got, ok := m.Get(8); !ok || got != 200 {
		t.Fatalf("Get: got (%d, %v), want (200, true)", got, ok)
	}
}

// A second LoadOrCompute waits out the first and loads its committed value
// without running its own fn.
func TestLoadOrCompute_WaiterLoadsAfterCommit(t *testing.T) {
	m := New[uint64, uint64](Options{InitialSize: 16})

	started := make(chan struct{})
	release := make(chan struct{})
	go func() {
		m.LoadOrCompute(6, func() (uint64, bool) {
			close(started)
			<-release
			return 42, true
		})
	}()
	<-started

	type lcRes struct {
		v      uint64
		loaded bool
	}
	waiterDone := make(chan lcRes, 1)
	go func() {
		v, loaded := m.LoadOrCompute(6, func() (uint64, bool) {
			t.Error("waiter's fn ran while the winner's claim was pending or committed")
			return 0, true
		})
		waiterDone <- lcRes{v, loaded}
	}()

	select {
	case r := <-waiterDone:
		t.Fatalf("waiter returned (%d, %v) while the claim was pending", r.v, r.loaded)
	case <-time.After(100 * time.Millisecond):
	}

	close(release)
	select {
	case r := <-waiterDone:
		if !r.loaded || r.v != 42 {
			t.Fatalf("waiter: got (%d, %v), want (42, true)", r.v, r.loaded)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("waiter still blocked after the claim resolved")
	}
}

// After a save=false abort, the waiting LoadOrCompute runs its own fn.
func TestLoadOrCompute_WaiterComputesAfterAbort(t *testing.T) {
	m := New[uint64, uint64](Options{InitialSize: 16})

	started := make(chan struct{})
	release := make(chan struct{})
	go func() {
		m.LoadOrCompute(6, func() (uint64, bool) {
			close(started)
			<-release
			return 42, false
		})
	}()
	<-started

	type lcRes struct {
		v      uint64
		loaded bool
	}
	waiterDone := make(chan lcRes, 1)
	go func() {
		v, loaded := m.LoadOrCompute(6, func() (uint64, bool) { return 43, true })
		waiterDone <- lcRes{v, loaded}
	}()

	select {
	case r := <-waiterDone:
		t.Fatalf("waiter returned (%d, %v) while the claim was pending", r.v, r.loaded)
	case <-time.After(100 * time.Millisecond):
	}

	close(release)
	select {
	case r := <-waiterDone:
		if r.loaded || r.v != 43 {
			t.Fatalf("waiter: got (%d, %v), want (43, false)", r.v, r.loaded)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("waiter still blocked after the claim aborted")
	}
	if got, ok := m.Get(6); !ok || got != 43 {
		t.Fatalf("Get: got (%d, %v), want (43, true)", got, ok)
	}
}

// Resize carries a pending claim by hash; the holder commits in the new index.
func TestLoadOrCompute_CommitAfterResize(t *testing.T) {
	m := newWithSeed[uint64, uint64](Options{InitialSize: 16}, testSeed)
	const key = uint64(42)
	hash := maphash.Comparable(testSeed, key)

	started := make(chan struct{})
	release := make(chan struct{})
	var v uint64
	var loaded bool
	done := make(chan struct{})
	go func() {
		defer close(done)
		v, loaded = m.LoadOrCompute(key, func() (uint64, bool) {
			close(started)
			<-release
			return 777, true
		})
	}()
	<-started

	oldIdx := m.getActiveIndex()
	oldPb := oldIdx.getBin(hash)
	oldSi, _ := m.findShadowSlot(oldIdx, oldPb, atomicLoadHeader(&oldPb.Header), hash)
	if oldSi < 0 {
		t.Fatal("claim not present in the old index while fn runs")
	}

	if !m.triggerResize() {
		t.Fatal("triggerResize refused")
	}
	newIdx := m.getActiveIndex()
	if newIdx == oldIdx {
		t.Fatal("resize did not swap the active index")
	}

	// Carrying a claim leaves its old hash intact, without a sentinel.
	if st := atomicLoadHeader(&oldPb.Header).getBinState(); st != BinDoneTransfer {
		t.Fatalf("old bin state: got %d, want BinDoneTransfer", st)
	}
	if got := oldIdx.getSlotByIndex(oldPb, oldSi).Key; got != hash {
		t.Fatalf("old claim slot key: got %#x, want the claim hash %#x (sentineled?)", got, hash)
	}

	// Carried claim exists in the new index and the key is still absent.
	newPb := newIdx.getBin(hash)
	if si, _ := m.findShadowSlot(newIdx, newPb, atomicLoadHeader(&newPb.Header), hash); si < 0 {
		t.Fatal("claim was not carried into the new index")
	}
	if _, ok := m.Get(key); ok {
		t.Fatal("key visible while the carried claim is pending")
	}

	close(release)
	<-done
	if loaded || v != 777 {
		t.Fatalf("got (%d, %v), want (777, false)", v, loaded)
	}
	if got, ok := m.Get(key); !ok || got != 777 {
		t.Fatalf("Get after relocated commit: got (%d, %v), want (777, true)", got, ok)
	}
	if s := m.Size(); s != 1 {
		t.Fatalf("Size: got %d, want 1", s)
	}
}

// The abort path relocates the same way and releases the carried slot.
func TestLoadOrCompute_AbortAfterResize(t *testing.T) {
	m := newWithSeed[uint64, uint64](Options{InitialSize: 16}, testSeed)
	const key = uint64(42)
	hash := maphash.Comparable(testSeed, key)

	started := make(chan struct{})
	release := make(chan struct{})
	var v uint64
	var loaded bool
	done := make(chan struct{})
	go func() {
		defer close(done)
		v, loaded = m.LoadOrCompute(key, func() (uint64, bool) {
			close(started)
			<-release
			return 777, false
		})
	}()
	<-started

	if !m.triggerResize() {
		t.Fatal("triggerResize refused")
	}
	close(release)
	<-done

	if loaded || v != 777 {
		t.Fatalf("got (%d, %v), want (777, false)", v, loaded)
	}
	if _, ok := m.Get(key); ok {
		t.Fatal("key visible after an aborted claim")
	}
	newIdx := m.getActiveIndex()
	newPb := newIdx.getBin(hash)
	if si, _ := m.findShadowSlot(newIdx, newPb, atomicLoadHeader(&newPb.Header), hash); si >= 0 {
		t.Fatal("carried claim slot not released by the abort")
	}
	if s := m.Size(); s != 0 {
		t.Fatalf("Size: got %d, want 0", s)
	}

	// The released slot is reusable on the new index.
	if got, loaded := m.LoadOrCompute(key, func() (uint64, bool) { return 5, true }); loaded || got != 5 {
		t.Fatalf("recompute: got (%d, %v), want (5, false)", got, loaded)
	}
}

// The holder follows its claim across two resizes before committing.
func TestLoadOrCompute_DoubleRelocation(t *testing.T) {
	m := newWithSeed[uint64, uint64](Options{InitialSize: 16}, testSeed)
	const key = uint64(42)

	started := make(chan struct{})
	release := make(chan struct{})
	var v uint64
	var loaded bool
	done := make(chan struct{})
	go func() {
		defer close(done)
		v, loaded = m.LoadOrCompute(key, func() (uint64, bool) {
			close(started)
			<-release
			return 777, true
		})
	}()
	<-started

	first := m.getActiveIndex()
	if !m.triggerResize() {
		t.Fatal("first triggerResize refused")
	}
	second := m.getActiveIndex()
	if second == first {
		t.Fatal("first resize did not swap")
	}
	if !m.triggerResize() {
		t.Fatal("second triggerResize refused")
	}
	if m.getActiveIndex() == second {
		t.Fatal("second resize did not swap")
	}

	close(release)
	<-done
	if loaded || v != 777 {
		t.Fatalf("got (%d, %v), want (777, false)", v, loaded)
	}
	if got, ok := m.Get(key); !ok || got != 777 {
		t.Fatalf("Get after double relocation: got (%d, %v), want (777, true)", got, ok)
	}
}

// A claim sitting in a link slot is carried like any other.
func TestLoadOrCompute_LinkClaimCarriedAcrossResize(t *testing.T) {
	m := newWithSeed[uint64, uint64](Options{InitialSize: 16}, testSeed)
	keys := keysForBin(testSeed, 15, 0, 4, 1<<22)
	if keys == nil {
		t.Skip("could not synthesize colliding keys")
	}
	for i := range 3 {
		m.Insert(keys[i], uint64(i))
	}

	started := make(chan struct{})
	release := make(chan struct{})
	var v uint64
	var loaded bool
	done := make(chan struct{})
	go func() {
		defer close(done)
		v, loaded = m.LoadOrCompute(keys[3], func() (uint64, bool) {
			close(started)
			<-release
			return 99, true
		})
	}()
	<-started

	if !m.triggerResize() {
		t.Fatal("triggerResize refused")
	}
	close(release)
	<-done

	if loaded || v != 99 {
		t.Fatalf("got (%d, %v), want (99, false)", v, loaded)
	}
	for i := range 3 {
		if got, ok := m.Get(keys[i]); !ok || got != uint64(i) {
			t.Fatalf("Get(keys[%d]): got (%d, %v), want (%d, true)", i, got, ok, i)
		}
	}
	if got, ok := m.Get(keys[3]); !ok || got != 99 {
		t.Fatalf("Get(keys[3]): got (%d, %v), want (99, true)", got, ok)
	}
}

// Recover inside fn so its zero-valued named returns abort the claim.
func TestLoadOrCompute_RecoverInsideFnPattern(t *testing.T) {
	m := New[uint64, uint64](Options{InitialSize: 16})

	v, loaded := m.LoadOrCompute(12, func() (v uint64, save bool) {
		defer func() { recover() }()
		panic("boom")
	})
	if loaded || v != 0 {
		t.Fatalf("recovered compute: got (%d, %v), want the abort (0, false)", v, loaded)
	}
	if _, ok := m.Get(12); ok {
		t.Fatal("key visible after an aborted (recovered) compute")
	}

	// The abort released the claim.
	if v, loaded := m.LoadOrCompute(12, func() (uint64, bool) { return 9, true }); loaded || v != 9 {
		t.Fatalf("recompute: got (%d, %v), want (9, false)", v, loaded)
	}
	if got, ok := m.Get(12); !ok || got != 9 {
		t.Fatalf("Get: got (%d, %v), want (9, true)", got, ok)
	}
}

// An unrecovered panic propagates and leaves the claim pending. Readers
// still see the key as absent. Avoid Insert and LoadOrCompute on that hash:
// they would wait indefinitely.
func TestLoadOrCompute_UnrecoveredPanicPropagates(t *testing.T) {
	m := New[uint64, uint64](Options{InitialSize: 16})
	m.Insert(1000, 1)

	func() {
		defer func() {
			if r := recover(); r != "boom" {
				t.Fatalf("recover: got %v, want \"boom\"", r)
			}
		}()
		m.LoadOrCompute(13, func() (uint64, bool) { panic("boom") })
		t.Fatal("unreachable: LoadOrCompute returned past a panicking fn")
	}()

	if _, ok := m.Get(13); ok {
		t.Fatal("key visible after fn panicked")
	}
	if _, ok := m.Delete(13); ok {
		t.Fatal("Delete removed an orphaned claim")
	}
	if got, ok := m.Get(1000); !ok || got != 1 {
		t.Fatalf("unrelated key: got (%d, %v), want (1, true)", got, ok)
	}
	if s := m.Size(); s != 1 {
		t.Fatalf("Size: got %d, want 1", s)
	}
}

// Concurrent callers share one computation per key while the table resizes.
func TestLoadOrCompute_StressWithResizes(t *testing.T) {
	const (
		goroutines = 16
		numKeys    = 300
	)
	m := New[uint64, uint64](Options{InitialSize: 16})

	fnRuns := make([]atomic.Int32, numKeys)
	results := make([][]uint64, goroutines)
	var wg sync.WaitGroup
	start := make(chan struct{})

	for g := range goroutines {
		results[g] = make([]uint64, numKeys)
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			// Each goroutine walks the keys at a different stride to spread
			// contention.
			for i := range numKeys {
				k := uint64((i*(g+7) + g) % numKeys)
				v, _ := m.LoadOrCompute(k, func() (uint64, bool) {
					fnRuns[k].Add(1)
					return k*1000 + uint64(g) + 1, true // never 0: 0 marks "not visited" below
				})
				results[g][k] = v
			}
		}()
	}

	// Force resizes while the workload runs.
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for range 3 {
			m.triggerResize()
			time.Sleep(time.Millisecond)
		}
	}()

	close(start)
	wg.Wait()

	for k := range numKeys {
		if n := fnRuns[k].Load(); n != 1 {
			t.Errorf("key %d: fn executions = %d, want 1", k, n)
		}
		committed, ok := m.Get(uint64(k))
		if !ok {
			t.Errorf("key %d missing after stress", k)
			continue
		}
		for g := range goroutines {
			if got := results[g][k]; got != 0 && got != committed {
				t.Errorf("key %d: goroutine %d returned %d, committed %d", k, g, got, committed)
			}
		}
	}
	if s := m.Size(); s != numKeys {
		t.Errorf("Size: got %d, want %d", s, numKeys)
	}
}

// Compare the existing-key path with BenchmarkGetHit.
func BenchmarkLoadOrComputeHit(b *testing.B) {
	keys := benchKeys()
	m := prefilledMap(keys)
	fn := func() (uint64, bool) { b.Fatal("fn ran on hit path"); return 0, false }
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		if _, loaded := m.LoadOrCompute(keys[i&(benchKeyspace-1)], fn); !loaded {
			b.Fatal("miss")
		}
	}
}

// A successful compute reserves, publishes, and commits before deletion.
func BenchmarkLoadOrComputeCompute(b *testing.B) {
	keys := benchKeys()
	m := prefilledMap(keys)
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		k := uint64(i) | 1<<62 | 1<<63
		if _, loaded := m.LoadOrCompute(k, func() (uint64, bool) { return k, true }); loaded {
			b.Fatal("unexpected hit")
		}
		if _, ok := m.Delete(k); !ok {
			b.Fatal("delete failed")
		}
	}
}

// Valid (0b11) and Shadow (0b10) share their high bit: every mask must match
// exactly, and the bin-state bits must never leak into the slot masks.
func TestHeaderMasks_ShadowExactness(t *testing.T) {
	var h Header
	h = h.setSlotStateAndVersion(0, SlotValid)
	h = h.setSlotStateAndVersion(1, SlotShadow)
	h = h.setSlotStateAndVersion(2, SlotTrying)
	h = h.setSlotStateAndVersion(3, SlotShadow)  // link slot (single, first)
	h = h.setSlotStateAndVersion(7, SlotValid)   // link slot (pair0, first)
	h = h.setSlotStateAndVersion(14, SlotShadow) // last slot
	h = h.setBinState(BinInTransfer)             // must not contaminate any mask

	if got := h.getSlotState(1); got != SlotShadow {
		t.Errorf("getSlotState(1): got %d, want SlotShadow", got)
	}
	if got, want := h.validMask3(), uint32(0x01); got != want {
		t.Errorf("validMask3: got %#x, want %#x (Shadow must not read as Valid)", got, want)
	}
	if got, want := h.shadowMask3(), uint32(0x04); got != want {
		t.Errorf("shadowMask3: got %#x, want %#x", got, want)
	}
	if got, want := h.invalidMask3(), uint32(0x00); got != want {
		t.Errorf("invalidMask3: got %#x, want %#x", got, want)
	}
	if got, want := h.validMask4(PRIMARY_SLOTS), uint32(0x00); got != want {
		t.Errorf("validMask4(3): got %#x, want %#x", got, want)
	}
	if got, want := h.shadowMask4(PRIMARY_SLOTS), uint32(0x01); got != want {
		t.Errorf("shadowMask4(3): got %#x, want %#x", got, want)
	}
	if got, want := h.validMask4(PRIMARY_SLOTS+LINK_SLOTS), uint32(0x01); got != want {
		t.Errorf("validMask4(7): got %#x, want %#x", got, want)
	}
	// Spread positions for slots 1, 3, 14: bits 2, 6, 28.
	if got, want := h.shadowMaskAll(), uint32(1<<2|1<<6|1<<28); got != want {
		t.Errorf("shadowMaskAll: got %#x, want %#x", got, want)
	}

	// A Valid slot 14 next to Done bin-state bits: neither mask may see
	// phantom bits from the header's [31:30] region.
	var h2 Header
	h2 = h2.setSlotStateAndVersion(14, SlotValid)
	h2 = h2.setBinState(BinDoneTransfer)
	if got, want := h2.shadowMaskAll(), uint32(0); got != want {
		t.Errorf("shadowMaskAll with Valid slot 14 + Done: got %#x, want %#x", got, want)
	}
	if got, want := h2.validMask4(PRIMARY_SLOTS+2*LINK_SLOTS), uint32(0x40); got != want {
		t.Errorf("validMask4(11) for slot 14: got %#x, want %#x", got, want)
	}
	var h3 Header
	h3 = h3.setSlotStateAndVersion(14, SlotShadow)
	h3 = h3.setBinState(BinInTransfer)
	if got, want := h3.shadowMaskAll(), uint32(1<<28); got != want {
		t.Errorf("shadowMaskAll slot14 Shadow + InTransfer: got %#x, want %#x", got, want)
	}
	if got, want := h3.validMask4(PRIMARY_SLOTS+2*LINK_SLOTS), uint32(0x00); got != want {
		t.Errorf("validMask4(11) slot14 Shadow: got %#x, want %#x", got, want)
	}
}

// After a failed publish CAS, LoadOrCompute must rescan for both entries and
// claims. Race two computes with an Insert under header churn, then check
// that one writer won, all callers agree, and no computed value was discarded.
func TestLoadOrCompute_PublishRetryMeetsInsertOrClaim(t *testing.T) {
	m := newWithSeed[uint64, uint64](Options{InitialSize: 16}, testSeed)
	keys := keysForBin(testSeed, 15, 0, 2, 1<<22)
	if keys == nil {
		t.Skip("could not synthesize colliding keys")
	}
	k, churnKey := keys[0], keys[1]
	hash := maphash.Comparable(testSeed, k)

	const rounds = 2000
	for r := 0; r < rounds; r++ {
		var fnRuns atomic.Int32
		var wg sync.WaitGroup
		gate := newStartGate(4)
		res := make([]uint64, 2)
		loaded := make([]bool, 2)
		wg.Add(4)
		for i := range 2 {
			go func() {
				defer wg.Done()
				gate.wait()
				res[i], loaded[i] = m.LoadOrCompute(k, func() (uint64, bool) {
					fnRuns.Add(1)
					return 100 + uint64(i), true
				})
			}()
		}
		go func() {
			defer wg.Done()
			gate.wait()
			m.Insert(k, 1)
		}()
		go func() {
			defer wg.Done()
			gate.wait()
			// Change the bin header without writing k.
			m.Insert(churnKey, 3)
			m.Delete(churnKey)
		}()
		gate.release()
		wg.Wait()

		if n := countValidSlotsForHash(m, hash); n != 1 {
			t.Fatalf("round %d: %d Valid slots for the key's hash, want exactly 1 (duplicate key or lost insert)", r, n)
		}
		got, ok := m.Get(k)
		if !ok {
			t.Fatalf("round %d: key missing", r)
		}
		switch n := fnRuns.Load(); n {
		case 0:
			if got != 1 {
				t.Fatalf("round %d: fn never ran but value is %d, want the Insert's 1", r, got)
			}
		case 1:
			if got != 100 && got != 101 {
				t.Fatalf("round %d: fn ran once (save=true) but value is %d, want the computed value", r, got)
			}
		default:
			t.Fatalf("round %d: fn ran %d times, want at most 1", r, n)
		}
		computers := 0
		for i := range 2 {
			if res[i] != got {
				t.Fatalf("round %d: caller %d returned %d, map holds %d", r, i, res[i], got)
			}
			if !loaded[i] {
				computers++
			}
		}
		if computers != int(fnRuns.Load()) {
			t.Fatalf("round %d: loaded=false count %d, fn runs %d", r, computers, fnRuns.Load())
		}
		if _, ok := m.Delete(k); !ok {
			t.Fatalf("round %d: reset Delete failed", r)
		}
	}
}

// A resize triggered inside fn must carry the caller's own claim.
func TestLoadOrCompute_ResizeTriggeredInsideFn(t *testing.T) {
	m := New[uint64, uint64](Options{InitialSize: 16})
	oldIdx := m.getActiveIndex()

	calls := 0
	v, loaded := m.LoadOrCompute(7, func() (uint64, bool) {
		calls++
		// 16 bins share a 2-bucket link arena, so a few hundred inserts are
		// bound to overflow a bin past its links and force a resize.
		for k := uint64(1000); k < 1400; k++ {
			m.Insert(k, k)
		}
		if m.getActiveIndex() == oldIdx {
			t.Error("inserts inside fn did not trigger a resize")
		}
		return 77, true
	})
	if loaded || v != 77 || calls != 1 {
		t.Fatalf("got (%d, %v) calls=%d, want (77, false) calls=1", v, loaded, calls)
	}
	if got, ok := m.Get(7); !ok || got != 77 {
		t.Fatalf("Get after commit across an fn-triggered resize: got (%d, %v), want (77, true)", got, ok)
	}
	for k := uint64(1000); k < 1400; k++ {
		if got, ok := m.Get(k); !ok || got != k {
			t.Fatalf("Get(%d): got (%d, %v), want (%d, true)", k, got, ok, k)
		}
	}
	if s := m.Size(); s != 401 {
		t.Fatalf("Size: got %d, want 401", s)
	}
}

// A nested compute with a different hash can use the same bin.
func TestLoadOrCompute_NestedComputeOfOtherKey(t *testing.T) {
	m := newWithSeed[uint64, uint64](Options{InitialSize: 16}, testSeed)
	keys := keysForBin(testSeed, 15, 0, 2, 1<<22)
	if keys == nil {
		t.Skip("could not synthesize colliding keys")
	}

	inner := 0
	v, loaded := m.LoadOrCompute(keys[0], func() (uint64, bool) {
		iv, iloaded := m.LoadOrCompute(keys[1], func() (uint64, bool) {
			inner++
			return 2, true
		})
		if iloaded || iv != 2 {
			t.Errorf("inner compute: got (%d, %v), want (2, false)", iv, iloaded)
		}
		return 1, true
	})
	if loaded || v != 1 || inner != 1 {
		t.Fatalf("outer compute: got (%d, %v) inner=%d, want (1, false) inner=1", v, loaded, inner)
	}
	for i, want := range []uint64{1, 2} {
		if got, ok := m.Get(keys[i]); !ok || got != want {
			t.Fatalf("Get(keys[%d]): got (%d, %v), want (%d, true)", i, got, ok, want)
		}
	}
}

// An Insert parked on a claim stays parked across a resize that carries the
// claim, and after the relocated commit it returns the committed value.
func TestLoadOrCompute_ParkedInsertFollowsRelocatedCommit(t *testing.T) {
	m := New[uint64, uint64](Options{InitialSize: 16})

	started := make(chan struct{})
	release := make(chan struct{})
	holderDone := make(chan struct{})
	go func() {
		defer close(holderDone)
		m.LoadOrCompute(8, func() (uint64, bool) {
			close(started)
			<-release
			return 100, true
		})
	}()
	<-started

	type insRes struct {
		prev uint64
		ok   bool
	}
	insDone := make(chan insRes, 1)
	go func() {
		prev, ok := m.Insert(8, 200)
		insDone <- insRes{prev, ok}
	}()

	select {
	case r := <-insDone:
		t.Fatalf("Insert returned (%d, %v) while the claim was pending", r.prev, r.ok)
	case <-time.After(50 * time.Millisecond):
	}

	if !m.triggerResize() {
		t.Fatal("triggerResize refused")
	}
	select {
	case r := <-insDone:
		t.Fatalf("Insert returned (%d, %v) while the carried claim was pending", r.prev, r.ok)
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	<-holderDone
	select {
	case r := <-insDone:
		if r.ok || r.prev != 100 {
			t.Fatalf("Insert after relocated commit: got (%d, %v), want (100, false)", r.prev, r.ok)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Insert still blocked after the relocated commit")
	}
	if got, ok := m.Get(8); !ok || got != 100 {
		t.Fatalf("Get: got (%d, %v), want (100, true)", got, ok)
	}
}

func TestClaimHeaderTransitions(t *testing.T) {
	m := newWithSeed[uint64, uint64](Options{InitialSize: 32}, testSeed)
	idx := m.getActiveIndex()
	keys := keysForBin(testSeed, idx.mask, 0, MAX_SLOTS_PER_BIN, 1<<22)
	if keys == nil {
		t.Fatal("could not synthesize colliding keys")
	}
	pb := idx.getBinByIndex(0)
	if idx.attachSingle(pb) == NO_LINK || idx.attachPair(pb) == NO_LINK {
		t.Fatal("could not attach all link buckets")
	}

	for i := range MAX_SLOTS_PER_BIN {
		for pattern := range 4 {
			// Construct expected fields independently of the transition helpers.
			headerFor := func(version uint32, state uint8) Header {
				h := Header(version) << versionShift
				for j := range MAX_SLOTS_PER_BIN {
					s := uint8((j + pattern) % 4)
					if j == i {
						s = state
					}
					h |= Header(s) << (2 * j)
				}
				return h
			}
			for _, version := range []uint32{0, 1, 0x80000000, 0xfffffffe, 0xffffffff} {
				for _, save := range []bool{false, true} {
					var entries [MAX_SLOTS_PER_BIN]*Entry[uint64, uint64]
					for j := range MAX_SLOTS_PER_BIN {
						slot := idx.getSlotByIndex(pb, j)
						slot.Key = maphash.Comparable(testSeed, keys[j])
						if j != i && (j+pattern)%4 == SlotValid {
							entries[j] = &Entry[uint64, uint64]{Key: keys[j], Value: uint64(j)}
						}
						slot.Val = entries[j]
					}
					slot := idx.getSlotByIndex(pb, i)
					before := headerFor(version, SlotTrying)
					pb.Header = before
					if !m.publishClaim(pb, before, i) {
						t.Fatalf("slot %d: publish failed", i)
					}
					if want := headerFor(version+1, SlotShadow); pb.Header != want {
						t.Fatalf("publish slot %d, pattern %d, version %#x: got %#x, want %#x", i, pattern, version, pb.Header, want)
					}

					calls := 0
					value, loaded := m.computeAndResolve(idx, pb, slot, i, slot.Key, keys[i], func() (uint64, bool) {
						calls++
						return 77, save
					})
					if calls != 1 || value != 77 || loaded {
						t.Fatalf("slot %d, save %t: calls=%d, value=%d, loaded=%t", i, save, calls, value, loaded)
					}
					state := uint8(SlotInvalid)
					if save {
						state = SlotValid
					}
					if want := headerFor(version+2, state); pb.Header != want {
						t.Fatalf("resolve slot %d, pattern %d, version %#x, save %t: got %#x, want %#x", i, pattern, version, save, pb.Header, want)
					}
					if save {
						if entry := atomicLoadSlotVal(slot); entry == nil || entry.Key != keys[i] || entry.Value != 77 {
							t.Fatalf("slot %d: committed entry = %+v", i, entry)
						}
					} else if atomicLoadSlotVal(slot) != nil {
						t.Fatalf("slot %d: abort stored an entry", i)
					}
					for j := range MAX_SLOTS_PER_BIN {
						if j != i && atomicLoadSlotVal(idx.getSlotByIndex(pb, j)) != entries[j] {
							t.Fatalf("resolve slot %d changed slot %d's entry", i, j)
						}
					}
				}
			}
		}
	}
}

func TestUnreserveSlot(t *testing.T) {
	for i := range MAX_SLOTS_PER_BIN {
		for _, clearEntry := range []bool{false, true} {
			for binState := uint8(BinNoTransfer); binState <= BinDoneTransfer; binState++ {
				t.Run(fmt.Sprintf("slot_%d/entry_%t/bin_%d", i, clearEntry, binState), func(t *testing.T) {
					m, idx, pb := reservationTestBin(t, 3)
					var h Header
					for j := range MAX_SLOTS_PER_BIN {
						h = h.setSlotStateAndVersion(j, SlotValid)
					}
					h = h.setSlotStateAndVersion(i, SlotTrying).setBinState(binState)
					pb.Header = h
					slot := idx.getSlotByIndex(pb, i)
					slot.Key = 42
					var clear *Slot[uint64, uint64]
					if clearEntry {
						slot.Val = &Entry[uint64, uint64]{Key: 42, Value: 7}
						clear = slot
					}
					entry := slot.Val

					m.unreserveSlot(pb, clear, i)

					if slot.Key != 42 {
						t.Fatal("unreserve changed the hash")
					}
					if binState != BinNoTransfer {
						if pb.Header != h || slot.Val != entry {
							t.Fatal("unreserve changed a transferring bin")
						}
						return
					}
					if slot.Val != nil {
						t.Fatal("released slot retains an entry")
					}
					for j := range MAX_SLOTS_PER_BIN {
						want := uint8(SlotValid)
						if j == i {
							want = SlotInvalid
						}
						if got := pb.Header.getSlotState(j); got != want {
							t.Fatalf("slot %d state: got %d, want %d", j, got, want)
						}
					}
					if got, want := uint64(pb.Header)>>versionShift, (uint64(h)>>versionShift)+1; got != want {
						t.Fatalf("version: got %d, want %d", got, want)
					}
					if pb.Header.getBinState() != BinNoTransfer {
						t.Fatal("unreserve changed the bin state")
					}
				})
			}
		}
	}
}
