package allocator

// LoadOrCompute returns the existing value for key if present (loaded=true).
// Otherwise it calls fn to compute a new value. If fn returns save=true the
// value is inserted and returned (loaded=false). If fn returns save=false
// nothing is stored and the value is returned as-is.
//
// Under concurrent calls for the same absent key, fn may be called by multiple
// goroutines but only one will insert successfully.
// Use [LoadOrComputeOnce] if a compute function should only be called once per key.
func (m *Map[K, V]) LoadOrCompute(key K, fn func() (value V, save bool)) (value V, loaded bool) {
	if v, ok := m.Get(key); ok {
		return v, true
	}
	val, save := fn()
	if !save {
		return val, false
	}
	prev, inserted := m.Insert(key, val)
	if inserted {
		return val, false
	}
	return prev, true
}

// LoadOrComputeOnce is like [LoadOrCompute], except that fn is guaranteed to be
// called at most once per absent key. Loads remain lock-free. Concurrent callers
// for the same absent key coordinate via a per-key channel so that exactly one
// goroutine computes while the others block until the result is ready.
// If fn returns save=false, the coordination resets and the next caller retries.
func (m *Map[K, V]) LoadOrComputeOnce(key K, fn func() (value V, save bool)) (value V, loaded bool) {
	for {
		if v, ok := m.Get(key); ok {
			// Happy path.
			return v, true
		}

		cm := m.getComputeMap()
		ch := make(chan struct{})
		existing, inserted := cm.Insert(key, ch)

		if !inserted {
			<-existing // wait for winner to finish
			continue
		}

		// We own the computation for this key.
		defer func() {
			close(ch)
			cm.Delete(key)
		}()
		break
	}

	// Re-check that the key wasn't added.
	if v, ok := m.Get(key); ok {
		return v, true
	}
	val, save := fn()
	if !save {
		return val, false
	}
	if prev, ins := m.Insert(key, val); !ins {
		return prev, true
	}
	return val, false
}

func (m *Map[K, V]) getComputeMap() *Map[K, chan struct{}] {
	if cm := m.compute.Load(); cm != nil {
		return cm
	}
	cm := New[K, chan struct{}](Options{InitialSize: 16})
	if m.compute.CompareAndSwap(nil, cm) {
		return cm
	}
	return m.compute.Load()
}
