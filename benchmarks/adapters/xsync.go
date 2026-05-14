package adapters

import "github.com/puzpuzpuz/xsync/v4"

type xsyncAdapter[K comparable, V any] struct {
	m *xsync.Map[K, V]
}

func NewXSyncAdapter[K comparable, V any]() MapAdapter[K, V] {
	return &xsyncAdapter[K, V]{
		m: xsync.NewMap[K, V](),
	}
}

func (a *xsyncAdapter[K, V]) Name() string        { return "xsync" }
func (a *xsyncAdapter[K, V]) Get(key K) (V, bool) { return a.m.Load(key) }

func (a *xsyncAdapter[K, V]) Insert(key K, val V) bool {
	_, loaded := a.m.LoadOrStore(key, val)
	return !loaded // true if inserted (key was new)
}

func (a *xsyncAdapter[K, V]) Delete(key K) bool {
	_, loaded := a.m.LoadAndDelete(key)
	return loaded // true if deleted (key existed)
}

func (a *xsyncAdapter[K, V]) Put(key K, val V) bool {
	_, loaded := a.m.LoadAndStore(key, val)
	return loaded // true if updated (key existed)
}

func (a *xsyncAdapter[K, V]) Range(yield func(K, V) bool) {
	a.m.Range(yield)
}

func (a *xsyncAdapter[K, V]) LoadOrCompute(key K, fn func() (V, bool)) (V, bool) {
	return a.m.LoadOrCompute(key, func() (V, bool) {
		v, save := fn()
		return v, !save // xsync: cancel=true means don't store
	})
}

// xsync's LoadOrCompute already holds a bucket lock during compute, so fn is called at most once.
func (a *xsyncAdapter[K, V]) LoadOrComputeOnce(key K, fn func() (V, bool)) (V, bool) {
	return a.m.LoadOrCompute(key, func() (V, bool) {
		v, save := fn()
		return v, !save
	})
}

func (a *xsyncAdapter[K, V]) Size() int { return a.m.Size() }
func (a *xsyncAdapter[K, V]) Close()    {}
