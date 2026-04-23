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

func (a *xsyncAdapter[K, V]) Size() int { return a.m.Size() }
func (a *xsyncAdapter[K, V]) Close()    {}
