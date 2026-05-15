package adapters

// MapAdapter is a thin generic wrapper around concurrent map implementations.
// All methods must be safe for concurrent use.
type MapAdapter[K comparable, V any] interface {
	Name() string
	Get(key K) (V, bool)
	Insert(key K, value V) bool // insert-if-absent; true if inserted (key was new)
	Delete(key K) bool          // true if deleted (key existed)
	Put(key K, value V) bool    // update-if-exists; true if updated (key existed)
	// LoadOrCompute returns existing value if present (loaded=true), otherwise
	// calls fn, inserts the result, and returns it (loaded=false).
	// fn may be called by multiple goroutines; only one result is kept.
	LoadOrCompute(key K, fn func() (V, bool)) (V, bool)
	// LoadOrComputeOnce is like LoadOrCompute but fn is called at most once per key.
	LoadOrComputeOnce(key K, fn func() (V, bool)) (V, bool)
	Range(yield func(K, V) bool)
	Size() int
	Close()
}
