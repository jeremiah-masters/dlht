package adapters

// MapAdapter is a thin generic wrapper around concurrent map implementations.
// All methods must be safe for concurrent use.
type MapAdapter[K comparable, V any] interface {
	Name() string
	Get(key K) (V, bool)
	Insert(key K, value V) bool // insert-if-absent; true if inserted (key was new)
	Delete(key K) bool          // true if deleted (key existed)
	Put(key K, value V) bool    // update-if-exists; true if updated (key existed)
	Size() int
	Close()
}
