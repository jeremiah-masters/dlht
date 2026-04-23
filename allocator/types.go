package allocator

import (
	"structs"
	"sync/atomic"

	"github.com/jeremiah-masters/dlht/internal/cpu"
)

const (
	BinNoTransfer   = 0
	BinInTransfer   = 1
	BinDoneTransfer = 2
)

const (
	SlotInvalid = 0
	SlotTrying  = 1
	SlotValid   = 2
)

const (
	PRIMARY_SLOTS     = 3
	LINK_SLOTS        = 4
	MAX_LINKS         = 3
	MAX_SLOTS_PER_BIN = PRIMARY_SLOTS + MAX_LINKS*LINK_SLOTS
)

type Key = comparable

type Entry[K Key, V any] struct {
	_ structs.HostLayout

	Key   K
	Value V
}

// Slot is 16 bytes: 8B Key (uint64) + 8B Val (pointer). Must remain exactly 16B for DWCAS.
type Slot[K Key, V any] struct {
	_ structs.HostLayout

	Key uint64       // hash of the key
	Val *Entry[K, V] // wraps variable-length key+value
}

// Header bit layout: [63:32] version | [31:30] bin_state | [29:0] slot_states (15×2b)
type Header uint64

// LinkMeta packs link bucket indices into 64 bits: [63:32] pairStart | [31:0] single
type LinkMeta uint64

type PrimaryBucket[K Key, V any] struct {
	_ structs.HostLayout

	Header   Header
	LinkMeta LinkMeta
	Slots    [PRIMARY_SLOTS]Slot[K, V]
}

type LinkBucket[K Key, V any] struct {
	_ structs.HostLayout

	Slots [LINK_SLOTS]Slot[K, V]
}

type index[K Key, V any] struct {
	_ structs.HostLayout

	mask         uint64
	bins         []PrimaryBucket[K, V]
	links        []LinkBucket[K, V]
	_            [cpu.CacheLineSize - 56]byte // 56 = 8 + 24 + 24  (mask + 2 slices)
	linkNext     uint32
	linkCapacity uint32
	indexNext    atomic.Pointer[index[K, V]] // Store next index after resize
}

type Map[K Key, V any] struct {
	_ structs.HostLayout

	active     atomic.Pointer[index[K, V]] // Current active index
	hashConfig HashConfig
	_          [cpu.CacheLineSize - 16]byte // 16 = 8 + 8 (active pointer + hash config)
	resizeCtx  atomic.Pointer[resizeContext[K, V]]
}

type Options struct {
	InitialSize uint64
}
