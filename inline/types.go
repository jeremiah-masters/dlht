package inline

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

type Integer interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 |
		~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~uintptr
}

// Slot is 16 bytes: 8B Key (uint64) + 8B Val (uint64). Must remain exactly 16B for DWCAS.
type Slot[V Integer] struct {
	_ structs.HostLayout

	Key uint64 // actual uint64 key
	Val uint64 // inline-encoded value
}

// Header bit layout: [63:32] version | [31:30] bin_state | [29:0] slot_states (15x2b)
type Header uint64

// LinkMeta packs link bucket indices into 64 bits: [63:32] pairStart | [31:0] single
type LinkMeta uint64

type PrimaryBucket[V Integer] struct {
	_ structs.HostLayout

	Header   Header
	LinkMeta LinkMeta
	Slots    [PRIMARY_SLOTS]Slot[V]
}

type LinkBucket[V Integer] struct {
	_ structs.HostLayout

	Slots [LINK_SLOTS]Slot[V]
}

type index[V Integer] struct {
	_ structs.HostLayout

	mask         uint64
	bins         []PrimaryBucket[V]
	links        []LinkBucket[V]
	_            [cpu.CacheLineSize - 56]byte // 56 = 8 + 24 + 24  (mask + 2 slices)
	linkNext     uint32
	linkCapacity uint32
	indexNext    atomic.Pointer[index[V]] // Store next index after resize
}

type Map[V Integer] struct {
	_ structs.HostLayout

	active     atomic.Pointer[index[V]] // Current active index
	hashConfig HashConfig
	_          [cpu.CacheLineSize - 32]byte // 32 = 8 (active) + 24 (HashConfig)
	resizeCtx  atomic.Pointer[resizeContext[V]]
}

type Options struct {
	InitialSize uint64
}
