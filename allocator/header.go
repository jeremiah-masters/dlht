package allocator

import (
	"sync/atomic"
	"unsafe"
)

const (
	versionShift   = 32
	binStateShift  = 30
	binStateMask   = 0x3 << binStateShift
	slotStatesMask = (1 << 30) - 1
)

func (h Header) getBinState() uint8 { return uint8((uint64(h) & binStateMask) >> binStateShift) }

func (h Header) getSlotStates() uint32 { return uint32(uint64(h) & slotStatesMask) }

func (h Header) getSlotState(slotIdx int) uint8 {
	s := h.getSlotStates()
	shift := uint(slotIdx*2) & 63 // prove to compiler shift < 64; elided on both arm64 and amd64
	return uint8((s >> shift) & 0x3)
}

func (h Header) setBinState(binState uint8) Header {
	raw := uint64(h) &^ binStateMask
	raw |= uint64(binState&0x3) << binStateShift
	return Header(raw)
}

// setSlotStateAndVersion sets a slots state and increments the version in one pass.
// Avoids the redundant header decomposition of setSlotState().incrementVersion().
func (h Header) setSlotStateAndVersion(slotIdx int, state uint8) Header {
	shift := uint(slotIdx*2) & 63 // prove to compiler shift < 64; elided on both arm64 and amd64
	mask := uint64(0x3) << shift
	val := uint64(state&0x3) << shift
	raw := (uint64(h) &^ mask) | val
	raw += 1 << versionShift
	return Header(raw)
}

func atomicLoadHeader(addr *Header) Header { return Header(atomic.LoadUint64((*uint64)(addr))) }

func atomicCASHeader(addr *Header, old, new Header) bool {
	return atomic.CompareAndSwapUint64((*uint64)(addr), uint64(old), uint64(new))
}

func atomicAndHeader(addr *Header, mask uint64) {
	atomic.AndUint64((*uint64)(unsafe.Pointer(addr)), mask)
}

func atomicLoadSlotVal[K Key, V any](slot *Slot[K, V]) *Entry[K, V] {
	return (*Entry[K, V])(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&slot.Val))))
}

func atomicStoreSlotVal[K Key, V any](slot *Slot[K, V], entry *Entry[K, V]) {
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&slot.Val)), unsafe.Pointer(entry))
}

// validMask3 returns a spread bitmask indicating which of the first 3 slots are valid (state == 2).
// Bits are at positions 0, 2, 4 (not compacted to 0, 1, 2). Use bits.TrailingZeros32(mask) >> 1
// to convert to a slot index.
//
// Invariant: slot state 3 (0b11) never occurs in the protocol, so the high bit
// of each 2-bit state field alone identifies Valid (state 2 = 0b10).
func (h Header) validMask3() uint32 {
	return (uint32(h) >> 1) & 0x15
}

// validMask4 returns a spread bitmask indicating which of 4 slots starting at base are valid.
// Bits are at positions 0, 2, 4, 6 (not compacted to 0, 1, 2, 3). Use bits.TrailingZeros32(mask) >> 1
// to convert to a slot index relative to base.
//
// Invariant: slot state 3 (0b11) never occurs in the protocol.
func (h Header) validMask4(base uint) uint32 {
	return (uint32(h) >> ((base << 1) + 1)) & 0x55
}

// invalidMask3 returns a spread bitmask indicating which of the first 3 slots are invalid (state == 0).
// Bits are at positions 0, 2, 4 (not compacted to 0, 1, 2). Use bits.TrailingZeros32(mask) >> 1
// to convert to a slot index.
func (h Header) invalidMask3() uint32 {
	s := uint32(h) & 0x3F
	return ^(s | (s >> 1)) & 0x15
}

// invalidMask4 returns a spread bitmask indicating which of 4 slots starting at base are invalid.
// Bits are at positions 0, 2, 4, 6 (not compacted). Use bits.TrailingZeros32(mask) >> 1
// to convert to a slot index relative to base.
func (h Header) invalidMask4(base uint) uint32 {
	s := (uint32(h) >> (base << 1)) & 0xFF
	return ^(s | (s >> 1)) & 0x55
}

func b2u32(b bool) uint32 {
	if b {
		return 1
	}
	return 0
}
