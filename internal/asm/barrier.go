package asm

// MemoryBarrier issues a full memory barrier ensuring all prior
// loads and stores are globally visible before any subsequent
// loads or stores. This is implemented as MFENCE on AMD64 and
// DMB ISH on ARM64.
//
//go:nosplit
//go:noescape
func MemoryBarrier()
