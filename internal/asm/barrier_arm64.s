//go:build arm64

#include "textflag.h"

// MemoryBarrier issues a full memory barrier (DMB ISH)
// This ensures all prior loads and stores are globally visible
// before any subsequent loads or stores.
TEXT ·MemoryBarrier(SB), NOSPLIT|NOFRAME, $0-0
	DMB $0xb // ISH - Inner Shareable, full barrier
	RET
