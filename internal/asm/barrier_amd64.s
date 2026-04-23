//go:build amd64

#include "textflag.h"

// MemoryBarrier issues a full memory barrier (MFENCE)
// This ensures all prior loads and stores are globally visible
// before any subsequent loads or stores.
TEXT ·MemoryBarrier(SB), NOSPLIT|NOFRAME, $0-0
	MFENCE
	RET
