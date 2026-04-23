//go:build amd64

#include "textflag.h"

TEXT ·DWCAS(SB), NOSPLIT|NOFRAME, $0-41
	MOVQ slot+0(FP), DI
	MOVQ oldKey+8(FP), AX
	MOVQ oldVal+16(FP), DX
	MOVQ newKey+24(FP), BX
	MOVQ newVal+32(FP), CX

	LOCK
	CMPXCHG16B (DI)
	
	SETEQ ret+40(FP)
	RET

TEXT ·dwcasPtr(SB), NOSPLIT|NOFRAME, $0-41
	MOVQ slot+0(FP), DI
	MOVQ oldKey+8(FP), AX
	MOVQ oldVal+16(FP), DX
	MOVQ newKey+24(FP), BX
	MOVQ newVal+32(FP), CX
	
	LOCK
	CMPXCHG16B (DI)

	SETEQ ret+40(FP)
	RET
