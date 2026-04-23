//go:build arm64

#include "textflag.h"

TEXT ·Prefetch(SB), NOSPLIT|NOFRAME, $0-8
	MOVD addr+0(FP), R0
    PRFM (R0), PLDL1KEEP
	RET
