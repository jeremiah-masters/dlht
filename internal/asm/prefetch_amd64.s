//go:build amd64

#include "textflag.h"

TEXT ·Prefetch(SB), NOSPLIT|NOFRAME, $0-8
	MOVQ addr+0(FP), AX
	PREFETCHT0 (AX)
	RET
