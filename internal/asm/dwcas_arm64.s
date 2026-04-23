//go:build arm64

#include "textflag.h"

TEXT ·DWCAS(SB), NOSPLIT|NOFRAME, $0-41
	MOVD	slot+0(FP), R5
	MOVD	oldKey+8(FP), R0
	MOVD	oldVal+16(FP), R1
	MOVD	newKey+24(FP), R2
	MOVD	newVal+32(FP), R3
	MOVD	R0, R6
	MOVD	R1, R7
	CASPD	(R0, R1), (R5), (R2, R3)
	CMP     R0, R6
	CCMP	EQ, R1, R7, $0
	CSET	EQ, R0
	MOVB	R0, ret+40(FP)
	RET

TEXT ·dwcasPtr(SB), NOSPLIT|NOFRAME, $0-41
	MOVD	slot+0(FP), R5
	MOVD	oldKey+8(FP), R0
	MOVD	oldVal+16(FP), R1
	MOVD	newKey+24(FP), R2
	MOVD	newVal+32(FP), R3
	MOVD	R0, R6
	MOVD	R1, R7
	WORD    $0x4820fca2 // CASPAL (R0, R1), (R5), (R2, R3)
	CMP     R0, R6
	CCMP	EQ, R1, R7, $0
	CSET	EQ, R0
	MOVB	R0, ret+40(FP)
	RET
