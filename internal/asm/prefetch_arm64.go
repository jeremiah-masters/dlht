//go:build arm64

package asm

import "unsafe"

//go:nosplit
//go:noescape
func Prefetch(addr unsafe.Pointer)
