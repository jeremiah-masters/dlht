//go:build amd64

package asm

import "unsafe"

//go:nosplit
//go:noescape
func Prefetch(addr unsafe.Pointer)
