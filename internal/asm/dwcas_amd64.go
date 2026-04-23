//go:build amd64

package asm

import (
	"structs"
	"unsafe"
)

//go:linkname writeBarrier runtime.writeBarrier
var writeBarrier struct {
	_ structs.HostLayout

	enabled bool
	pad     [3]byte
	alignme uint64
}

//go:nosplit
//go:noescape
func DWCAS(slot unsafe.Pointer, oldKey, oldVal, newKey, newVal uint64) bool

//go:nosplit
func dwcasPtr(slot unsafe.Pointer, oldKey uint64, oldVal unsafe.Pointer, newKey uint64, newVal unsafe.Pointer) bool

//go:nosplit
func DWCASPtr(slot unsafe.Pointer, oldKey uint64, oldVal unsafe.Pointer, newKey uint64, newVal unsafe.Pointer) bool {
	if writeBarrier.enabled {
		runtimeatomicwb((*unsafe.Pointer)(unsafe.Add(slot, 8)), newVal)
	}
	return dwcasPtr(slot, oldKey, oldVal, newKey, newVal)
}

//go:linkname runtimeatomicwb runtime.atomicwb
func runtimeatomicwb(ptr *unsafe.Pointer, new unsafe.Pointer)
