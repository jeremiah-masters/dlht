package cpu

import _ "unsafe"

//go:nosplit
//go:linkname Yield sync.runtime_doSpin
func Yield()
