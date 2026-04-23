//go:build arm64

package cpu

const (
	// Copied from Go src/internal/cpu/cpu_arm64.go
	// Apple M series has 128 byte cache line size
	CacheLineSize = 128
)
