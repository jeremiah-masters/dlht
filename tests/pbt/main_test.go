package pbt

import (
	"os"
	"runtime"
	"testing"
)

// GOMAXPROCS=1 pins goroutine scheduling so rapid seeds reproduce more reliably
// across runs. Rapid cannot shrink schedules, but this removes a large source of
// noise from the concurrent property tests.
func TestMain(m *testing.M) {
	runtime.GOMAXPROCS(1)
	os.Exit(m.Run())
}
