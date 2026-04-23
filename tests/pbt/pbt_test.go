package pbt

import (
	"os"
	"runtime"
	"testing"
)

func TestMain(m *testing.M) {
	runtime.GOMAXPROCS(1)
	os.Exit(m.Run())
}
