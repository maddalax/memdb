package util

import (
	"fmt"
	"time"
)

func TracePerf(name string, cb func()) {
	now := time.Now()
	cb()
	took := time.Since(now)
	fmt.Printf("%s took %d ms\n", name, took.Milliseconds())
}
