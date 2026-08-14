//go:build !linux
// +build !linux

package broker

import (
	"io"
	"os"
)

// sendFile on non-Linux platforms falls back to a safe copy implementation.
func sendFile(w io.Writer, f *os.File, off int64, count int64) (int64, error) {
	return sendFileFallback(w, f, off, count)
}
