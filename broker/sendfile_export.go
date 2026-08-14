package broker

import (
	"io"
	"os"
)

// SendFile is an exported wrapper around the platform-specific sendFile
// implementation. It is used by external packages (HTTP handler) to
// attempt zero-copy transfers when available.
func SendFile(w io.Writer, f *os.File, off int64, count int64) (int64, error) {
	return sendFile(w, f, off, count)
}
