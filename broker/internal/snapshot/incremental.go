package snapshot

import (
	"io"
	"log"
)

// This file contains a small placeholder for incremental/delta snapshot
// support. The production implementation should record deltas between
// successive snapshots and store a chain of snapshot manifests.

// CreateDelta reads a full snapshot and returns an io.ReadCloser with the
// delta payload. Here we simply forward the original snapshot as-is.
func CreateDelta(r io.Reader) (io.ReadCloser, error) {
	log.Printf("CreateDelta: placeholder returning full snapshot")
	// In a real implementation, build a delta stream.
	pr, pw := io.Pipe()
	go func() {
		_, err := io.Copy(pw, r)
		_ = pw.CloseWithError(err)
	}()
	return pr, nil
}
