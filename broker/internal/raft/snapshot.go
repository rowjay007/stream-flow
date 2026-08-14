package raft

import "io"

// Snapshot represents a Raft snapshot that can be sent to peers or persisted.
type Snapshot struct {
	ID   string
	Data io.ReadCloser
}
