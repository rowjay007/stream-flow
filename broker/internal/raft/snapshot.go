package raft

import "io"

type Snapshot struct {
	ID   string
	Data io.ReadCloser
}
