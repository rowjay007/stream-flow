package raft

import (
	"io"
)

// Storage represents the persistent storage interface required by the
// etcd/raft library. In production this should provide a WAL and snapshot
// backed by the broker segment files. For Phase 2 we provide a minimal stub
// that satisfies the types and will be implemented later.
type Storage interface {
	// SaveSnapshot persists the provided snapshot data and returns an ID.
	SaveSnapshot(r io.Reader) (snapshotID string, err error)

	// LoadSnapshot returns a reader for the latest snapshot.
	LoadSnapshot() (io.ReadCloser, error)

	// AppendWAL appends entries to the write-ahead log.
	AppendWAL(entries []byte) error

	// ReadWAL reads WAL entries since the provided index.
	ReadWAL(fromIndex uint64) ([]byte, error)
	// Compact truncates WAL entries up to the provided index (inclusive).
	Compact(index uint64) error
}

// NewInMemoryStorage returns a trivial in-memory storage used for tests and
// initial integration.
func NewInMemoryStorage() Storage {
	return &memStorage{}
}

type memStorage struct{}

func (m *memStorage) SaveSnapshot(r io.Reader) (string, error) { return "", nil }
func (m *memStorage) LoadSnapshot() (io.ReadCloser, error)     { return nil, nil }
func (m *memStorage) AppendWAL(entries []byte) error           { return nil }
func (m *memStorage) ReadWAL(fromIndex uint64) ([]byte, error) {
	return nil, nil
}
func (m *memStorage) Compact(index uint64) error { _ = index; return nil }
