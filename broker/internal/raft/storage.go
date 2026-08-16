package raft

import (
	"io"
)

type Storage interface {
	SaveSnapshot(r io.Reader) (snapshotID string, err error)

	LoadSnapshot() (io.ReadCloser, error)

	AppendWAL(entries []byte) error

	ReadWAL(fromIndex uint64) ([]byte, error)

	Compact(index uint64) error
}

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
