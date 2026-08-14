package raft

import (
	raftlib "go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

// persistentMemory wraps a MemoryStorage and persists appended entries to
// the configured WAL-backed Storage. It embeds the memory storage so it
// implements raftlib.Storage methods while intercepting Append calls.
type persistentMemory struct {
	*raftlib.MemoryStorage
	store Storage
}

func newPersistentMemory(ms *raftlib.MemoryStorage, s Storage) *persistentMemory {
	return &persistentMemory{MemoryStorage: ms, store: s}
}

// Append persists the entries to WAL first, then appends to the in-memory
// storage so the raft node can proceed.
func (p *persistentMemory) Append(ents []raftpb.Entry) error {
	if len(ents) == 0 {
		return nil
	}
	// Marshal entries into bytes using the helper marshalEntries.
	if b, err := marshalEntries(ents); err == nil {
		if err := p.store.AppendWAL(b); err != nil {
			return err
		}
	} else {
		return err
	}
	return p.MemoryStorage.Append(ents)
}
