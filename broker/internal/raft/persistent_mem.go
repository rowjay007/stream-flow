package raft

import (
	raftlib "go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

type persistentMemory struct {
	*raftlib.MemoryStorage
	store Storage
}

func newPersistentMemory(ms *raftlib.MemoryStorage, s Storage) *persistentMemory {
	return &persistentMemory{MemoryStorage: ms, store: s}
}

func (p *persistentMemory) Append(ents []raftpb.Entry) error {
	if len(ents) == 0 {
		return nil
	}

	if b, err := marshalEntries(ents); err == nil {
		if err := p.store.AppendWAL(b); err != nil {
			return err
		}
	} else {
		return err
	}
	return p.MemoryStorage.Append(ents)
}
