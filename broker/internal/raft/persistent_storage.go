package raft

import (
	"sync"

	raftlib "go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

// walAdapter adapts our simple WAL Storage to the etcd/raft Storage interface
// used by the raft state machine. It is read-only: writing still happens via
// AppendWAL called from the Node Ready loop.
type walAdapter struct {
	store Storage
	mu    sync.Mutex
	ents  []raftpb.Entry
}

// NewEtcdStorageAdapter builds an adapter by reading the WAL and unmarshalling
// entries into memory for the raft library to consume via the Storage API.
func NewEtcdStorageAdapter(s Storage) (raftlib.Storage, error) {
	a := &walAdapter{store: s}
	if data, err := s.ReadWAL(0); err == nil && len(data) > 0 {
		if ents, err := unmarshalEntries(data); err == nil {
			a.ents = ents
		}
	}
	return a, nil
}

func (w *walAdapter) InitialState() (raftpb.HardState, raftpb.ConfState, error) {
	return raftpb.HardState{}, raftpb.ConfState{}, nil
}

func (w *walAdapter) Entries(lo, hi, maxSize uint64) ([]raftpb.Entry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	var out []raftpb.Entry
	for _, e := range w.ents {
		if uint64(e.Index) >= lo && uint64(e.Index) < hi {
			out = append(out, e)
			if maxSize > 0 && uint64(len(out)) >= maxSize {
				break
			}
		}
	}
	if len(out) == 0 {
		return nil, raftlib.ErrUnavailable
	}
	return out, nil
}

func (w *walAdapter) Term(i uint64) (uint64, error) {
	if i == 0 {
		return 0, nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, e := range w.ents {
		if uint64(e.Index) == i {
			return uint64(e.Term), nil
		}
	}
	return 0, raftlib.ErrUnavailable
}

func (w *walAdapter) LastIndex() (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.ents) == 0 {
		return 0, nil
	}
	return uint64(w.ents[len(w.ents)-1].Index), nil
}

func (w *walAdapter) FirstIndex() (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.ents) == 0 {
		return 1, nil
	}
	return uint64(w.ents[0].Index), nil
}

func (w *walAdapter) Snapshot() (raftpb.Snapshot, error) {
	return raftpb.Snapshot{}, raftlib.ErrUnavailable
}
