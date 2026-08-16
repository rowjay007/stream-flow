package raft

import (
	"go.etcd.io/etcd/raft/v3/raftpb"
	"path/filepath"
	"testing"
)

func TestWALCompactionKeepsLaterEntries(t *testing.T) {
	dir := t.TempDir()
	ws, err := NewWALStorage(filepath.Join(dir, "wal"), nil)
	if err != nil {
		t.Fatalf("new wal: %v", err)
	}
	// create entries with Index 1..5
	var ents []raftpb.Entry
	for i := uint64(1); i <= 5; i++ {
		ents = append(ents, raftpb.Entry{Index: i, Term: 1})
	}
	b, err := marshalEntries(ents)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := ws.AppendWAL(b); err != nil {
		t.Fatalf("append wal: %v", err)
	}

	if err := ws.Compact(3); err != nil {
		t.Fatalf("compact: %v", err)
	}

	data, err := ws.ReadWAL(0)
	if err != nil {
		t.Fatalf("read wal: %v", err)
	}
	out, err := unmarshalEntries(data)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("expected 2 entries after compaction, got %d", len(out))
	}
	if out[0].Index != 4 || out[1].Index != 5 {
		t.Fatalf("unexpected indices: %+v", out)
	}
}
