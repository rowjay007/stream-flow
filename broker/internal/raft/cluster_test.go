package raft

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestNodeProposePersistsWAL is a deterministic smoke test for raft persistence.
// Multi-node elections are covered by dedicated transport/integration tests.
func TestNodeProposePersistsWAL(t *testing.T) {
	dir, err := os.MkdirTemp("", "raftnode")
	if err != nil {
		t.Fatalf("mktemp: %v", err)
	}
	defer os.RemoveAll(dir)

	store, err := NewWALStorage(filepath.Join(dir, "wal"), nil)
	if err != nil {
		t.Fatalf("wal store: %v", err)
	}

	n := NewNode(1, store, NewInProcTransport(256))
	if err := n.Start(context.Background()); err != nil {
		t.Fatalf("start node: %v", err)
	}
	defer n.Stop()

	// Allow initial election to settle for the single-node cluster.
	time.Sleep(200 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := n.Propose(ctx, []byte("hello")); err != nil {
		t.Fatalf("propose: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		data, err := store.ReadWAL(0)
		if err == nil && len(data) > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("wal did not persist in time")
		}
		time.Sleep(50 * time.Millisecond)
	}
}
