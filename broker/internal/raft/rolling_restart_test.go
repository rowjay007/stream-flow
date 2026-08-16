package raft

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestSingleNodeRestart(t *testing.T) {
	dir := t.TempDir()
	store, err := NewWALStorage(filepath.Join(dir, "wal"), nil)
	if err != nil {
		t.Fatalf("new wal: %v", err)
	}
	tr := NewInProcTransport(256)
	node := NewNode(1, store, tr)
	if err := node.Start(context.Background()); err != nil {
		t.Fatalf("start node: %v", err)
	}
	if err := node.Propose(context.Background(), []byte("hello")); err != nil {
		t.Fatalf("propose: %v", err)
	}

	time.Sleep(200 * time.Millisecond)
	node.Stop()

	store2, err := NewWALStorage(filepath.Join(dir, "wal"), nil)
	if err != nil {
		t.Fatalf("new wal2: %v", err)
	}
	tr2 := NewInProcTransport(256)
	node2 := NewNode(1, store2, tr2)
	if err := node2.Start(context.Background()); err != nil {
		t.Fatalf("start node2: %v", err)
	}
	defer node2.Stop()
	data, err := node2.store.ReadWAL(0)
	if err != nil {
		t.Fatalf("read wal after restart: %v", err)
	}
	if len(data) == 0 {
		t.Fatalf("expected wal data after restart")
	}
}

func TestClusterNodesStartStop(t *testing.T) {
	nodes := make([]*Node, 3)
	for i := 0; i < 3; i++ {
		dir := t.TempDir()
		store, err := NewWALStorage(filepath.Join(dir, "wal"), nil)
		if err != nil {
			t.Fatalf("wal store: %v", err)
		}
		nodes[i] = NewNode(uint64(i+1), store, NewInProcTransport(256))
		if err := nodes[i].Start(context.Background()); err != nil {
			t.Fatalf("start node: %v", err)
		}
	}

	time.Sleep(300 * time.Millisecond)
	for i := 0; i < 3; i++ {
		if err := nodes[i].Stop(); err != nil {
			t.Fatalf("stop node %d: %v", i+1, err)
		}
	}
}
