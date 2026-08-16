package raft

import (
	"bytes"
	"io"
	"sync/atomic"
	"testing"
	"time"
)

type checkpointStoreStub struct{ n atomic.Int32 }

func (c *checkpointStoreStub) SaveSnapshot(r io.Reader) (string, error) {
	_, _ = io.ReadAll(r)
	c.n.Add(1)
	return "id", nil
}
func (c *checkpointStoreStub) LoadSnapshot() (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewBuffer(nil)), nil
}
func (c *checkpointStoreStub) AppendWAL(_ []byte) error         { return nil }
func (c *checkpointStoreStub) ReadWAL(_ uint64) ([]byte, error) { return nil, nil }
func (c *checkpointStoreStub) Compact(_ uint64) error           { return nil }

func TestCheckpointScheduler(t *testing.T) {
	store := &checkpointStoreStub{}
	s := NewCheckpointScheduler()
	s.Interval = 10 * time.Millisecond
	go s.Run(store, func() io.Reader { return bytes.NewBufferString("snap") })
	time.Sleep(35 * time.Millisecond)
	s.Stop()
	if store.n.Load() < 2 {
		t.Fatalf("expected periodic checkpoints, got %d", store.n.Load())
	}
}
