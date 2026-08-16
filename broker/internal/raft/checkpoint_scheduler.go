package raft

import (
	"io"
	"time"
)

type CheckpointScheduler struct {
	Interval time.Duration
	StopCh   chan struct{}
}

func NewCheckpointScheduler() *CheckpointScheduler {
	return &CheckpointScheduler{Interval: 30 * time.Second, StopCh: make(chan struct{})}
}

func (s *CheckpointScheduler) Run(store Storage, makeSnapshot func() io.Reader) {
	if store == nil || makeSnapshot == nil {
		return
	}
	ticker := time.NewTicker(s.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-s.StopCh:
			return
		case <-ticker.C:
			_, _ = store.SaveSnapshot(makeSnapshot())
		}
	}
}

func (s *CheckpointScheduler) Stop() {
	select {
	case <-s.StopCh:
		return
	default:
		close(s.StopCh)
	}
}
