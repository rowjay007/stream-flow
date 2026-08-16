package raft

import "sync"

type ISRManager struct {
	mu       sync.RWMutex
	members  map[uint64]struct{}
	minISR   int
	leaderID uint64
}

func NewISRManager(minISR int) *ISRManager {
	if minISR <= 0 {
		minISR = 1
	}
	return &ISRManager{members: make(map[uint64]struct{}), minISR: minISR}
}

func (m *ISRManager) SetLeader(id uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.leaderID = id
	m.members[id] = struct{}{}
}

func (m *ISRManager) MarkInSync(id uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.members[id] = struct{}{}
}

func (m *ISRManager) MarkOutOfSync(id uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.members, id)
}

func (m *ISRManager) CanAcceptWrites() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.members) >= m.minISR
}

func (m *ISRManager) Members() []uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]uint64, 0, len(m.members))
	for id := range m.members {
		out = append(out, id)
	}
	return out
}
