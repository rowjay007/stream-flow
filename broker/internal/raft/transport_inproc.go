package raft

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/gogo/protobuf/proto"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

type InProcTransport struct {
	mu     sync.RWMutex
	peers  map[string]chan []byte
	buf    int
	closed bool
}

func NewInProcTransport(buf int) *InProcTransport {
	return &InProcTransport{peers: make(map[string]chan []byte), buf: buf}
}

func (t *InProcTransport) Register(addr string) (<-chan []byte, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return nil, errors.New("transport closed")
	}
	if _, ok := t.peers[addr]; ok {
		return nil, fmt.Errorf("address already registered: %s", addr)
	}
	ch := make(chan []byte, t.buf)
	t.peers[addr] = ch
	return ch, nil
}

func (t *InProcTransport) Unregister(addr string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if ch, ok := t.peers[addr]; ok {
		close(ch)
		delete(t.peers, addr)
	}
}

func (t *InProcTransport) Send(ctx context.Context, to string, payload []byte) error {
	t.mu.RLock()
	ch, ok := t.peers[to]
	t.mu.RUnlock()
	if !ok {
		return fmt.Errorf("peer not found: %s", to)
	}

	select {
	case ch <- append([]byte(nil), payload...):
		return nil
	default:
		return fmt.Errorf("peer channel full: %s", to)
	}
}

func (t *InProcTransport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return nil
	}
	for k, ch := range t.peers {
		close(ch)
		delete(t.peers, k)
	}
	t.closed = true
	return nil
}

func marshalMessage(m raftpb.Message) ([]byte, error) {
	return proto.Marshal(&m)
}

func unmarshalMessage(b []byte) (raftpb.Message, error) {
	var m raftpb.Message
	if err := proto.Unmarshal(b, &m); err != nil {
		return m, err
	}
	return m, nil
}
