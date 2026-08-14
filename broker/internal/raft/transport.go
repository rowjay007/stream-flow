package raft

import (
	"context"
)

// Transport abstracts the RPC transport used to exchange Raft messages
// between peers. We'll implement a gRPC-based transport later. For now
// this is a lightweight interface used by the raft node stub.
type Transport interface {
	Send(ctx context.Context, to string, payload []byte) error
	Close() error
}

// NewInMemoryTransport returns a no-op transport for local testing.
func NewInMemoryTransport() Transport { return &noopTransport{} }

type noopTransport struct{}

func (n *noopTransport) Send(ctx context.Context, to string, payload []byte) error { return nil }
func (n *noopTransport) Close() error                                              { return nil }
