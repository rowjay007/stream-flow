package raft

import (
	"context"
)

type Transport interface {
	Send(ctx context.Context, to string, payload []byte) error
	Close() error
}

func NewInMemoryTransport() Transport { return &noopTransport{} }

type noopTransport struct{}

func (n *noopTransport) Send(ctx context.Context, to string, payload []byte) error { return nil }
func (n *noopTransport) Close() error                                              { return nil }
