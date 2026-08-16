package region

import (
	"context"
	"log"
)

type Gateway struct {
	Addr string
}

func NewGateway(addr string) *Gateway {
	return &Gateway{Addr: addr}
}

func (g *Gateway) Forward(ctx context.Context, remote string, payload []byte) error {
	log.Printf("region gateway %s forward to %s (len=%d)", g.Addr, remote, len(payload))
	return nil
}
