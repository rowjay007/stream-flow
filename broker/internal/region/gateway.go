package region

import (
	"context"
	"log"
)

// Gateway is a very small prototype for forwarding raft messages across
// regions. It is intentionally minimal: real implementation should handle
// secure persistent streams, retries, and backpressure.
type Gateway struct {
	// address or identifier for this gateway
	Addr string
}

func NewGateway(addr string) *Gateway {
	return &Gateway{Addr: addr}
}

// Forward is a placeholder that would forward a payload to a remote region.
func (g *Gateway) Forward(ctx context.Context, remote string, payload []byte) error {
	log.Printf("region gateway %s forward to %s (len=%d)", g.Addr, remote, len(payload))
	return nil
}
