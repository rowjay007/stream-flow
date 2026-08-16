package region

import internalregion "streamflow/broker/internal/region"

// Gateway re-exports the prototype region gateway from the internal package so
// top-level commands can construct it without violating Go internal package rules.
type Gateway = internalregion.Gateway

func NewGateway(addr string) *Gateway {
	return internalregion.NewGateway(addr)
}
