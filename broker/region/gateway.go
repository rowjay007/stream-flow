package region

import internalregion "streamflow/broker/internal/region"

type Gateway = internalregion.Gateway

func NewGateway(addr string) *Gateway {
	return internalregion.NewGateway(addr)
}
