package connectors

type HTTPConnector struct {
	Endpoint string
	Method   string
}

func (c *HTTPConnector) Name() string { return "http" }
