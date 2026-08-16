package connectors

// HTTPConnector is a built-in connector surface for HTTP pull/push streams.
type HTTPConnector struct {
	Endpoint string
	Method   string
}

func (c *HTTPConnector) Name() string { return "http" }
