package sdk

import "context"

// Client is a tiny in-process SDK client used for local benchmarks and tests.
type Client struct {
	brokerAddr string
}

func NewClient(brokerAddr string) *Client {
	return &Client{brokerAddr: brokerAddr}
}

func (c *Client) Produce(ctx context.Context, topic string, key, value []byte) error {
	_ = c.brokerAddr
	_ = topic
	_ = key
	_ = value
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

func (c *Client) Consume(ctx context.Context, topic string) (<-chan []byte, error) {
	_ = c.brokerAddr
	_ = topic
	ch := make(chan []byte)
	go func() {
		defer close(ch)
		<-ctx.Done()
	}()
	return ch, nil
}
