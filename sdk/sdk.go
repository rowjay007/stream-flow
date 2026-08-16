package sdk

import (
	"fmt"
	"os"
	"path/filepath"

	"streamflow/broker"
)

type Producer struct {
	broker *broker.Broker
}

func NewProducer(dir string) (*Producer, error) {
	b, err := broker.NewBroker(dir)
	if err != nil {
		return nil, err
	}
	return &Producer{broker: b}, nil
}

func (p *Producer) CreateTopic(name string) error {
	_, err := p.broker.CreateTopic(name)
	return err
}

func (p *Producer) Send(topic string, key, value []byte, headers map[string]string) (broker.Record, error) {
	return p.broker.Produce(topic, key, value, headers)
}

type Consumer struct {
	broker *broker.Broker
}

func NewConsumer(dir string) (*Consumer, error) {
	b, err := broker.NewBroker(dir)
	if err != nil {
		return nil, err
	}
	return &Consumer{broker: b}, nil
}

func (c *Consumer) Poll(topic string, offset int64, max int) ([]broker.Record, error) {
	return c.broker.Consume(topic, offset, max)
}

func (c *Consumer) Commit(topic, group string, offset int64) error {
	return c.broker.CommitOffset(topic, group, offset)
}

func Example() {
	_ = fmt.Sprintf
	_ = filepath.Join
	_ = os.TempDir
}
