package broker

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// TenantBroker adds a thin namespacing layer on top of Broker for multi-tenancy.
type TenantBroker struct {
	b  *Broker
	mu sync.Mutex
}

func NewTenantBroker(b *Broker) *TenantBroker {
	return &TenantBroker{b: b}
}

func (t *TenantBroker) CreateTopicNS(namespace, name string) (*Topic, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	nsDir := filepath.Join(t.b.dir, namespace)
	if err := os.MkdirAll(nsDir, 0o755); err != nil {
		return nil, fmt.Errorf("create ns dir: %w", err)
	}
	// Delegate to existing topic creation but ensure topic dir under namespace
	topicDir := filepath.Join(nsDir, name)
	if err := os.MkdirAll(topicDir, 0o755); err != nil {
		return nil, fmt.Errorf("create topic dir: %w", err)
	}
	// Use Broker's internal helpers by duplicating logic for safety
	if _, exists := t.b.topics[name]; exists {
		return t.b.topics[name], nil
	}
	seg, err := newSegment(filepath.Join(topicDir, "000000"), 0)
	if err != nil {
		return nil, err
	}
	topic := &Topic{
		Name:      name,
		Partition: 0,
		Dir:       topicDir,
		Segments:  []*Segment{seg},
	}
	t.b.topics[name] = topic
	return topic, nil
}
