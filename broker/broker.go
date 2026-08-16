package broker

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

var ErrDraining = errors.New("broker is draining")

type Broker struct {
	dir          string
	mu           sync.RWMutex
	topics       map[string]*Topic
	drainingTill time.Time
	producerSeq  map[string]int64
	dedupCache   map[string]Record
	txState      map[string]*Transaction
	coordinator  *ConsumerGroupCoordinator
}

func NewBroker(dir string) (*Broker, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &Broker{
		dir:         dir,
		topics:      make(map[string]*Topic),
		producerSeq: make(map[string]int64),
		dedupCache:  make(map[string]Record),
		txState:     make(map[string]*Transaction),
		coordinator: NewConsumerGroupCoordinator(),
	}, nil
}

func (b *Broker) CreateTopic(name string) (*Topic, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.topics[name]; exists {
		return b.topics[name], nil
	}

	topicDir := filepath.Join(b.dir, name)
	if err := os.MkdirAll(topicDir, 0o755); err != nil {
		return nil, fmt.Errorf("create topic dir: %w", err)
	}

	topic := &Topic{
		Name:      name,
		Partition: 0,
		Dir:       topicDir,
		Segments:  make([]*Segment, 0, 4),
	}
	seg, err := newSegment(filepath.Join(topicDir, "000000"), 0)
	if err != nil {
		return nil, err
	}
	topic.Segments = append(topic.Segments, seg)
	b.topics[name] = topic
	return topic, nil
}

func (b *Broker) ListTopics() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()

	names := make([]string, 0, len(b.topics))
	for name := range b.topics {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (b *Broker) Produce(topicName string, key, value []byte, headers map[string]string) (Record, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.isDrainingLocked() {
		return Record{}, ErrDraining
	}

	topic, ok := b.topics[topicName]
	if !ok {
		return Record{}, fmt.Errorf("topic %q not found", topicName)
	}

	record := Record{
		Key:       append([]byte(nil), key...),
		Value:     append([]byte(nil), value...),
		Headers:   cloneHeaders(headers),
		Timestamp: time.Now().UTC(),
		Offset:    topic.NextOffset,
	}

	seg := topic.Segments[len(topic.Segments)-1]
	offset, err := seg.Append(record)
	if err != nil {
		return Record{}, err
	}
	record.Offset = offset
	topic.NextOffset = offset + 1
	return record, nil
}

func (b *Broker) Drain(d time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.drainingTill = time.Now().Add(d)
}

func (b *Broker) IsDraining() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.isDrainingLocked()
}

func (b *Broker) isDrainingLocked() bool {
	if b.drainingTill.IsZero() {
		return false
	}
	if time.Now().After(b.drainingTill) {
		return false
	}
	return true
}

func (b *Broker) Consume(topicName string, fromOffset int64, max int) ([]Record, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	topic, ok := b.topics[topicName]
	if !ok {
		return nil, fmt.Errorf("topic %q not found", topicName)
	}
	if max <= 0 {
		max = 100
	}

	result := make([]Record, 0, max)
	for _, seg := range topic.Segments {
		recs, err := seg.ReadRange(fromOffset, max-len(result))
		if err != nil {
			return nil, err
		}
		result = append(result, recs...)
		if len(result) >= max {
			break
		}
	}
	return result, nil
}

func (b *Broker) Fetch(topicName string, fromOffset int64, maxBytes int64) ([]Record, error) {
	if maxBytes <= 0 {
		maxBytes = 1024 * 1024
	}
	var acc []Record
	for offset := fromOffset; ; offset++ {
		recs, err := b.Consume(topicName, offset, 1)
		if err != nil {
			return acc, err
		}
		if len(recs) == 0 {
			break
		}
		acc = append(acc, recs[0])
		if int64(len(acc)) >= maxBytes {
			break
		}
	}
	return acc, nil
}

func (b *Broker) FetchRaw(topicName string, offset int64) (*os.File, int64, int64, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	topic, ok := b.topics[topicName]
	if !ok {
		return nil, 0, 0, fmt.Errorf("topic %q not found", topicName)
	}

	for _, seg := range topic.Segments {
		seg.mu.Lock()
		pos, exists := seg.offsets[offset]
		if !exists {
			seg.mu.Unlock()
			continue
		}

		var sizeBuf [4]byte
		if _, err := seg.LogFile.ReadAt(sizeBuf[:], pos); err != nil {
			seg.mu.Unlock()
			return nil, 0, 0, fmt.Errorf("read payload length: %w", err)
		}
		length := int64(binary.BigEndian.Uint32(sizeBuf[:]))
		seg.mu.Unlock()
		return seg.LogFile, pos + 4, length, nil
	}

	return nil, 0, 0, fmt.Errorf("offset %d not found", offset)
}

func (b *Broker) CommitOffset(topicName, groupID string, offset int64) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	groupDir := filepath.Join(b.dir, "groups", groupID)
	if err := os.MkdirAll(groupDir, 0o755); err != nil {
		return fmt.Errorf("create group dir: %w", err)
	}
	path := filepath.Join(groupDir, topicName+".offset")
	return os.WriteFile(path, []byte(fmt.Sprintf("%d", offset)), 0o644)
}

func (b *Broker) LoadOffset(topicName, groupID string) (int64, error) {
	path := filepath.Join(b.dir, "groups", groupID, topicName+".offset")
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	var off int64
	_, err = fmt.Sscanf(string(data), "%d", &off)
	if err != nil {
		return 0, err
	}
	return off, nil
}

func cloneHeaders(headers map[string]string) map[string]string {
	if len(headers) == 0 {
		return nil
	}
	out := make(map[string]string, len(headers))
	for k, v := range headers {
		out[k] = v
	}
	return out
}

func init() {
	_ = binary.BigEndian
	_ = time.Now
}
