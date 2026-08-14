package broker

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	raftint "streamflow/broker/internal/raft"
)

// Broker implements the single-node broker foundation described in Phase 1.
type Broker struct {
	dir      string
	mu       sync.RWMutex
	topics   map[string]*Topic
	raftNode *raftint.Node
}

func NewBroker(dir string) (*Broker, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	b := &Broker{dir: dir, topics: make(map[string]*Topic)}

	// Initialize WAL-backed storage for Raft and start a local node for Phase 2.
	raftDir := filepath.Join(dir, "raft")
	store, err := raftint.NewWALStorage(raftDir)
	if err == nil {
		tr := raftint.NewInMemoryTransport()
		node := raftint.NewNode(1, store, tr)
		if err := node.Start(context.Background()); err == nil {
			b.raftNode = node
		}
	}

	return b, nil
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

func (b *Broker) Produce(topicName string, key, value []byte, headers map[string]string) (Record, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

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

// FetchRaw returns an open file handle positioned at the payload start, the
// payload offset within the file, and the payload length for a single record
// referenced by topicName and offset. Caller must NOT close the returned
// *os.File (it is owned by the segment).
func (b *Broker) FetchRaw(topicName string, offset int64) (*os.File, int64, int64, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	topic, ok := b.topics[topicName]
	if !ok {
		return nil, 0, 0, fmt.Errorf("topic %q not found", topicName)
	}

	// Find the segment containing the offset.
	for _, seg := range topic.Segments {
		seg.mu.Lock()
		pos, ok := seg.offsets[offset]
		seg.mu.Unlock()
		if !ok {
			continue
		}
		// Read the length prefix.
		var sizeBuf [4]byte
		if _, err := seg.LogFile.ReadAt(sizeBuf[:], pos); err != nil {
			return nil, 0, 0, fmt.Errorf("read payload length: %w", err)
		}
		length := int64(binary.BigEndian.Uint32(sizeBuf[:]))
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
