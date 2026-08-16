package broker

import (
	"errors"
	"fmt"
)

var ErrOutOfOrderSequence = errors.New("out-of-order producer sequence")

// ProduceIdempotent enforces monotonic producer sequences per topic+producer.
// Duplicate sequence numbers return the cached record and duplicate=true.
func (b *Broker) ProduceIdempotent(topicName string, key, value []byte, headers map[string]string, producerID string, sequence int64) (Record, bool, error) {
	if producerID == "" {
		return Record{}, false, fmt.Errorf("producerID is required")
	}

	seqKey := topicName + "|" + producerID
	dedupKey := seqKey + "|" + fmt.Sprintf("%d", sequence)

	b.mu.Lock()
	if cached, ok := b.dedupCache[dedupKey]; ok {
		b.mu.Unlock()
		return cached, true, nil
	}
	last, hasLast := b.producerSeq[seqKey]
	if hasLast && sequence < last {
		b.mu.Unlock()
		return Record{}, false, ErrOutOfOrderSequence
	}
	b.mu.Unlock()

	rec, err := b.Produce(topicName, key, value, headers)
	if err != nil {
		return Record{}, false, err
	}

	b.mu.Lock()
	if sequence > b.producerSeq[seqKey] {
		b.producerSeq[seqKey] = sequence
	}
	b.dedupCache[dedupKey] = rec
	b.mu.Unlock()
	return rec, false, nil
}
