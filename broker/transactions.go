package broker

import (
	"errors"
	"fmt"
	"time"
)

var (
	ErrTxNotFound = errors.New("transaction not found")
	ErrTxClosed   = errors.New("transaction is already closed")
)

type TxRecord struct {
	Topic   string
	Key     []byte
	Value   []byte
	Headers map[string]string
}

type Transaction struct {
	ID         string
	ProducerID string
	Epoch      int64
	Status     string
	CreatedAt  time.Time
	Records    []TxRecord
}

func (b *Broker) BeginTransaction(producerID string, epoch int64) (string, error) {
	if producerID == "" {
		return "", fmt.Errorf("producerID is required")
	}
	txID := fmt.Sprintf("%s-%d-%d", producerID, epoch, time.Now().UnixNano())
	b.mu.Lock()
	defer b.mu.Unlock()
	b.txState[txID] = &Transaction{
		ID:         txID,
		ProducerID: producerID,
		Epoch:      epoch,
		Status:     "open",
		CreatedAt:  time.Now().UTC(),
		Records:    make([]TxRecord, 0, 16),
	}
	return txID, nil
}

func (b *Broker) TxProduce(txID, topic string, key, value []byte, headers map[string]string) error {
	if topic == "" {
		return fmt.Errorf("topic is required")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	tx, ok := b.txState[txID]
	if !ok {
		return ErrTxNotFound
	}
	if tx.Status != "open" {
		return ErrTxClosed
	}
	tx.Records = append(tx.Records, TxRecord{
		Topic:   topic,
		Key:     append([]byte(nil), key...),
		Value:   append([]byte(nil), value...),
		Headers: cloneHeaders(headers),
	})
	return nil
}

func (b *Broker) CommitTransaction(txID string) (int, error) {
	b.mu.Lock()
	tx, ok := b.txState[txID]
	if !ok {
		b.mu.Unlock()
		return 0, ErrTxNotFound
	}
	if tx.Status != "open" {
		b.mu.Unlock()
		return 0, ErrTxClosed
	}
	tx.Status = "committing"
	records := append([]TxRecord(nil), tx.Records...)
	b.mu.Unlock()

	committed := 0
	for _, r := range records {
		if _, err := b.CreateTopic(r.Topic); err != nil {
			return committed, err
		}
		if _, err := b.Produce(r.Topic, r.Key, r.Value, r.Headers); err != nil {
			return committed, err
		}
		committed++
	}

	b.mu.Lock()
	tx.Status = "committed"
	b.mu.Unlock()
	return committed, nil
}

func (b *Broker) AbortTransaction(txID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	tx, ok := b.txState[txID]
	if !ok {
		return ErrTxNotFound
	}
	if tx.Status != "open" {
		return ErrTxClosed
	}
	tx.Status = "aborted"
	tx.Records = nil
	return nil
}

func (b *Broker) ConsumeReadCommitted(topicName string, fromOffset int64, max int) ([]Record, error) {
	return b.Consume(topicName, fromOffset, max)
}
