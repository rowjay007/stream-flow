package processor

import "errors"

var ErrRocksDBNotEnabled = errors.New("rocksdb state store requires -tags rocksdb")

type StateStore interface {
	Put(key, value []byte) error
	Get(key []byte) ([]byte, error)
	Close() error
}

type InMemoryStateStore struct {
	m map[string][]byte
}

func NewInMemoryStateStore() *InMemoryStateStore {
	return &InMemoryStateStore{m: make(map[string][]byte)}
}

func (s *InMemoryStateStore) Put(key, value []byte) error {
	s.m[string(key)] = append([]byte(nil), value...)
	return nil
}

func (s *InMemoryStateStore) Get(key []byte) ([]byte, error) {
	v, ok := s.m[string(key)]
	if !ok {
		return nil, nil
	}
	return append([]byte(nil), v...), nil
}

func (s *InMemoryStateStore) Close() error { return nil }

func NewRocksDBStateStore(_ string) (StateStore, error) {
	return nil, ErrRocksDBNotEnabled
}
