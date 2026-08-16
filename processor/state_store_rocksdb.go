//go:build rocksdb

package processor

/*
#cgo LDFLAGS: -lrocksdb
*/
import "C"

import (
	"fmt"
	"os"
	"path/filepath"
)

// RocksDBStateStore is a build-tagged placeholder that validates directory wiring
// for RocksDB-backed state when compiled with -tags rocksdb.
type RocksDBStateStore struct {
	dir string
	kv  map[string][]byte
}

func NewRocksDBStateStore(dir string) (StateStore, error) {
	if dir == "" {
		return nil, fmt.Errorf("rocksdb dir is required")
	}
	if err := os.MkdirAll(filepath.Clean(dir), 0o755); err != nil {
		return nil, err
	}
	return &RocksDBStateStore{dir: dir, kv: make(map[string][]byte)}, nil
}

func (s *RocksDBStateStore) Put(key, value []byte) error {
	s.kv[string(key)] = append([]byte(nil), value...)
	return nil
}

func (s *RocksDBStateStore) Get(key []byte) ([]byte, error) {
	v := s.kv[string(key)]
	return append([]byte(nil), v...), nil
}

func (s *RocksDBStateStore) Close() error { return nil }
