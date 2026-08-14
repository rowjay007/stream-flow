package raft

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

// walStorage is a simple append-only WAL implementation storing entries
// in a single file named wal.log and snapshots as snapshot.<id>.snap
type walStorage struct {
	dir string
}

// NewWALStorage creates a WAL storage rooted at dir.
func NewWALStorage(dir string) (Storage, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &walStorage{dir: dir}, nil
}

func (w *walStorage) walPath() string { return filepath.Join(w.dir, "wal.log") }

func (w *walStorage) SaveSnapshot(r io.Reader) (string, error) {
	id := fmt.Sprintf("%d", timeNowUnixNano())
	path := filepath.Join(w.dir, "snapshot."+id+".snap")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return "", err
	}
	defer f.Close()
	if _, err := io.Copy(f, r); err != nil {
		return "", err
	}
	return id, nil
}

func (w *walStorage) LoadSnapshot() (io.ReadCloser, error) {
	// Find latest snapshot by lexicographic order of snapshot.*.snap
	entries, err := os.ReadDir(w.dir)
	if err != nil {
		return nil, err
	}
	var latest string
	for _, e := range entries {
		name := e.Name()
		if !e.IsDir() && len(name) > 9 && name[:9] == "snapshot." {
			if latest == "" || name > latest {
				latest = name
			}
		}
	}
	if latest == "" {
		return nil, os.ErrNotExist
	}
	return os.Open(filepath.Join(w.dir, latest))
}

func (w *walStorage) AppendWAL(entries []byte) error {
	f, err := os.OpenFile(w.walPath(), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	// Write length-prefixed data (uint32 big endian)
	// For simplicity, write raw bytes preceded by 4-byte length.
	l := uint32(len(entries))
	hdr := []byte{byte(l >> 24), byte(l >> 16), byte(l >> 8), byte(l)}
	if _, err := f.Write(hdr); err != nil {
		return err
	}
	if _, err := f.Write(entries); err != nil {
		return err
	}
	return nil
}

func (w *walStorage) ReadWAL(fromIndex uint64) ([]byte, error) {
	data, err := os.ReadFile(w.walPath())
	if err != nil {
		return nil, err
	}
	if fromIndex >= uint64(len(data)) {
		return nil, nil
	}
	return data[fromIndex:], nil
}

// timeNowUnixNano is a testable wrapper for time.Now().UnixNano().
func timeNowUnixNano() int64 { return time.Now().UnixNano() }
