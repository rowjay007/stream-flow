package raft

import (
	"fmt"
	"io"
	"io/ioutil"
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
	// Read all wal files (rotated and current) in lexicographic order
	entries, err := os.ReadDir(w.dir)
	if err != nil {
		return nil, err
	}
	var files []string
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() {
			continue
		}
		if name == "wal.log" || (len(name) > 4 && name[:4] == "wal.") {
			files = append(files, filepath.Join(w.dir, name))
		}
	}
	// sort lexicographically (os.ReadDir gives directory order but ensure sort)
	if len(files) == 0 {
		return nil, nil
	}
	// simple concatenation
	var out []byte
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			return nil, err
		}
		out = append(out, b...)
	}
	if fromIndex >= uint64(len(out)) {
		return nil, nil
	}
	return out[fromIndex:], nil
}

// RotateWAL renames the current wal.log to wal.<timestamp>.log and starts a new wal.log file.
func (w *walStorage) RotateWAL() error {
	old := w.walPath()
	if _, err := os.Stat(old); os.IsNotExist(err) {
		return nil
	}
	ts := fmt.Sprintf("%d", timeNowUnixNano())
	newname := filepath.Join(w.dir, "wal."+ts+".log")
	if err := os.Rename(old, newname); err != nil {
		return err
	}
	// create fresh wal.log
	f, err := os.OpenFile(old, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	return f.Close()
}

// PurgeOldSnapshots keeps only the latest `keep` snapshots and deletes older ones.
func (w *walStorage) PurgeOldSnapshots(keep int) error {
	entries, err := os.ReadDir(w.dir)
	if err != nil {
		return err
	}
	var snaps []os.DirEntry
	for _, e := range entries {
		name := e.Name()
		if !e.IsDir() && len(name) > 9 && name[:9] == "snapshot." {
			snaps = append(snaps, e)
		}
	}
	if len(snaps) <= keep {
		return nil
	}
	// sort by name lexicographically (older first)
	// entries already in directory order, use ioutil.ReadDir for sorted
	files, err := ioutil.ReadDir(w.dir)
	if err != nil {
		return err
	}
	var snapFiles []os.FileInfo
	for _, fi := range files {
		name := fi.Name()
		if !fi.IsDir() && len(name) > 9 && name[:9] == "snapshot." {
			snapFiles = append(snapFiles, fi)
		}
	}
	// delete older ones, keep latest `keep`
	n := len(snapFiles)
	for i := 0; i < n-keep; i++ {
		os.Remove(filepath.Join(w.dir, snapFiles[i].Name()))
	}
	return nil
}

// timeNowUnixNano is a testable wrapper for time.Now().UnixNano().
func timeNowUnixNano() int64 { return time.Now().UnixNano() }
