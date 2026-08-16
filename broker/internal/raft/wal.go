package raft

import (
	"context"
	"fmt"
	raftpb "go.etcd.io/etcd/raft/v3/raftpb"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"streamflow/broker/storage"
	"strings"
	"time"
)

type walStorage struct {
	dir     string
	offload storage.Offloader
}

func NewWALStorage(dir string, off storage.Offloader) (Storage, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &walStorage{dir: dir, offload: off}, nil
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

	if w.offload != nil {

		rf, err := os.Open(path)
		if err == nil {
			defer rf.Close()

			if _, err := w.offload.Upload(context.Background(), "snapshots", filepath.Base(path), rf, -1); err != nil {

			}
		}
	}
	return id, nil
}

func (w *walStorage) LoadSnapshot() (io.ReadCloser, error) {

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

		if w.offload != nil {
			keys, err := w.offload.List(context.Background(), "snapshots", "snapshot.")
			if err == nil && len(keys) > 0 {

				latestKey := keys[0]
				for _, k := range keys {
					if k > latestKey {
						latestKey = k
					}
				}

				rc, err := w.offload.Download(context.Background(), "snapshots", latestKey)
				if err == nil {
					defer rc.Close()
					dst := filepath.Join(w.dir, latestKey)
					if f, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644); err == nil {
						io.Copy(f, rc)
						f.Close()
						return os.Open(dst)
					}
				}
			}
		}
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
	sort.Strings(files)
	if len(files) == 0 {
		return nil, nil
	}

	var out []byte
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			return nil, err
		}
		i := 0
		for i < len(b) {
			if i+4 > len(b) {
				break
			}
			l := int(uint32(b[i])<<24 | uint32(b[i+1])<<16 | uint32(b[i+2])<<8 | uint32(b[i+3]))
			i += 4
			if l < 0 || i+l > len(b) {
				break
			}
			out = append(out, b[i:i+l]...)
			i += l
		}
	}
	if fromIndex >= uint64(len(out)) {
		return nil, nil
	}
	return out[fromIndex:], nil
}

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

	f, err := os.OpenFile(old, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	return f.Close()
}

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

	n := len(snapFiles)
	for i := 0; i < n-keep; i++ {
		os.Remove(filepath.Join(w.dir, snapFiles[i].Name()))
	}
	return nil
}

func (w *walStorage) Compact(compactIndex uint64) error {
	data, err := w.ReadWAL(0)
	if err != nil {
		return err
	}
	ents, err := unmarshalEntries(data)
	if err != nil {
		return err
	}
	var keep []raftpb.Entry
	hasIndexedEntries := false
	for _, e := range ents {
		if e.Index > 0 {
			hasIndexedEntries = true
			break
		}
	}
	for i, e := range ents {
		if hasIndexedEntries {
			if e.Index > compactIndex {
				keep = append(keep, e)
			}
			continue
		}

		if uint64(i+1) > compactIndex {
			keep = append(keep, e)
		}
	}

	b, err := marshalEntries(keep)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(w.walPath(), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()

	l := uint32(len(b))
	hdr := []byte{byte(l >> 24), byte(l >> 16), byte(l >> 8), byte(l)}
	if _, err := f.Write(hdr); err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		return err
	}

	entries, err := os.ReadDir(w.dir)
	if err == nil {
		for _, e := range entries {
			name := e.Name()
			if !e.IsDir() && name != "wal.log" && strings.HasPrefix(name, "wal.") {
				os.Remove(filepath.Join(w.dir, name))
			}
		}
	}
	return nil
}

func timeNowUnixNano() int64 { return time.Now().UnixNano() }
