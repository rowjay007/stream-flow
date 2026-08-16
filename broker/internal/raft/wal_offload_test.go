package raft

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"streamflow/broker/storage"
)

func TestWALSaveAndRestoreViaOffload(t *testing.T) {
	tmp := t.TempDir()
	// create offloader storage dir
	offdir := t.TempDir()
	off := storage.NewLocalOffloader(offdir)

	// create wal storage with offloader
	ws, err := NewWALStorage(filepath.Join(tmp, "wal"), off)
	if err != nil {
		t.Fatalf("new wal: %v", err)
	}

	// save a snapshot
	content := "snapshot-contents"
	if _, err := ws.SaveSnapshot(strings.NewReader(content)); err != nil {
		t.Fatalf("save snapshot: %v", err)
	}

	// remove local snapshot to force offload-based restore
	entries, _ := os.ReadDir(filepath.Join(tmp, "wal"))
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "snapshot.") {
			os.Remove(filepath.Join(tmp, "wal", e.Name()))
		}
	}

	// now call LoadSnapshot which should fetch from offloader
	rc, err := ws.LoadSnapshot()
	if err != nil {
		t.Fatalf("load snapshot: %v", err)
	}
	defer rc.Close()
	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read snapshot: %v", err)
	}
	if string(b) != content {
		t.Fatalf("unexpected snapshot content: %s", string(b))
	}

	// ensure a local copy was saved
	// find file in dir
	found := false
	entries, _ = os.ReadDir(filepath.Join(tmp, "wal"))
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "snapshot.") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected local snapshot to be restored")
	}
	_ = ws
	_ = context.Background()
}
