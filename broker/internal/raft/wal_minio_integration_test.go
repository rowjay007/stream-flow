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

func TestWALMinIOIntegration(t *testing.T) {
	endpoint := os.Getenv("MINIO_ENDPOINT")
	access := os.Getenv("MINIO_ACCESS_KEY")
	secret := os.Getenv("MINIO_SECRET_KEY")
	bucket := os.Getenv("MINIO_BUCKET")
	if endpoint == "" || access == "" || secret == "" || bucket == "" {
		t.Skip("MINIO env vars not set; skipping MinIO integration test")
	}

	off, err := storage.NewS3Offloader(endpoint, access, secret, false)
	if err != nil {
		t.Fatalf("new s3 offloader: %v", err)
	}

	tmp := t.TempDir()
	ws, err := NewWALStorage(filepath.Join(tmp, "wal"), off)
	if err != nil {
		t.Fatalf("new wal: %v", err)
	}

	content := "minio-snapshot"
	if _, err := ws.SaveSnapshot(strings.NewReader(content)); err != nil {
		t.Fatalf("save snapshot: %v", err)
	}

	entries, _ := os.ReadDir(filepath.Join(tmp, "wal"))
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "snapshot.") {
			os.Remove(filepath.Join(tmp, "wal", e.Name()))
		}
	}

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

	_ = off
	_ = context.Background()
}
