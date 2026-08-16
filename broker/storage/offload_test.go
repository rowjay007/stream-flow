package storage

import (
	"context"
	"io"
	"os"
	"strings"
	"testing"
)

func TestLocalOffloaderUploadDownload(t *testing.T) {
	dir := t.TempDir()
	off := NewLocalOffloader(dir)
	ctx := context.Background()
	bucket := "b"
	key := "k.txt"
	content := "hello snapshot"
	r := strings.NewReader(content)
	if _, err := off.Upload(ctx, bucket, key, r, int64(len(content))); err != nil {
		t.Fatal(err)
	}
	rc, err := off.Download(ctx, bucket, key)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != content {
		t.Fatalf("unexpected content: %s", string(b))
	}

	p := dir + "/" + bucket + "/" + key
	if _, err := os.Stat(p); err != nil {
		t.Fatal(err)
	}
}
