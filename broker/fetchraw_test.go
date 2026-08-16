package broker

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestFetchRawAndSendFileFallback(t *testing.T) {
	dir := filepath.Join(os.TempDir(), "streamflow-fetchraw-test")
	_ = os.RemoveAll(dir)
	defer os.RemoveAll(dir)

	br, err := NewBroker(dir)
	if err != nil {
		t.Fatalf("new broker: %v", err)
	}
	if _, err := br.CreateTopic("orders"); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	rec, err := br.Produce("orders", []byte("k"), []byte("v"), map[string]string{"x": "y"})
	if err != nil {
		t.Fatalf("produce: %v", err)
	}

	f, pos, length, err := br.FetchRaw("orders", rec.Offset)
	if err != nil {
		t.Fatalf("fetch raw: %v", err)
	}

	payload := make([]byte, length)
	if _, err := f.ReadAt(payload, pos); err != nil {
		t.Fatalf("read payload: %v", err)
	}
	var out Record
	if err := json.Unmarshal(payload, &out); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if out.Offset != rec.Offset {
		t.Fatalf("offset mismatch: got %d want %d", out.Offset, rec.Offset)
	}

	var buf bytes.Buffer
	if _, err := SendFile(&buf, f, pos, length); err != nil {
		t.Fatalf("sendfile fallback: %v", err)
	}
	var out2 Record
	if err := json.Unmarshal(buf.Bytes(), &out2); err != nil {
		t.Fatalf("unmarshal buffer: %v", err)
	}
	if out2.Offset != rec.Offset {
		t.Fatalf("offset mismatch after sendfile: %d vs %d", out2.Offset, rec.Offset)
	}
}

func TestSegmentIndexLoad(t *testing.T) {
	dir := filepath.Join(os.TempDir(), "streamflow-index-test")
	_ = os.RemoveAll(dir)
	defer os.RemoveAll(dir)

	br, err := NewBroker(dir)
	if err != nil {
		t.Fatalf("new broker: %v", err)
	}
	if _, err := br.CreateTopic("orders"); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	for i := 0; i < 3; i++ {
		if _, err := br.Produce("orders", []byte("k"), []byte("v"), nil); err != nil {
			t.Fatalf("produce: %v", err)
		}
	}

	t0 := br.topics["orders"]
	seg := t0.Segments[0]
	path := seg.Path

	s2, err := newSegment(path, 0)
	if err != nil {
		t.Fatalf("newSegment: %v", err)
	}

	time.Sleep(10 * time.Millisecond)
	if len(s2.offsets) == 0 {
		t.Fatalf("expected offsets loaded, got 0")
	}
}
