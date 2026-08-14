package broker

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestBrokerCreateAndConsume(t *testing.T) {
	dir := filepath.Join(os.TempDir(), "streamflow-broker-test")
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	broker, err := NewBroker(dir)
	if err != nil {
		t.Fatalf("new broker: %v", err)
	}
	if _, err = broker.CreateTopic("orders"); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	for i := 0; i < 5; i++ {
		_, err = broker.Produce("orders", []byte("k"), []byte("v"), map[string]string{"id": "1"})
		if err != nil {
			t.Fatalf("produce: %v", err)
		}
	}

	recs, err := broker.Consume("orders", 0, 10)
	if err != nil {
		t.Fatalf("consume: %v", err)
	}
	if len(recs) != 5 {
		t.Fatalf("expected 5 records, got %d", len(recs))
	}
	if recs[0].Offset != 0 || recs[4].Offset != 4 {
		t.Fatalf("unexpected offsets: %#v", recs)
	}
}

func TestBrokerCommitOffset(t *testing.T) {
	dir := filepath.Join(os.TempDir(), "streamflow-broker-offset-test")
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	broker, err := NewBroker(dir)
	if err != nil {
		t.Fatalf("new broker: %v", err)
	}
	if _, err = broker.CreateTopic("orders"); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	if err = broker.CommitOffset("orders", "g1", 42); err != nil {
		t.Fatalf("commit offset: %v", err)
	}
	got, err := broker.LoadOffset("orders", "g1")
	if err != nil {
		t.Fatalf("load offset: %v", err)
	}
	if got != 42 {
		t.Fatalf("expected 42, got %d", got)
	}
}

func TestSegmentAppendAndReadBack(t *testing.T) {
	seg, err := newSegment(filepath.Join(os.TempDir(), "streamflow-segment-test"), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(filepath.Dir(seg.Path))

	rec := Record{Key: []byte("k"), Value: []byte("v"), Timestamp: time.Now().UTC(), Headers: map[string]string{"x": "y"}}
	offset, err := seg.Append(rec)
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if offset != 0 {
		t.Fatalf("expected offset 0, got %d", offset)
	}

	out, err := seg.ReadRange(0, 10)
	if err != nil {
		t.Fatalf("read range: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 record, got %d", len(out))
	}
	if string(out[0].Key) != "k" || string(out[0].Value) != "v" {
		t.Fatalf("unexpected readback: %#v", out[0])
	}
}

func BenchmarkBrokerProduce(b *testing.B) {
	dir := filepath.Join(os.TempDir(), "streamflow-benchmark")
	if err := os.RemoveAll(dir); err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	br, err := NewBroker(dir)
	if err != nil {
		b.Fatal(err)
	}
	if _, err = br.CreateTopic("bench"); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err = br.Produce("bench", []byte("k"), []byte("v"), map[string]string{"source": "bench"}); err != nil {
			b.Fatal(err)
		}
	}
}
