package broker

import (
	"os"
	"path/filepath"
	"testing"
)

func TestProduceIdempotentDeduplicates(t *testing.T) {
	dir := t.TempDir()
	b, err := NewBroker(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.CreateTopic("orders"); err != nil {
		t.Fatal(err)
	}

	r1, dup, err := b.ProduceIdempotent("orders", []byte("k"), []byte("v"), nil, "p1", 1)
	if err != nil || dup {
		t.Fatalf("first write failed: err=%v dup=%v", err, dup)
	}
	r2, dup, err := b.ProduceIdempotent("orders", []byte("k"), []byte("v"), nil, "p1", 1)
	if err != nil || !dup {
		t.Fatalf("dedup write failed: err=%v dup=%v", err, dup)
	}
	if r1.Offset != r2.Offset {
		t.Fatalf("expected duplicate to return original offset, got %d and %d", r1.Offset, r2.Offset)
	}
}

func TestTransactionCommitAbort(t *testing.T) {
	b, err := NewBroker(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.CreateTopic("orders"); err != nil {
		t.Fatal(err)
	}

	txID, err := b.BeginTransaction("producer-a", 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.TxProduce(txID, "orders", []byte("k1"), []byte("v1"), nil); err != nil {
		t.Fatal(err)
	}
	committed, err := b.CommitTransaction(txID)
	if err != nil {
		t.Fatal(err)
	}
	if committed != 1 {
		t.Fatalf("expected 1 committed record, got %d", committed)
	}

	recs, err := b.ConsumeReadCommitted("orders", 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 1 {
		t.Fatalf("expected 1 committed record, got %d", len(recs))
	}

	txID2, err := b.BeginTransaction("producer-a", 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.TxProduce(txID2, "orders", []byte("k2"), []byte("v2"), nil); err != nil {
		t.Fatal(err)
	}
	if err := b.AbortTransaction(txID2); err != nil {
		t.Fatal(err)
	}
	recs, err = b.ConsumeReadCommitted("orders", 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 1 {
		t.Fatalf("abort should not append data, got %d", len(recs))
	}
}

func TestConsumerGroupCoordinatorRebalance(t *testing.T) {
	c := NewConsumerGroupCoordinator()
	c.Join("g", "a")
	c.Join("g", "b")
	c.Rebalance("g", 4)
	assign := c.Assignment("g")
	if len(assign["a"])+len(assign["b"]) != 4 {
		t.Fatalf("expected 4 total partitions assigned, got %#v", assign)
	}
}

func TestSparseMMapIndexAppendFind(t *testing.T) {
	path := filepath.Join(t.TempDir(), "idx.mmap")
	idx, err := OpenSparseMMapIndex(path, 32)
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()
	if err := idx.Append(10, 512); err != nil {
		t.Fatal(err)
	}
	pos, ok := idx.Find(10)
	if !ok || pos != 512 {
		t.Fatalf("expected to find offset 10 at 512, got pos=%d ok=%v", pos, ok)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("index file missing: %v", err)
	}
}
