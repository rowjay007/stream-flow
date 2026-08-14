package bench

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"streamflow/broker"
)

func BenchmarkBrokerProduce(b *testing.B) {
	dir := filepath.Join(os.TempDir(), "streamflow-bench")
	_ = os.RemoveAll(dir)
	defer os.RemoveAll(dir)

	br, err := broker.NewBroker(dir)
	if err != nil {
		b.Fatal(err)
	}
	if _, err = br.CreateTopic("bench"); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	start := time.Now()
	for i := 0; i < b.N; i++ {
		_, err := br.Produce("bench", []byte("k"), []byte("v"), nil)
		if err != nil {
			b.Fatal(err)
		}
	}
	dur := time.Since(start)
	b.Logf("sent %d records in %s (%.2f rec/s)", b.N, dur, float64(b.N)/dur.Seconds())
}
