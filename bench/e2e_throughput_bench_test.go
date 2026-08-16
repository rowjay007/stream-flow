package bench

import (
	"testing"
	"time"
)

// BenchmarkInMemoryEventRate provides an upper-bound pipeline event-rate baseline.
func BenchmarkInMemoryEventRate(b *testing.B) {
	payload := map[string]interface{}{"id": "a", "v": 1}
	var sink int
	start := time.Now()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if payload["v"].(int) > 0 {
			sink++
		}
	}
	b.StopTimer()
	d := time.Since(start)
	rate := float64(b.N) / d.Seconds()
	b.Logf("in-memory events/sec=%.2f", rate)
	if sink == 0 {
		b.Fatal("unreachable sink")
	}
}
