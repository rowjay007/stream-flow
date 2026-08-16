package processor

import (
	"testing"
	"time"
)

func TestWindowingAndCEPAndJoin(t *testing.T) {
	now := time.Now().UTC()
	in := []TimedRecord{
		{At: now, Record: Record{"id": "1", "v": 1}},
		{At: now.Add(1 * time.Second), Record: Record{"id": "1", "v": 2}},
		{At: now.Add(7 * time.Second), Record: Record{"id": "2", "v": 3}},
	}

	tw := TumblingWindows(in, 5*time.Second)
	if len(tw) != 2 {
		t.Fatalf("expected 2 tumbling windows, got %d", len(tw))
	}

	sw := SlidingWindows(in, 5*time.Second, 2*time.Second)
	if len(sw) == 0 {
		t.Fatalf("expected sliding windows")
	}

	sessions := SessionWindows(in, 3*time.Second)
	if len(sessions) != 2 {
		t.Fatalf("expected 2 sessions, got %d", len(sessions))
	}

	wm := WatermarkGenerator{AllowedLateness: 2 * time.Second}
	got := wm.Observe(now.Add(10 * time.Second))
	if got != now.Add(8*time.Second) {
		t.Fatalf("unexpected watermark: %v", got)
	}

	lr := ApplyLateDataPipeline("k1")
	if lr.RetractKey == "" || lr.UpsertKey == "" {
		t.Fatalf("expected late-data keys")
	}

	cep := NewCEPStateMachine("A", "B", "C")
	if cep.Next("A") || cep.Next("B") || !cep.Next("C") {
		t.Fatalf("expected CEP match on A->B->C")
	}

	left := []TimedRecord{{At: now, Record: Record{"id": "1", "l": "x"}}}
	right := []TimedRecord{{At: now.Add(500 * time.Millisecond), Record: Record{"id": "1", "r": "y"}}}
	joined := StreamJoinWithin(left, right, "id", time.Second)
	if len(joined) != 1 {
		t.Fatalf("expected one joined record, got %d", len(joined))
	}
}
