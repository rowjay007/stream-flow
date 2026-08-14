package processor

import "testing"

func TestWindowAggregateOperator(t *testing.T) {
	in := make(chan Record)
	win := &WindowAggregateOperator{Key: "group", WindowSize: 2}
	out := win.Process(in)

	go func() {
		defer close(in)
		in <- Record{"group": "a"}
		in <- Record{"group": "a"}
		in <- Record{"group": "b"}
		in <- Record{"group": "b"}
	}()

	var emitted int
	for r := range out {
		if _, ok := r["counts"]; !ok {
			t.Fatalf("expected counts field")
		}
		emitted++
	}
	if emitted < 2 {
		t.Fatalf("expected at least 2 windows emitted, got %d", emitted)
	}
}

func TestJoinOperator(t *testing.T) {
	in := make(chan Record)
	right := map[string]Record{"1": {"name": "Alice"}}
	join := &JoinOperator{LeftKey: "id", RightKey: "id", RightMap: right}
	out := join.Process(in)

	go func() {
		defer close(in)
		in <- Record{"id": "1", "x": 10}
		in <- Record{"id": "2", "x": 20}
	}()

	var got int
	for r := range out {
		if r["name"] != "Alice" {
			t.Fatalf("unexpected join result: %#v", r)
		}
		got++
	}
	if got != 1 {
		t.Fatalf("expected 1 joined row, got %d", got)
	}
}
