package processor

import (
	"testing"
)

func TestProcessorPipeline(t *testing.T) {
	in := make(chan Record)
	// map: add field "x2" = x * 2
	mapOp := &MapOperator{Fn: func(r Record) Record {
		x, _ := r["x"].(int)
		r2 := Record{"x2": x * 2, "group": r["group"]}
		return r2
	}}
	// filter: only even x2
	filt := &FilterOperator{Pred: func(r Record) bool {
		v, _ := r["x2"].(int)
		return v%2 == 0
	}}
	// aggregate by group
	agg := &AggregateOperator{Key: "group"}

	rt := NewRuntime(mapOp, filt, agg)

	outCh := rt.Run(in)

	// send inputs
	go func() {
		defer close(in)
		in <- Record{"x": 1, "group": "a"}
		in <- Record{"x": 2, "group": "a"}
		in <- Record{"x": 3, "group": "b"}
	}()

	out := <-outCh
	counts, ok := out["counts"].(map[string]int)
	if !ok {
		t.Fatalf("expected counts map")
	}
	if counts["a"] == 0 {
		t.Fatalf("expected group a count > 0")
	}
}
