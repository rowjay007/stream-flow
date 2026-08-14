package streamql

import (
	"testing"

	"streamflow/connectors"
	"streamflow/processor"
)

func TestExecuteSimplePlan(t *testing.T) {
	q, err := Parse("SELECT x, group FROM src WHERE x > 1")
	if err != nil {
		t.Fatal(err)
	}
	plan := Planify(q)

	src := &connectors.InMemorySource{Records: []processor.Record{
		processor.Record{"x": 1, "group": "a"},
		processor.Record{"x": 2, "group": "a"},
		processor.Record{"x": 3, "group": "b"},
	}}

	out := RunPlan(plan, src.Run())
	var results []map[string]interface{}
	for r := range out {
		results = append(results, r)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
}

func TestExecuteGroupByCount(t *testing.T) {
	q, err := Parse("SELECT group, COUNT(*) FROM src WHERE x > 1 GROUP BY group")
	if err != nil {
		t.Fatal(err)
	}
	plan := Planify(q)

	src := &connectors.InMemorySource{Records: []processor.Record{
		processor.Record{"x": 1, "group": "a"},
		processor.Record{"x": 2, "group": "a"},
		processor.Record{"x": 3, "group": "b"},
	}}

	out := RunPlan(plan, src.Run())
	var final processor.Record
	for r := range out {
		final = r
	}
	counts, ok := final["counts"].(map[string]int)
	if !ok {
		t.Fatalf("expected counts map in final result")
	}
	if counts["a"] != 1 || counts["b"] != 1 {
		t.Fatalf("unexpected counts: %#v", counts)
	}
}
