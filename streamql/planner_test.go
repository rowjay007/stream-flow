package streamql

import "testing"

func TestPlanify(t *testing.T) {
	q, err := Parse("SELECT x FROM s WHERE x = 1")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	p := Planify(q)
	if p.Source != "s" {
		t.Fatalf("source: %s", p.Source)
	}
	if len(p.Projections) != 1 || p.Projections[0] != "x" {
		t.Fatalf("projections: %v", p.Projections)
	}
	if p.Filter != "x = 1" {
		t.Fatalf("filter: %s", p.Filter)
	}
}
