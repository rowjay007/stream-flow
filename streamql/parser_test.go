package streamql

import "testing"

func TestParseSimpleSelect(t *testing.T) {
	q, err := Parse("SELECT a, b FROM topic1 WHERE a > 10")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	if len(q.Select) != 2 || q.Select[0] != "a" || q.Select[1] != "b" {
		t.Fatalf("unexpected select: %v", q.Select)
	}
	if q.From != "topic1" {
		t.Fatalf("unexpected from: %s", q.From)
	}
	if q.Where != "a > 10" {
		t.Fatalf("unexpected where: %s", q.Where)
	}
}
