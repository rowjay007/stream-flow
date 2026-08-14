package streamql

import "strings"

type Plan struct {
	Source      string
	Projections []string
	Filter      string
	GroupBy     []string
	Aggregates  []string
	WindowMs    int
}

// Planify converts a Query AST into a simple physical plan.
func Planify(q *Query) *Plan {
	if q == nil {
		return nil
	}
	// detect simple aggregates in select list
	var aggs []string
	for _, s := range q.Select {
		ls := strings.ToLower(s)
		if strings.HasPrefix(ls, "count(") || strings.HasPrefix(ls, "sum(") || strings.HasPrefix(ls, "avg(") {
			aggs = append(aggs, s)
		}
	}
	return &Plan{Source: q.From, Projections: q.Select, Filter: q.Where, GroupBy: q.GroupBy, Aggregates: aggs, WindowMs: q.WindowMs}
}
