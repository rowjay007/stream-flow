package streamql

import (
	"regexp"
	"strconv"
	"strings"

	"streamflow/processor"
)

// BuildOperators converts a Plan into processor Operators (projection + filter).
func BuildOperators(p *Plan) []processor.Operator {
	var ops []processor.Operator

	// optional filter: apply before projection or aggregation so predicates have original fields
	if strings.TrimSpace(p.Filter) != "" {
		pred := compilePredicate(p.Filter)
		filt := &processor.FilterOperator{Pred: func(r processor.Record) bool { return pred(r) }}
		ops = append(ops, filt)
	}

	// If aggregates are present, build aggregate operator
	if len(p.Aggregates) > 0 && len(p.GroupBy) > 0 {
		// parse aggregate specs
		re := regexp.MustCompile(`(?i)^\s*(sum|avg|count)\s*\(\s*([a-zA-Z0-9_*]+)\s*\)\s*(?:as\s+([a-zA-Z0-9_]+))?\s*$`)
		var specs []processor.AggSpec
		for _, a := range p.Aggregates {
			m := re.FindStringSubmatch(a)
			if m == nil {
				continue
			}
			funcName := strings.ToLower(m[1])
			field := m[2]
			alias := ""
			if len(m) >= 4 {
				alias = m[3]
			}
			specs = append(specs, processor.AggSpec{Func: funcName, Field: field, Alias: alias})
		}

		// choose time-windowed or single-shot aggregation
		if len(specs) == 1 && specs[0].Func == "count" && specs[0].Field == "*" {
			// preserve legacy AggregateOperator output shape
			ga := &processor.AggregateOperator{Key: p.GroupBy[0]}
			ops = append(ops, ga)
			return ops
		}
		if p.WindowMs > 0 {
			twa := &processor.TimeWindowAggregateOperator{Key: p.GroupBy[0], Aggs: specs, Duration: p.WindowMs}
			ops = append(ops, twa)
			return ops
		}
		afa := &processor.AggregateFuncsOperator{Key: p.GroupBy[0], Aggs: specs}
		ops = append(ops, afa)
		return ops
	}

	// projection operator
	proj := &processor.MapOperator{Fn: func(r processor.Record) processor.Record {
		if len(p.Projections) == 0 || (len(p.Projections) == 1 && p.Projections[0] == "*") {
			return r
		}
		out := make(processor.Record)
		for _, f := range p.Projections {
			if v, ok := r[f]; ok {
				out[f] = v
			}
		}
		return out
	}}
	ops = append(ops, proj)

	return ops
}

// RunPlan runs a Plan against an input source channel and returns the output channel.
func RunPlan(p *Plan, src <-chan processor.Record) <-chan processor.Record {
	ops := BuildOperators(p)
	rt := processor.NewRuntime(ops...)
	return rt.Run(src)
}

// compilePredicate supports very small subset: "field = value", "field > value", "field < value".
func compilePredicate(expr string) func(processor.Record) bool {
	expr = strings.TrimSpace(expr)
	// split by space: field op value
	parts := strings.Fields(expr)
	if len(parts) < 3 {
		return func(processor.Record) bool { return true }
	}
	field := parts[0]
	op := parts[1]
	val := strings.Join(parts[2:], " ")
	// strip optional quotes
	val = strings.Trim(val, "'\"")

	return func(r processor.Record) bool {
		v, ok := r[field]
		if !ok {
			return false
		}
		switch t := v.(type) {
		case int:
			iv, err := strconv.Atoi(val)
			if err != nil {
				return false
			}
			switch op {
			case "=", "==":
				return t == iv
			case ">":
				return t > iv
			case "<":
				return t < iv
			}
		case string:
			switch op {
			case "=", "==":
				return t == val
			}
		}
		return false
	}
}
