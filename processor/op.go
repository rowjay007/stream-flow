package processor

import "fmt"

// Record represents a single event payload in a generic map form.
type Record map[string]interface{}

// Operator consumes a channel of Records and returns an output channel of Records.
type Operator interface {
	Process(in <-chan Record) <-chan Record
}

// MapOperator applies a transformation function to each record.
type MapOperator struct {
	Fn func(Record) Record
}

func (m *MapOperator) Process(in <-chan Record) <-chan Record {
	out := make(chan Record)
	go func() {
		defer close(out)
		for r := range in {
			nr := m.Fn(r)
			if nr != nil {
				out <- nr
			}
		}
	}()
	return out
}

// FilterOperator filters records by predicate. If predicate returns true,
// the record is forwarded.
type FilterOperator struct {
	Pred func(Record) bool
}

func (f *FilterOperator) Process(in <-chan Record) <-chan Record {
	out := make(chan Record)
	go func() {
		defer close(out)
		for r := range in {
			if f.Pred(r) {
				out <- r
			}
		}
	}()
	return out
}

// AggregateOperator performs a simple count aggregation grouping by the
// provided key. When the input channel closes it emits a single Record with
// counts under the field "counts".
type AggregateOperator struct {
	Key string
}

func (a *AggregateOperator) Process(in <-chan Record) <-chan Record {
	out := make(chan Record, 1)
	go func() {
		defer close(out)
		counts := make(map[string]int)
		for r := range in {
			if v, ok := r[a.Key]; ok {
				s := ""
				switch t := v.(type) {
				case string:
					s = t
				default:
					s = fmt.Sprintf("%v", t)
				}
				counts[s]++
			}
		}
		out <- Record{"counts": counts}
	}()
	return out
}

// WindowAggregateOperator emits aggregated counts per key every WindowSize
// records (simple count-based tumbling window).
type WindowAggregateOperator struct {
	Key        string
	WindowSize int
}

func (w *WindowAggregateOperator) Process(in <-chan Record) <-chan Record {
	out := make(chan Record)
	go func() {
		defer close(out)
		counts := make(map[string]int)
		seen := 0
		for r := range in {
			if v, ok := r[w.Key]; ok {
				s := ""
				switch t := v.(type) {
				case string:
					s = t
				default:
					s = fmt.Sprintf("%v", t)
				}
				counts[s]++
			}
			seen++
			if w.WindowSize > 0 && seen%w.WindowSize == 0 {
				out <- Record{"counts": counts}
				counts = make(map[string]int)
			}
		}
		// emit remaining
		if len(counts) > 0 {
			out <- Record{"counts": counts}
		}
	}()
	return out
}

// JoinOperator performs a simple hash-join against a preloaded right-side map.
type JoinOperator struct {
	LeftKey  string
	RightKey string
	RightMap map[string]Record
}

func (j *JoinOperator) Process(in <-chan Record) <-chan Record {
	out := make(chan Record)
	go func() {
		defer close(out)
		for l := range in {
			lk := ""
			if v, ok := l[j.LeftKey]; ok {
				switch t := v.(type) {
				case string:
					lk = t
				default:
					lk = fmt.Sprintf("%v", t)
				}
			}
			if r, ok := j.RightMap[lk]; ok {
				merged := make(Record)
				for k, v := range l {
					merged[k] = v
				}
				for k, v := range r {
					merged[k] = v
				}
				out <- merged
			}
		}
	}()
	return out
}
