package processor

import (
	"fmt"
	"time"
)

type Record map[string]interface{}

type Operator interface {
	Process(in <-chan Record) <-chan Record
}

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

		if len(counts) > 0 {
			out <- Record{"counts": counts}
		}
	}()
	return out
}

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

type AggSpec struct {
	Func  string
	Field string
	Alias string
}

type AggregateFuncsOperator struct {
	Key  string
	Aggs []AggSpec
}

func (a *AggregateFuncsOperator) Process(in <-chan Record) <-chan Record {
	out := make(chan Record, 1)
	go func() {
		defer close(out)
		sums := make(map[string]map[string]float64)
		counts := make(map[string]map[string]int)
		for r := range in {
			g := ""
			if v, ok := r[a.Key]; ok {
				switch t := v.(type) {
				case string:
					g = t
				default:
					g = fmt.Sprintf("%v", t)
				}
			}
			if _, ok := sums[g]; !ok {
				sums[g] = make(map[string]float64)
				counts[g] = make(map[string]int)
			}
			for _, spec := range a.Aggs {
				alias := spec.Alias
				if alias == "" {
					alias = spec.Func + "_" + spec.Field
				}
				switch spec.Func {
				case "count":
					counts[g][alias]++
				case "sum", "avg":

					if v, ok := r[spec.Field]; ok {
						switch n := v.(type) {
						case int:
							sums[g][alias] += float64(n)
							counts[g][alias]++
						case float64:
							sums[g][alias] += n
							counts[g][alias]++
						}
					}
				}
			}
		}

		res := make(Record)
		groups := make(map[string]map[string]interface{})
		for g, s := range sums {
			groups[g] = make(map[string]interface{})
			for alias, sum := range s {

				c := counts[g][alias]
				if c == 0 {
					groups[g][alias] = sum
				} else {

					isAvg := false
					for _, spec := range a.Aggs {
						al := spec.Alias
						if al == "" {
							al = spec.Func + "_" + spec.Field
						}
						if al == alias && spec.Func == "avg" {
							isAvg = true
							break
						}
					}
					if isAvg {
						groups[g][alias] = sum / float64(c)
					} else {
						groups[g][alias] = sum
					}
				}
			}

			for alias, cnt := range counts[g] {
				if _, ok := s[alias]; !ok {
					groups[g][alias] = cnt
				}
			}
		}
		res["groups"] = groups
		out <- res
	}()
	return out
}

type TimeWindowAggregateOperator struct {
	Key      string
	Aggs     []AggSpec
	Duration int
}

func (t *TimeWindowAggregateOperator) Process(in <-chan Record) <-chan Record {
	out := make(chan Record)
	go func() {
		defer close(out)

		ticker := time.NewTicker(time.Duration(t.Duration) * time.Millisecond)
		defer ticker.Stop()
		sums := make(map[string]map[string]float64)
		counts := make(map[string]map[string]int)
		flush := func() {
			if len(sums) == 0 && len(counts) == 0 {
				return
			}
			res := make(Record)
			groups := make(map[string]map[string]interface{})
			for g, s := range sums {
				groups[g] = make(map[string]interface{})
				for alias, sum := range s {
					c := counts[g][alias]
					if c == 0 {
						groups[g][alias] = sum
					} else {
						isAvg := false
						for _, spec := range t.Aggs {
							al := spec.Alias
							if al == "" {
								al = spec.Func + "_" + spec.Field
							}
							if al == alias && spec.Func == "avg" {
								isAvg = true
								break
							}
						}
						if isAvg {
							groups[g][alias] = sum / float64(c)
						} else {
							groups[g][alias] = sum
						}
					}
				}
				for alias, cnt := range counts[g] {
					if _, ok := s[alias]; !ok {
						groups[g][alias] = cnt
					}
				}
			}
			res["groups"] = groups
			out <- res

			sums = make(map[string]map[string]float64)
			counts = make(map[string]map[string]int)
		}

		for {
			select {
			case r, ok := <-in:
				if !ok {
					flush()
					return
				}
				g := ""
				if v, ok := r[t.Key]; ok {
					switch vv := v.(type) {
					case string:
						g = vv
					default:
						g = fmt.Sprintf("%v", vv)
					}
				}
				if _, ok := sums[g]; !ok {
					sums[g] = make(map[string]float64)
					counts[g] = make(map[string]int)
				}
				for _, spec := range t.Aggs {
					alias := spec.Alias
					if alias == "" {
						alias = spec.Func + "_" + spec.Field
					}
					switch spec.Func {
					case "count":
						counts[g][alias]++
					case "sum", "avg":
						if v, ok := r[spec.Field]; ok {
							switch n := v.(type) {
							case int:
								sums[g][alias] += float64(n)
								counts[g][alias]++
							case float64:
								sums[g][alias] += n
								counts[g][alias]++
							}
						}
					}
				}
			case <-ticker.C:
				flush()
			}
		}
	}()
	return out
}
