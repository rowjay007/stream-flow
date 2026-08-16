package processor

import (
	"sort"
	"time"
)

type TimedRecord struct {
	At     time.Time
	Record Record
}

func TumblingWindows(in []TimedRecord, size time.Duration) map[time.Time][]Record {
	out := make(map[time.Time][]Record)
	for _, tr := range in {
		bucket := tr.At.Truncate(size)
		out[bucket] = append(out[bucket], tr.Record)
	}
	return out
}

func SlidingWindows(in []TimedRecord, size, slide time.Duration) map[time.Time][]Record {
	out := make(map[time.Time][]Record)
	if len(in) == 0 {
		return out
	}
	sort.Slice(in, func(i, j int) bool { return in[i].At.Before(in[j].At) })
	start := in[0].At.Truncate(slide)
	end := in[len(in)-1].At
	for w := start; !w.After(end); w = w.Add(slide) {
		wEnd := w.Add(size)
		for _, tr := range in {
			if !tr.At.Before(w) && tr.At.Before(wEnd) {
				out[w] = append(out[w], tr.Record)
			}
		}
	}
	return out
}

func SessionWindows(in []TimedRecord, gap time.Duration) [][]Record {
	if len(in) == 0 {
		return nil
	}
	sort.Slice(in, func(i, j int) bool { return in[i].At.Before(in[j].At) })
	var out [][]Record
	current := []Record{in[0].Record}
	last := in[0].At
	for i := 1; i < len(in); i++ {
		if in[i].At.Sub(last) > gap {
			out = append(out, current)
			current = []Record{}
		}
		current = append(current, in[i].Record)
		last = in[i].At
	}
	out = append(out, current)
	return out
}

type WatermarkGenerator struct {
	AllowedLateness time.Duration
	maxEvent        time.Time
}

func (w *WatermarkGenerator) Observe(ts time.Time) time.Time {
	if ts.After(w.maxEvent) {
		w.maxEvent = ts
	}
	return w.maxEvent.Add(-w.AllowedLateness)
}

type LateDataResult struct {
	RetractKey string
	UpsertKey  string
}

func ApplyLateDataPipeline(primaryKey string) LateDataResult {
	return LateDataResult{RetractKey: primaryKey + ":retract", UpsertKey: primaryKey + ":upsert"}
}

type CEPStateMachine struct {
	Pattern []string
	idx     int
}

func NewCEPStateMachine(pattern ...string) *CEPStateMachine {
	return &CEPStateMachine{Pattern: pattern}
}

func (c *CEPStateMachine) Next(event string) bool {
	if len(c.Pattern) == 0 {
		return false
	}
	if c.Pattern[c.idx] == event {
		c.idx++
		if c.idx == len(c.Pattern) {
			c.idx = 0
			return true
		}
		return false
	}
	c.idx = 0
	if c.Pattern[0] == event {
		c.idx = 1
	}
	return false
}

func StreamJoinWithin(left, right []TimedRecord, key string, bound time.Duration) []Record {
	out := make([]Record, 0)
	for _, l := range left {
		lv, lok := l.Record[key]
		if !lok {
			continue
		}
		for _, r := range right {
			rv, rok := r.Record[key]
			if !rok || rv != lv {
				continue
			}
			delta := l.At.Sub(r.At)
			if delta < 0 {
				delta = -delta
			}
			if delta <= bound {
				m := make(Record)
				for k, v := range l.Record {
					m["left."+k] = v
				}
				for k, v := range r.Record {
					m["right."+k] = v
				}
				out = append(out, m)
			}
		}
	}
	return out
}
