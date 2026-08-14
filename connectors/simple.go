package connectors

import "streamflow/processor"

// Source produces Records.
type Source interface {
	Run() <-chan processor.Record
}

// Sink consumes Records.
type Sink interface {
	Consume(<-chan processor.Record)
}

// InMemorySource emits a predefined set of records then closes.
type InMemorySource struct {
	Records []processor.Record
}

func (s *InMemorySource) Run() <-chan processor.Record {
	ch := make(chan processor.Record)
	go func() {
		defer close(ch)
		for _, r := range s.Records {
			ch <- r
		}
	}()
	return ch
}

// InMemorySink collects records into a slice (not concurrency-safe for simplicity).
type InMemorySink struct {
	Collected []processor.Record
}

func (s *InMemorySink) Consume(in <-chan processor.Record) {
	for r := range in {
		s.Collected = append(s.Collected, r)
	}
}
