package connectors

import (
	"streamflow/processor"
	"streamflow/schema"
)

type Source interface {
	Run() <-chan processor.Record
}

type Sink interface {
	Consume(<-chan processor.Record)
}

type InMemorySource struct {
	Records    []processor.Record
	Registry   *schema.Registry
	SchemaName string
}

func (s *InMemorySource) Run() <-chan processor.Record {
	ch := make(chan processor.Record)
	go func() {
		defer close(ch)
		for _, r := range s.Records {

			if s.Registry != nil && s.SchemaName != "" {

				if !s.Registry.Validate(s.SchemaName, r) {
					continue
				}
			}
			ch <- r
		}
	}()
	return ch
}

type InMemorySink struct {
	Collected []processor.Record
}

func (s *InMemorySink) Consume(in <-chan processor.Record) {
	for r := range in {
		s.Collected = append(s.Collected, r)
	}
}
