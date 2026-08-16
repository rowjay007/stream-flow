package connectors

import "streamflow/processor"

type KafkaBridgeConnector struct {
	SourceTopic string
	SinkTopic   string
}

func (c *KafkaBridgeConnector) Name() string { return "kafka-bridge" }

func (c *KafkaBridgeConnector) Run(source Source, sink Sink) {
	if source == nil || sink == nil {
		return
	}
	ch := source.Run()
	sink.Consume(ch)
}

func ToRecord(k, v string) processor.Record {
	return processor.Record{"key": k, "value": v}
}
