package bench

import (
	"flag"
	"fmt"
	"streamflow/broker"
	"time"
)

func RunLoadBench() {
	dir := flag.String("dir", "./data", "broker dir")
	n := flag.Int("n", 1000, "number of messages")
	flag.Parse()

	b, err := broker.NewBroker(*dir)
	if err != nil {
		panic(err)
	}
	_, _ = b.CreateTopic("bench")
	start := time.Now()
	for i := 0; i < *n; i++ {
		_, err := b.Produce("bench", []byte(fmt.Sprintf("k%d", i)), []byte("payload"), nil)
		if err != nil {
			panic(err)
		}
	}
	dur := time.Since(start)
	fmt.Printf("published %d msgs in %s\n", *n, dur)
}
