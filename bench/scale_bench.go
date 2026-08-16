package bench

import (
	"flag"
	"fmt"
	"streamflow/broker"
	"sync"
	"time"
)

func RunScaleBench() {
	dir := flag.String("dir", "./data", "broker dir")
	n := flag.Int("n", 10000, "number of messages")
	c := flag.Int("c", 8, "concurrency")
	flag.Parse()

	b, err := broker.NewBroker(*dir)
	if err != nil {
		panic(err)
	}
	_, _ = b.CreateTopic("bench")
	start := time.Now()
	var wg sync.WaitGroup
	wg.Add(*c)
	per := *n / *c
	for i := 0; i < *c; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < per; j++ {
				_, err := b.Produce("bench", []byte(fmt.Sprintf("k-%d-%d", id, j)), []byte("payload"), nil)
				if err != nil {
					panic(err)
				}
			}
		}(i)
	}
	wg.Wait()
	dur := time.Since(start)
	fmt.Printf("published %d msgs in %s\n", *n, dur)
}
