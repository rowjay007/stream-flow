package planner

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	proc "streamflow/processor"
)

func RunDistributed(ctx context.Context, addrs []string, src <-chan proc.Record, timeout time.Duration) ([]proc.Record, error) {
	p := NewPlanner()
	frags := p.Distribute(addrs, nil)

	var clients []*proc.ClientStream
	for _, f := range frags {
		cs, err := proc.NewClientStream(ctx, f.Address)
		if err != nil {

			for _, c := range clients {
				c.Close()
			}
			return nil, err
		}
		clients = append(clients, cs)
	}

	var mu sync.Mutex
	var outputs []proc.Record
	var wg sync.WaitGroup

	for _, c := range clients {
		wg.Add(1)
		go func(c *proc.ClientStream) {
			defer wg.Done()
			for {
				rec, err := c.Recv()
				if err != nil {
					return
				}
				mu.Lock()
				outputs = append(outputs, rec)
				mu.Unlock()
			}
		}(c)
	}

	i := 0
	for r := range src {
		b, _ := json.Marshal(r)

		var rr proc.Record
		_ = json.Unmarshal(b, &rr)
		cs := clients[i%len(clients)]
		_ = cs.Send(rr)
		i++
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(timeout):
	}

	for _, c := range clients {
		c.Close()
	}
	return outputs, nil
}
