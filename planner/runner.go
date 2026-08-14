package planner

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	proc "streamflow/processor"
)

// RunDistributed distributes source records to processors and collects outputs.
func RunDistributed(ctx context.Context, addrs []string, src <-chan proc.Record, timeout time.Duration) ([]proc.Record, error) {
	p := NewPlanner()
	frags := p.Distribute(addrs, nil)

	// open client streams
	var clients []*proc.ClientStream
	for _, f := range frags {
		cs, err := proc.NewClientStream(ctx, f.Address)
		if err != nil {
			// close opened
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
	// start receivers
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

	// send records round-robin
	i := 0
	for r := range src {
		b, _ := json.Marshal(r)
		// unmarshal back to proc.Record to maintain types
		var rr proc.Record
		_ = json.Unmarshal(b, &rr)
		cs := clients[i%len(clients)]
		_ = cs.Send(rr)
		i++
	}

	// wait a bit for output
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
