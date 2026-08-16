package planner

import (
	"context"
	"testing"
	"time"

	"streamflow/processor"
)

func TestRunDistributed(t *testing.T) {

	s1, l1, err := processor.StartProcessorServer("127.0.0.1:0", "", "")
	if err != nil {
		t.Fatal(err)
	}
	defer s1.Stop()
	s2, l2, err := processor.StartProcessorServer("127.0.0.1:0", "", "")
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Stop()

	addr1 := l1.Addr().String()
	addr2 := l2.Addr().String()

	src := make(chan processor.Record)
	go func() {
		defer close(src)
		for i := 0; i < 4; i++ {
			src <- processor.Record{"x": i, "group": "g"}
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	outs, err := RunDistributed(ctx, []string{addr1, addr2}, src, 1*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(outs) == 0 {
		t.Fatalf("expected outputs from distributed processors")
	}
}
