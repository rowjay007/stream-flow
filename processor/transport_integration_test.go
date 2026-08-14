package processor

import (
	"context"
	"testing"
	"time"
)

func TestProcessorEchoStream(t *testing.T) {
	// start server
	srv, lis, err := StartProcessorServer("127.0.0.1:0", "", "")
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Stop()
	addr := lis.Addr().String()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cs, err := NewClientStream(ctx, addr)
	if err != nil {
		t.Fatal(err)
	}
	defer cs.Close()

	// send a record
	rec := Record{"x": 1, "group": "a"}
	if err := cs.Send(rec); err != nil {
		t.Fatal(err)
	}
	// receive echo
	got, err := cs.Recv()
	if err != nil {
		t.Fatal(err)
	}
	switch v := got["x"].(type) {
	case float64:
		if int(v) != 1 {
			t.Fatalf("unexpected echo value: %#v", got)
		}
	case int:
		if v != 1 {
			t.Fatalf("unexpected echo value: %#v", got)
		}
	default:
		t.Fatalf("unexpected type for x: %#v", got)
	}
}
