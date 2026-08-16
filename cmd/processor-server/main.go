package main

import (
	"context"
	"flag"
	"fmt"
	"log"

	"streamflow/processor"
)

func main() {
	addr := flag.String("listen", "127.0.0.1:9090", "listen address for gRPC processor server")
	flag.Parse()
	srv, lis, err := processor.StartProcessorServer(*addr, "", "")
	if err != nil {
		log.Fatalf("failed to start processor server: %v", err)
	}
	fmt.Printf("processor server listening %s (listener %v)\n", *addr, lis.Addr())

	select {}
	_ = srv
	_ = context.TODO()
}
