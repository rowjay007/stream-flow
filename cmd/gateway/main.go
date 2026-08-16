package main

import (
	"flag"
	"log"
	"streamflow/broker/region"
)

func main() {
	addr := flag.String("addr", "gateway-1", "gateway address")
	flag.Parse()
	g := region.NewGateway(*addr)
	log.Printf("started region gateway %s (prototype)", g.Addr)
	select {}
}
