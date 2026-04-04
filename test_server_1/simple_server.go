package main

import (
	ikkat "distributed-system-ikkat"
	"flag"
	"log"
	"time"
)

func main() {
	// 1. Get identity from CLI
	id := flag.String("id", "1", "unique server id")
	port := flag.String("port", "5001", "port to listen on")
	ts := flag.Int64("ts", time.Now().Unix(), "timestamp for election")
	flag.Parse()
	root_dir := "storage"

	// 2. Create the server object
	// We pass the ID and Port here so the server knows who it is.
	// Internally, NewServer can load the cluster map from a file or hardcoded list.
	srv := ikkat.NewServer(*id, *port, root_dir, *ts)

	// 3. Just call StartServer
	// All the "magic" (Recovery, Election, gRPC registration) happens inside this call.
	log.Printf("Starting Server %s on port %s...", *id, *port)
	if err := ikkat.StartServer(srv, *port, *ts); err != nil {
		log.Fatalf("Critical server failure: %v", err)
	}
}
