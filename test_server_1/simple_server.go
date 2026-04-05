package main

import (
	ikkat "distributed-system-ikkat"
	"flag"
	"log"
)

func main() {
	id := flag.String("id", "1", "unique server id") // can get it from cli
	port := flag.String("port", "5001", "port to listen on")
	flag.Parse()
	root_dir := "storage"
	// We pass the ID and Port here so the server knows who it is.
	// Internally, NewServer can load the cluster map from a file or hardcoded list.
	srv := ikkat.NewServer(*id, *port, root_dir)
	log.Printf("Starting Server %s on port %s...", *id, *port)
	if err := ikkat.StartServer(srv, *port); err != nil { // Recovery, Election, gRPC registration happens inside this call
		log.Fatalf("Critical server failure: %v", err)
	}
}
