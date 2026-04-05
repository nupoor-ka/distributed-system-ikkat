package main

import (
	ikkat "distributed-system-ikkat"
	"flag"
	"log"
)

func main() {
	servers := map[string]ikkat.ServerInfo{
		"1": {ID: "1", Address: "localhost:5001"},
		"2": {ID: "2", Address: "localhost:5002"},
		"3": {ID: "3", Address: "localhost:5003"},
	}
	id := flag.String("id", "", "unique server id") // can get it from cli
	port := flag.String("port", "", "port to listen on")
	flag.Parse()
	rootDir := flag.String("dir", "", "storage directory")
	if *rootDir == "" {
		log.Fatal("storage directory (--dir) is required")
	}
	// We pass the ID and Port here so the server knows who it is.
	// Internally, NewServer can load the cluster map from a file or hardcoded list.
	srv := ikkat.NewServer(*id, *port, *rootDir, servers)
	log.Printf("Starting Server %s on port %s...", *id, *port)
	if err := ikkat.StartServer(srv, *port); err != nil { // Recovery, Election, gRPC registration happens inside this call
		log.Fatalf("Critical server failure: %v", err)
	}
}
