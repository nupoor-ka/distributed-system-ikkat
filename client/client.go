package main

import (
	"context"
	"fmt"
	"log"
	"time"

	pb "distributed-fs/proto"

	"google.golang.org/grpc"
)

// Represents the struct in which file store in client cache
type CachedFile struct {
	fd      int32
	version int32
	data    []byte
	dirty   bool
}

func main() {

	//creates a connection to grpc server
	conn, err := grpc.Dial("localhost:50051", grpc.WithInsecure())
	if err != nil {
		log.Fatal(err)
	}

	//excutes this when function ends
	defer conn.Close()

	client := pb.NewFileServiceClient(conn)

	//creates a context with a 1-second timeout.  if RPC takes longer than 1 second → cancel it
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	//ensures resources used by context are cleaned up
	defer cancel()

	//This creates map storing cached file filename->CachedFile
	cache := make(map[string]*CachedFile)

	//Creates a file
	res, err := client.Create(ctx, &pb.FileRequest{
		Filename: "test.txt",
	})

	if err != nil {
		log.Fatal(err)
	}

	//Clients save metadata locally
	cache["test.txt"] = &CachedFile{
		fd:      res.Fd,
		version: res.Version,
		data:    []byte{},
		dirty:   false,
	}

	fmt.Println("File created with version:", res.Version)

	writeData := []byte("Hello Distributed Systems")

	//Modify cache file
	cache["test.txt"].data = writeData
	cache["test.txt"].dirty = true

	//Send the close request
	closeRes, err := client.Close(ctx, &pb.CloseRequest{
		Fd:      cache["test.txt"].fd,
		Version: cache["test.txt"].version,
		Dirty:   cache["test.txt"].dirty,
		Data:    cache["test.txt"].data,
	})

	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Server response:", closeRes.Message)
	fmt.Println("New version:", closeRes.Version)
}
