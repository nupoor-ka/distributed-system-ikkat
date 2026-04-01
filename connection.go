package main

import (
	pb "distributed-system-ikkat/filesystem"
	"fmt"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// function to start a server, handles the grpc initialisation portion
func StartServer(s *server, port string) error {
	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		return err
	}
	grpcServer := grpc.NewServer()
	pb.RegisterFileServiceServer(grpcServer, s)
	return grpcServer.Serve(lis)
}

// connecting client, hides grpc part from user
func DialClient(address string) (*client, *grpc.ClientConn, error) {
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials())) // set up client connection, given address of server, no need of credentials
	if err != nil {
		return nil, nil, fmt.Errorf("could not connect to %s: %v", address, err)
	}
	grpcClient := pb.NewFileServiceClient(conn) // FileServiceClient as defined using proto
	customClient := NewClient(grpcClient) // custom fs NewClient function, gives client struct
	return customClient, conn, nil // return *client, *grpc.ClientConn, error
}

// usage for start server

// usage for dial client
// func main() {
//     c, conn, err := DialClient("localhost:50051") // dial client
//     if err != nil {
//         log.Fatal(err)
//     }
//     defer conn.Close() // shut down the network connection on exit
//     c.Open(context.Background(), "output/test.txt", ReadMode, "client-1") // c is client object
// }
