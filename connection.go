package ikkat

import (
	pb "distributed-system-ikkat/filesystem"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// NewServer is the ONLY way for an outsider to create a server object
func NewServer(id string, port string, rootDir string) *server {
	return &server{
		id:           id,
		role:         Backup, // Default
		servers:      make(map[string]ServerInfo),
		table:        make(map[string]*FileEntry),
		requests:     make(map[string]*RequestEntry),
		rootDir:      rootDir,
		logFilePath:  "./log_" + id + ".txt",
	}
}

// function to start a server, handles the grpc initialisation portion
func StartServer(s *server, port string) error {

	address := ":" + port

	// Ensure directories exist
	inputDir := filepath.Join(s.rootDir, "input")
	outputDir := filepath.Join(s.rootDir, "output")

	os.MkdirAll(inputDir, os.ModePerm)
	os.MkdirAll(outputDir, os.ModePerm)

	lis, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}

	grpcServer := grpc.NewServer()

	// ✅ CLIENT API
	pb.RegisterFileServiceServer(grpcServer, s)

	// ✅ REPLICATION + CLUSTER
	pb.RegisterReplicationServiceServer(grpcServer, s)
	pb.RegisterHeartbeatServiceServer(grpcServer, s)
	pb.RegisterRecoveryServiceServer(grpcServer, s)

	log.Println("Server started on", address)
	log.Println("Input dir:", inputDir)
	log.Println("Output dir:", outputDir)

	return grpcServer.Serve(lis)
}

// connecting client, hides grpc part from user
func DialClient(address string) (*client, *grpc.ClientConn, error) {
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials())) // set up client connection, given address of server, no need of credentials
	if err != nil {
		return nil, nil, fmt.Errorf("could not connect to %s: %v", address, err)
	}
	grpcClient := pb.NewFileServiceClient(conn) // FileServiceClient as defined using proto
	customClient := NewClient(grpcClient)       // custom fs NewClient function, gives client struct
	return customClient, conn, nil              // return *client, *grpc.ClientConn, error
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
