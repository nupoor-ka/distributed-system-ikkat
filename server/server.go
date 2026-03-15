package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"

	pb "distributed-fs/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
)

// Metadata about an opened file
type FileMeta struct {
	file     *os.File // Pointer to the actual open file
	filename string
	version  int32
	clientIP string // IP address of the client who opened the file
}

// Server structure implementing the gRPC service
type server struct {
	pb.UnimplementedFileServiceServer

	files  map[int32]*FileMeta // fd -> file metadata
	meta   map[string]int32    // filename -> version
	nextFD int32               // Use to generate unique file descriptor

	mu sync.RWMutex
}

// Open existing file
func (s *server) Open(ctx context.Context, req *pb.FileRequest) (*pb.OpenResponse, error) {

	//Server attempts to open the requested file
	file, err := os.Open(req.Filename)
	//If file doesn't exist return error
	if err != nil {
		return nil, err
	}

	s.mu.Lock()

	//Generate the unique file descriptor
	fd := s.nextFD
	s.nextFD++

	//Fetch current version of file
	version := s.meta[req.Filename]

	// Extract client IP
	p, _ := peer.FromContext(ctx)
	clientIP := p.Addr.String()

	//Store metadata
	s.files[fd] = &FileMeta{
		file:     file,
		filename: req.Filename,
		version:  version,
		clientIP: clientIP,
	}

	s.mu.Unlock()

	fmt.Println("File opened:", req.Filename, "by client:", clientIP)

	//Send the file descriptor and version to the client.
	return &pb.OpenResponse{
		Fd:      fd,
		Version: version,
		Message: "file opened",
	}, nil
}

// Create new file
func (s *server) Create(ctx context.Context, req *pb.FileRequest) (*pb.OpenResponse, error) {

	//Creates a new file in the system
	file, err := os.Create(req.Filename)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()

	//Assign the unique file descriptor
	fd := s.nextFD
	s.nextFD++

	// Extract client IP
	p, _ := peer.FromContext(ctx)
	clientIP := p.Addr.String()

	//File starts with version 1
	s.meta[req.Filename] = 1

	//Stores the metadata
	s.files[fd] = &FileMeta{
		file:     file,
		filename: req.Filename,
		version:  1,
		clientIP: clientIP,
	}

	s.mu.Unlock()

	fmt.Println("File created:", req.Filename, "by client:", clientIP)

	//Send the file descriptor and version to the client.
	return &pb.OpenResponse{
		Fd:      fd,
		Version: 1,
		Message: "file created",
	}, nil
}

// Read file contents
func (s *server) Read(ctx context.Context, req *pb.ReadRequest) (*pb.ReadResponse, error) {

	s.mu.RLock()

	//Find the file using the file descriptor.
	meta, ok := s.files[req.Fd]
	if !ok {
		s.mu.RUnlock()
		return nil, fmt.Errorf("invalid file descriptor")
	}

	//If filename starts with "input", check if another client is already processing it
	if strings.HasPrefix(meta.filename, "input") {
		for fd, f := range s.files {
			if fd != req.Fd && f.filename == meta.filename {
				s.mu.RUnlock()
				return nil, fmt.Errorf("file is already processing")
			}
		}
	}

	//Reset file pointer before reading
	meta.file.Seek(0, 0)

	//Read the entire file into memory.
	data, err := io.ReadAll(meta.file)
	if err != nil {
		s.mu.RUnlock()
		return nil, err
	}

	s.mu.RUnlock()

	fmt.Println("File read by:", meta.clientIP)

	//Sends the file content back to client
	return &pb.ReadResponse{
		Data: data,
	}, nil
}

// Write data to file
func (s *server) Write(ctx context.Context, req *pb.WriteRequest) (*pb.WriteResponse, error) {

	s.mu.RLock()

	//Find the file descriptor.
	meta, ok := s.files[req.Fd]
	if !ok {
		s.mu.RUnlock()
		return nil, fmt.Errorf("invalid file descriptor")
	}

	//Write the provided data to the file.
	n, err := meta.file.Write(req.Data)
	if err != nil {
		s.mu.RUnlock()
		return nil, err
	}

	s.mu.RUnlock()

	fmt.Println("Write operation by:", meta.clientIP)

	//Returns the number of bytes written
	return &pb.WriteResponse{
		BytesWritten: int32(n),
	}, nil
}

// Close file and updating it if modified
func (s *server) Close(ctx context.Context, req *pb.CloseRequest) (*pb.CloseResponse, error) {

	s.mu.Lock()

	//Get metadata of file descriptor
	meta, ok := s.files[req.Fd]
	if !ok {
		s.mu.Unlock()
		return nil, fmt.Errorf("invalid file descriptor")
	}

	//Get server version of given file
	serverVersion := s.meta[meta.filename]

	//If file is updated
	if req.Dirty {

		meta.file.Close()

		//Open the file
		file, err := os.OpenFile(meta.filename, os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			s.mu.Unlock()
			return nil, err
		}

		// Append new data to the file
		_, err = file.Write(req.Data)
		if err != nil {
			s.mu.Unlock()
			return nil, err
		}

		file.Close()

		// Increase version after update
		s.meta[meta.filename]++

		fmt.Println("File updated by:", meta.clientIP)

		//File descriptor should be deleted
		delete(s.files, req.Fd)

		s.mu.Unlock()

		//Return the new version
		return &pb.CloseResponse{
			Message: "file updated",
			Version: s.meta[meta.filename],
		}, nil
	}

	// If file not modified
	meta.file.Close()

	fmt.Println("File closed by:", meta.clientIP)

	delete(s.files, req.Fd)

	s.mu.Unlock()

	return &pb.CloseResponse{
		Message: "file closed",
		Version: serverVersion,
	}, nil
}

// Delete file
func (s *server) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {

	s.mu.Lock()

	//Delete the file
	err := os.Remove(req.Filename)
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}

	//Delete the file descriptor and metadata of file
	delete(s.meta, req.Filename)

	s.mu.Unlock()

	fmt.Println("File deleted:", req.Filename)

	return &pb.DeleteResponse{
		Message: "file deleted",
	}, nil
}

// Start server
func main() {

	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		panic(err)
	}

	//Initialize server object
	s := &server{
		files:  make(map[int32]*FileMeta),
		meta:   make(map[string]int32),
		nextFD: 1,
	}

	grpcServer := grpc.NewServer()

	pb.RegisterFileServiceServer(grpcServer, s)

	fmt.Println("Server running on port 50051")

	grpcServer.Serve(lis)
}
