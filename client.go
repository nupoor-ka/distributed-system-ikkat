package main

import (
	"context"
	"fmt"
	// "io"
	"os"
	"path/filepath"
	// "strings"
	// "sync"
	"time"

	pb "distributed-system-ikkat/filesystem"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const ClientCacheDir = "./cache" // dir for client side caching, nka

type CacheEntry struct { // one entry in the cache
	Filename  string // name of file, was given to server for request
    LocalPath string // path to this file on local device
    Version   int32 // version, will be sent by server
    Dirty     bool // has it been altered
    Fd        int32
}

type client struct {
	server pb.FileServiceClient
	cache map[string]*CacheEntry
}

// intiaiting a new client, given the title of the server from the get-go
func newClient() *client { // initiating a client
	os.MkdirAll(ClientCacheDir, 0755)

	c := &client{
		cache: make(map[string]*CacheEntry),
		server: server,
	}

	return c
}

// generates unique request id to ensure no 
func generateRequestID() string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}

func (c *client) Create(ctx context.Context, filename string, clientID string) (*CacheEntry, error) {

	req := &pb.CreateRequest{
		RequestId: generateRequestID(),
		Filename:  filename,
		ClientId:  clientID,
	}

	resp, err := c.server.Create(ctx, req)
	if err != nil {
		return nil, err
	}

	localPath := filepath.Join(ClientCacheDir, filename)

	os.MkdirAll(filepath.Dir(localPath), 0755)

	file, err := os.Create(localPath)
	if err != nil {
		return nil, err
	}
	file.Close()

	entry := &CacheEntry{
		Filename:  filename,
		LocalPath: localPath,
		Version:   resp.Version,
		Dirty:     false,
		Fd:        resp.Fd,
	}

	c.cache[filename] = entry

	return entry, nil
}

func (c *client) Open(ctx context.Context, filename string, mode pb.FileMode, clientID string) (*CacheEntry, error) {
	entry, ok := c.cache[filename]
	if ok { // found the entry in cache
		if entry.Dirty { // client has uncommitted writes
			return entry, nil
		}
		ta_req := &pb.TestAuthRequest{
			Filename: filename,
		}
		ta_resp, err :=c.server.TestAuth(ctx, ta_req)
		if err!=nil{
			return nil, err
		}
		if entry.Version == ta_resp.Version{
			return entry, nil // returning same entry as cache had up-to-date version
		}
	}
	req := &pb.FileRequest{
		RequestId: generateRequestID(),
		Filename:  filename,
		Mode:      mode,
		ClientId:  clientID,
	}
	resp, err := c.server.Open(ctx, req)
	if err != nil {
		return nil, err
	}
	localPath := filepath.Join(ClientCacheDir, filename)
	os.MkdirAll(filepath.Dir(localPath), 0755)
	err = os.WriteFile(localPath, resp.Data, 0644)
	if err != nil {
		return nil, err
	}
	new_entry := &CacheEntry{
		Filename:  filename,
		LocalPath: localPath,
		Version:   resp.Version,
		Dirty:     false,
		Fd:        resp.Fd,
	}
	c.cache[filename] = new_entry
	return entry, nil
}

// send read request to server
func (c *client) Read(ctx context.Context, filename string) ([]byte, error) {
	entry, ok := c.cache[filename]
	if !ok { // never stored file in cache
		return nil, status.Error(codes.NotFound, "file not open")
	}
	data, err := os.ReadFile(entry.LocalPath)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// file has been changed, send whole file to server
func (c *client) Write(ctx context.Context, filename string, data []byte) error {
	entry, ok := c.cache[filename]
	if !ok {
		return status.Error(codes.NotFound, "file not open")
	}
	err := os.WriteFile(entry.LocalPath, data, 0644) // 
	if err != nil {
		return err
	}
	entry.Dirty = true
	return nil
}

func (c *client) Close(ctx context.Context, filename string) error {

	entry, ok := c.cache[filename]
	if !ok {
		return status.Error(codes.NotFound, "file not open")
	}

	var data []byte

	if entry.Dirty {
		d, err := os.ReadFile(entry.LocalPath)
		if err != nil {
			return err
		}
		data = d
	}

	req := &pb.CloseRequest{
		RequestId: generateRequestID(),
		Fd:        entry.Fd,
		Dirty:     entry.Dirty,
		Version:   entry.Version,
		Data:      data,
	}

	resp, err := c.server.Close(ctx, req)
	if err != nil {
		return err
	}

	entry.Version = resp.Version
	entry.Dirty = false

	delete(c.cache, filename)

	return nil
}

func (c *client) Delete(ctx context.Context, filename string) error {
	entry, ok := c.cache[filename]
	if ok { // the file is in cache
		if entry.Dirty {
			return status.Error(
				codes.FailedPrecondition,
				"cannot delete: file has uncommitted changes",
			)
		}
		// Remove local file
		os.Remove(entry.LocalPath)

		// Remove from cache
		delete(c.cache, filename)
	}

	req := &pb.DeleteRequest{
		RequestId: generateRequestID(),
		Filename:  filename,
	}

	_, err := c.server.Delete(ctx, req)
	if err != nil {
		return err
	}

	return nil
}