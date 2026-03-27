package main

import (
	"context"
	"errors"
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

// no mutex for client since it is assumed that the client is single-threaded

const ClientCacheDir = "./cache" // dir for client side caching
const maxCacheEntries = 20       // store at most 20 files in cache, same cap on max open files

type CacheEntry struct { // one entry in the cache
	Filename  string // name of file, was given to server for request
	LocalPath string // path to this file on local device
	Version   int32  // version, will be sent by server
	Dirty     bool   // has it been altered
	Fd        int32  // file descriptor
	Closed    bool   // if true, this can be evicted, acc LRU
}

type client struct {
	server pb.FileServiceClient
	cache  map[string]*CacheEntry
	lru    []string
}

// intiaiting a new client, given the title of the server from the get-go
func newClient(server pb.FileServiceClient) *client { // initiating a client
	os.MkdirAll(ClientCacheDir, 0755) // owner rwx, grp r-x, other r-x
	c := &client{
		cache:  make(map[string]*CacheEntry),
		server: server,
	}
	return c
}

// generates unique request id to ensure no
func generateRequestID() string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}

// make space in cache, lru
func evictFromCache(c *client) error {
	for i, name := range c.lru {
		entry, ok := c.cache[name]
		if !ok {
			continue
		}
		if entry.Closed && !entry.Dirty {
			os.Remove(entry.LocalPath)
			delete(c.cache, name)
			c.lru = append(c.lru[:i], c.lru[i+1:]...)
			fmt.Println("Evicted:", name)
			return nil // only need to remove one
		}
	}
	return errors.New("max number of open files reached, close a file before opening another")
}

// every access of a file should move it to the top of lru
func touchLRU(c *client, filename string) {
	for i, name := range c.lru {
		if name == filename {
			c.lru = append(c.lru[:i], c.lru[i+1:]...) // removing it from its original spot
			break
		}
	}
	c.lru = append(c.lru, filename) // adding it to the end
}

// send create request to server,
func (c *client) Create(ctx context.Context, filename string, clientID string) (*CacheEntry, error) {
	if len(c.cache) >= maxCacheEntries {
		err := evictFromCache(c)
		if err != nil {
			return nil, err
		}
	}
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
	os.MkdirAll(filepath.Dir(localPath), 0755) // owner rwx, grp r-x, other r-x
	file, err := os.Create(localPath)
	if err != nil {
		return nil, err
	}
	file.Close() // what??
	entry := &CacheEntry{
		Filename:  filename,
		LocalPath: localPath,
		Version:   resp.Version,
		Dirty:     false,
		Fd:        resp.Fd,
		Closed:    false,
	}
	c.cache[filename] = entry
	c.lru = append(c.lru, filename)
	return entry, nil
}

// check cache, if not found, send open request to server
func (c *client) Open(ctx context.Context, filename string, mode pb.FileMode, clientID string) (*CacheEntry, error) {
	entry, ok := c.cache[filename]
	if ok { // found the entry in cache
		if entry.Dirty { // client has uncommitted writes
			return entry, nil
		}
		ta_req := &pb.TestAuthRequest{
			Filename: filename,
		}
		ta_resp, err := c.server.TestAuth(ctx, ta_req)
		if err != nil {
			return nil, err
		}
		if entry.Version == ta_resp.Version {
			touchLRU(c, filename)
			return entry, nil // returning same entry as cache had up-to-date version
		}
	}
	if len(c.cache) >= maxCacheEntries { // max 20 files open at once
		err := evictFromCache(c)
		if err != nil {
			return nil, err
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
	os.MkdirAll(filepath.Dir(localPath), 0755)     // owner rwx, grp r-x, other r-x
	err = os.WriteFile(localPath, resp.Data, 0644) // owner rw-, grp r--, other r--, don't need exec for this
	if err != nil {
		return nil, err
	}
	new_entry := &CacheEntry{
		Filename:  filename,
		LocalPath: localPath,
		Version:   resp.Version,
		Dirty:     false,
		Fd:        resp.Fd,
		Closed:    false,
	}
	c.cache[filename] = new_entry
	c.lru = append(c.lru, filename)
	return new_entry, nil
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
	touchLRU(c, filename)
	return data, nil
}

// file has been changed, send whole file to server
func (c *client) Write(ctx context.Context, filename string, data []byte) error {
	entry, ok := c.cache[filename]
	if !ok {
		return status.Error(codes.NotFound, "file not open")
	}
	if entry.Fd == 0 {
		return status.Error(codes.PermissionDenied, "not opened in write mode")
	}
	err := os.WriteFile(entry.LocalPath, data, 0644) // owner rw-, grp r--, other r--
	if err != nil {
		return err
	}
	entry.Dirty = true
	touchLRU(c, filename)
	return nil
}

// close a file, write if dirty
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
	entry.Version = resp.Version // this doesn't seem necessary
	entry.Dirty = false          // neither does this
	entry.Closed = true          // can be evicted from cache
	entry.Fd = 0
	touchLRU(c, filename)
	return nil
}

// send delete request to server
func (c *client) Delete(ctx context.Context, filename string) error {
	entry, ok := c.cache[filename]
	if ok { // the file is in cache
		if entry.Dirty {
			return status.Error(
				codes.FailedPrecondition,
				"cannot delete: file has uncommitted changes",
			)
		}
		if err := os.Remove(entry.LocalPath); err != nil && !os.IsNotExist(err) { // remove client side file
			return err
		}
		delete(c.cache, filename) // remove file cache entry
	}
	req := &pb.DeleteRequest{
		RequestId: generateRequestID(),
		Filename:  filename,
	}
	_, err := c.server.Delete(ctx, req)
	if err != nil {
		return err
	}
	for i, name := range c.lru { // need to remove it from cache
		if name == filename {
			c.lru = append(c.lru[:i], c.lru[i+1:]...)
			break
		}
	}
	return nil
}
