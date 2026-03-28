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

// create, open, read, write, close, commit operations are visible to client
// create, open, close and commit interact with server
// read, write, append should be local

// no mutex for client since it is assumed that the client is single-threaded
const (
	rpcTimeout      = 3 * time.Second        //
	maxTries        = 3                      // if no response or certain errors, retry thrice at most
	retryDelay      = 500 * time.Millisecond // time between two retries in such a case
	ClientCacheDir  = "./cache"              // dir for client side caching
	maxCacheEntries = 20                     // store at most 20 files in cache, same cap on max open files
)

type CacheEntry struct { // one entry in the cache
	Filename  string      // name of file, was given to server for request
	LocalPath string      // path to this file on local device
	Version   int32       // version, will be sent by server
	Dirty     bool        // has it been altered
	Fd        int32       // file descriptor
	Closed    bool        // if true, this can be evicted, acc LRU
	Mode      pb.FileMode // ReadMode, WriteMode from common
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
		if entry.Closed && !entry.Dirty && entry.Fd == 0{
			os.Remove(entry.LocalPath)
			delete(c.cache, name)
			c.lru = append(c.lru[:i], c.lru[i+1:]...)
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

// retry for write, used by both commit and close
func (c *client) retryWrite(ctx context.Context, req *pb.WriteRequest) (*pb.WriteResponse, error) {
	var lastErr error
	for attempt := 0; attempt < maxTries; attempt++ { // for maxTries number of tries
		ctx2, cancel := context.WithTimeout(ctx, rpcTimeout) // setting timeout
		r, err := c.server.Write(ctx2, req)                  // try write
		cancel()                                             // removes timer and context resources
		if err == nil {                                      // successful write
			return r, nil
		}
		lastErr = err
		code := status.Code(err)
		if code == codes.Unavailable || // only retrying in case of some transient error not logical error
			code == codes.DeadlineExceeded { // again, handled on both client and server side
			time.Sleep(retryDelay) // wait for a while, then retry
			continue
		}
		return nil, err
	}
	return nil, lastErr
}

// testAuth with retry logic
func (c *client) testAuth(ctx context.Context, filename string) (*pb.TestAuthResponse, error) {
	var lastErr error
	for attempt := 0; attempt < maxTries; attempt++ { // for maxTries number of tries
		ctx2, cancel := context.WithTimeout(ctx, rpcTimeout) // setting timeout
		req := &pb.TestAuthRequest{
			Filename: filename,
		}
		r, err := c.server.TestAuth(ctx2, req) // try testauth
		cancel()
		if err == nil { // successful write
			return r, nil
		}
		lastErr = err
		code := status.Code(err)
		if code == codes.Unavailable || // only retrying in case of some transient error not logical error
			code == codes.DeadlineExceeded { // again, handled on both client and server side
			time.Sleep(retryDelay) // wait for a while, then retry
			continue
		}
		return nil, err
	}
	return nil, lastErr
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
	var lastErr error = nil // adding retry logic
	var resp *pb.OpenResponse
	for attempt := 0; attempt < maxTries; attempt++ { // for maxTries number of tries
		ctx2, cancel := context.WithTimeout(ctx, rpcTimeout) // setting timeout
		r, err := c.server.Create(ctx2, req)                 // try sending req
		cancel()
		if err == nil { // successful write
			resp = r
			lastErr = nil
			break
		}
		lastErr = err
		code := status.Code(err)
		if code == codes.Unavailable || // only retrying in case of some transient error not logical error
			code == codes.DeadlineExceeded { // again, handled on both client and server side
			time.Sleep(retryDelay) // wait for a while, then retry
			continue
		}
	}
	if lastErr != nil {
		return nil, lastErr
	}
	localPath := filepath.Join(ClientCacheDir, filename)
	os.MkdirAll(filepath.Dir(localPath), 0755) // owner rwx, grp r-x, other r-x
	file, err := os.Create(localPath)
	if err != nil {
		return nil, err
	}
	file.Close() // closing file on os, does that mean will have to create and then open, not open by default on create?
	entry := &CacheEntry{
		Filename:  filename,
		LocalPath: localPath,
		Version:   resp.Version,
		Dirty:     false,
		Fd:        resp.Fd,
		Closed:    false,
	}
	c.cache[filename] = entry
	touchLRU(c, filename)
	return entry, nil
}

// check cache, if not found, send open request to server
func (c *client) Open(ctx context.Context, filename string, mode pb.FileMode, clientID string) (*CacheEntry, error) {
	entry, ok := c.cache[filename]
	if ok { // found the entry in cache
		if entry.Dirty { // client has uncommitted writes
			return entry, nil
		}
		ta_resp, err := c.testAuth(ctx, filename)
		if err != nil { // error in testauth request
			return nil, err
		}
		if entry.Version == ta_resp.Version {
			if entry.Mode!=mode { // trying to open in a mode other than current
				if entry.Mode == pb.FileMode(ReadMode){
					return nil, status.Error(codes.FailedPrecondition, "file already open in read mode, to open in write, close file then open in write mode")
				}
			}
			touchLRU(c, filename)
			entry.Mode = mode
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
	var lastErr error = nil // adding retry logic
	var resp *pb.OpenResponse
	for attempt := 0; attempt < maxTries; attempt++ { // for maxTries number of tries
		ctx2, cancel := context.WithTimeout(ctx, rpcTimeout) // setting timeout
		r, err := c.server.Open(ctx2, req)                   // try sending req
		cancel()
		if err == nil { // successful write
			resp = r
			lastErr = nil
			break
		}
		lastErr = err
		code := status.Code(err)
		if code == codes.Unavailable || // only retrying in case of some transient error not logical error
			code == codes.DeadlineExceeded { // again, handled on both client and server side
			time.Sleep(retryDelay) // wait for a while, then retry
			continue
		}
	}
	if lastErr != nil {
		return nil, lastErr
	}
	localPath := filepath.Join(ClientCacheDir, filename)
	os.MkdirAll(filepath.Dir(localPath), 0755)      // owner rwx, grp r-x, other r-x
	err := os.WriteFile(localPath, resp.Data, 0644) // owner rw-, grp r--, other r--, don't need exec for this
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
		Mode: mode,
	}
	c.cache[filename] = new_entry
	touchLRU(c, filename)
	return new_entry, nil
}

// send read request to server, currently always reading the whole file, doesn't allow partial reads
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

// just write to file
func (c *client) Write(ctx context.Context, filename string, data []byte) error {
	entry, ok := c.cache[filename]
	if !ok {
		return status.Error(codes.NotFound, "file not open")
	}
	if entry.Mode == pb.FileMode(ReadMode){
		return status.Error(codes.PermissionDenied, "file opened in read mode")
	}
	err := os.WriteFile(entry.LocalPath, data, 0644) // owner rw-, grp r--, other r--
	if err != nil {
		return err
	}
	entry.Dirty = true
	touchLRU(c, filename)
	return nil
}

// added append if client needs it
func (c *client) Append(filename string, data []byte) error {
    entry, ok := c.cache[filename]
    if !ok {
        return status.Error(codes.NotFound, "file not open")
    }
    if entry.Mode == pb.FileMode(ReadMode) {
        return status.Error(codes.PermissionDenied, "file opened in read mode")
    }
    f, err := os.OpenFile(entry.LocalPath, os.O_APPEND|os.O_WRONLY, 0644)
    if err != nil {
        return err
    }
    defer f.Close()
    _, err = f.Write(data)
    if err != nil {
        return err
    }
    entry.Dirty = true
    touchLRU(c, filename)
    return nil
}

// write the changes to the server but keep the file open
func (c *client) Commit(ctx context.Context, filename string) error {
	entry, ok := c.cache[filename]
	if !ok {
		return errors.New("file not in cache")
	}
	if entry.Closed {
		return errors.New("file is closed")
	}
	if !entry.Dirty { // no changes yet, no need to commit
		return nil
	}
	data, err := os.ReadFile(entry.LocalPath)
	if err != nil {
		return err
	}
	req := &pb.WriteRequest{
		RequestId: generateRequestID(),
		Fd:        entry.Fd,
		Version:   entry.Version,
		Data:      data,
	}
	resp, err := c.retryWrite(ctx, req)
	if err != nil {
		return err
	}
	entry.Version = resp.Version
	entry.Dirty = false
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
	entry.Version = resp.Version
	entry.Dirty = false
	entry.Closed = true // can be evicted from cache
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
