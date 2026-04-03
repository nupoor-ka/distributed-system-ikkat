package ikkat

import (
	"context"
	"errors"
	"fmt"
	"io"

	// "io"
	"os"
	"path/filepath"

	// "strings"
	// "sync"
	"time"

	pb "distributed-system-ikkat/filesystem"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// create, open, read, write, close, commit operations are visible to client
// create, read, close and commit interact with server
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
func NewClient(server pb.FileServiceClient) *client { // initiating a client
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
		if entry.Closed && !entry.Dirty && entry.Fd == 0 {
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
		stream, err := c.server.Write(ctx2)                  // streaming from client side, grpc.ClientStreamingClient[pb.WriteRequest, pb.WriteResponse]
		if err != nil {                                      // error in opening stream
			cancel() // removes timer and context resources
			lastErr = err
			time.Sleep(retryDelay) // wait and retry regardless of error type, can change this later
			continue
		}
		if err := stream.Send(req); err != nil { // start sending if stream opened successfully, if error in send
			cancel()
			lastErr = err
			time.Sleep(retryDelay)
			continue
		}
		resp, err := stream.CloseAndRecv() // close stream and wait for server response
		cancel()
		if err == nil { // successfully completed stream and got a response
			return resp, nil
		}
		lastErr = err
		time.Sleep(retryDelay)
		continue
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
			if entry.Mode != mode { // trying to open in a mode other than current
				if entry.Mode == pb.FileMode(ReadMode) {
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
	os.MkdirAll(filepath.Dir(localPath), 0755) // owner rwx, grp r-x, other r-x
	//err := os.WriteFile(localPath, resp.Data, 0644) // owner rw-, grp r--, other r--, don't need exec for this
	//if err != nil {
	//	return nil, err
	//}

	//Open is not sending data
	// Create EMPTY file (no data yet)
	file, err := os.Create(localPath)
	if err != nil {
		return nil, err
	}
	file.Close()
	new_entry := &CacheEntry{
		Filename:  filename,
		LocalPath: localPath,
		Version:   resp.Version,
		Dirty:     false,
		Fd:        resp.Fd,
		Closed:    false,
		Mode:      mode,
	}
	c.cache[filename] = new_entry
	touchLRU(c, filename)
	return new_entry, nil
}

func withClientID(ctx context.Context, clientID string) context.Context {
	md := metadata.New(map[string]string{
		"client-id": clientID,
	})
	return metadata.NewOutgoingContext(ctx, md)
}

// send read request to server, currently always reading the whole file, doesn't allow partial reads
// Chunking is applied
// At end returns full
func (c *client) Read(ctx context.Context, filename string, clientID string) ([]byte, error) {
	entry, ok := c.cache[filename]
	if !ok {
		return nil, status.Error(codes.NotFound, "file not open")
	}

	// Create/overwrite local file (use temp file for safety)
	tmpPath := entry.LocalPath + ".tmp"
	file, err := os.Create(tmpPath)
	if err != nil {
		return nil, err
	}

	ctx = withClientID(ctx, clientID)
	// Start streaming from server
	stream, err := c.server.Read(ctx, &pb.ReadRequest{
		Filename: filename,
		Fd:       entry.Fd,
	})
	if err != nil {
		file.Close()
		return nil, err
	}

	// Receive chunks
	for {
		chunk, err := stream.Recv()

		if err == io.EOF {
			break
		}
		if err != nil {
			file.Close()
			os.Remove(tmpPath)
			return nil, err
		}

		_, err = file.Write(chunk.Data)
		if err != nil {
			file.Close()
			os.Remove(tmpPath)
			return nil, err
		}
	}

	file.Close()

	// Replace old file atomically
	err = os.Rename(tmpPath, entry.LocalPath)
	if err != nil {
		return nil, err
	}

	// Read full data to return
	data, err := os.ReadFile(entry.LocalPath)
	if err != nil {
		return nil, err
	}

	touchLRU(c, filename)
	return data, nil
}

// just write to file
func (c *client) WriteFile(ctx context.Context, filename string, data []byte) error {
	entry, ok := c.cache[filename]
	if !ok {
		return status.Error(codes.NotFound, "file not open")
	}
	if entry.Mode == pb.FileMode(ReadMode) {
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
func (c *client) AppendFile(filename string, data []byte) error {
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

func (c *client) Commit(ctx context.Context, filename string, clientID string) error {

	entry, ok := c.cache[filename]
	if !ok {
		return errors.New("file not in cache")
	}
	if entry.Closed {
		return errors.New("file is closed")
	}
	if !entry.Dirty {
		return nil
	}

	file, err := os.Open(entry.LocalPath)
	if err != nil {
		return err
	}
	defer file.Close()

	// ✅ ADD METADATA HERE
	ctx = withClientID(ctx, clientID)

	// Start streaming RPC
	stream, err := c.server.Write(ctx)
	if err != nil {
		return err
	}

	buf := make([]byte, ChunkSize)

	for {
		n, err := file.Read(buf)

		if n > 0 {
			req := &pb.WriteRequest{
				RequestId: generateRequestID(),
				Fd:        entry.Fd,
				Version:   entry.Version,
				Data:      buf[:n],
			}

			if err := stream.Send(req); err != nil {
				return err
			}
		}

		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}

	// Close stream and receive response
	resp, err := stream.CloseAndRecv()
	if err != nil {
		return err
	}

	// Update metadata
	entry.Version = resp.Version
	entry.Dirty = false

	return nil
}

// close a file, write if dirty
func (c *client) Close(ctx context.Context, filename string, clientID string) error {

	entry, ok := c.cache[filename]
	if !ok {
		return status.Error(codes.NotFound, "file not open")
	}

	reqID := generateRequestID()

	// ✅ ADD METADATA HERE
	ctx = withClientID(ctx, clientID)

	// Start streaming RPC
	stream, err := c.server.Close(ctx)
	if err != nil {
		return err
	}

	if entry.Dirty {
		file, err := os.Open(entry.LocalPath)
		if err != nil {
			return err
		}
		defer file.Close()

		buf := make([]byte, ChunkSize)

		for {
			n, err := file.Read(buf)

			if n > 0 {
				req := &pb.CloseRequest{
					RequestId: reqID,
					Fd:        entry.Fd,
					Dirty:     true,
					Version:   entry.Version,
					Data:      buf[:n],
				}

				if err := stream.Send(req); err != nil {
					return err
				}
			}

			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}
		}
	} else {
		// No data, but still notify server
		req := &pb.CloseRequest{
			RequestId: reqID,
			Fd:        entry.Fd,
			Dirty:     false,
			Version:   entry.Version,
			Data:      nil,
		}

		if err := stream.Send(req); err != nil {
			return err
		}
	}

	// Close stream and receive response
	resp, err := stream.CloseAndRecv()
	if err != nil {
		return err
	}

	// Update metadata
	entry.Version = resp.Version
	entry.Dirty = false
	entry.Closed = true
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

func (c *client) ReadFile(filename string) ([]byte, error) {
	entry, ok := c.cache[filename]
	if !ok {
		return nil, status.Error(codes.NotFound, "file not open")
	}

	data, err := os.ReadFile(entry.LocalPath)
	if err != nil {
		return nil, err
	}

	touchLRU(c, filename)

	return data, nil
}
