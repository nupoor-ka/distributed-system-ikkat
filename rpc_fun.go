package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	pb "distributed-fs/filesystem"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

const (
	MaxOpenFiles     = 1000             //Limits how many files your server can keep open at once
	RequestCacheTTL  = 5 * time.Minute  //defines how long a cached request stays valid.
	LeaseTimeout     = 10 * time.Minute //A client holds access rights for 10 minutes before it expires
	ServerStorageDir = "./storage"      //Directory path where server stores files
)

type FileMode int

const (
	ReadMode FileMode = iota
	WriteMode
	ReadWriteMode
)

type FileMeta struct {
	mu       sync.Mutex
	file     *os.File //Pointer to the actual open file
	filename string
	version  int32
	clientID string    //Multiple client may share same IP thus used Client ID
	mode     FileMode  //READ or WRITE
	lastSeen time.Time //Last time this file was accessd
	//current time - lastSenn > 10 -- close file and revoke the resources
}

func (f *FileMeta) IsExpired() bool {
	return time.Since(f.lastSeen) > LeaseTimeout
}

type FileEntry struct {
	mu   sync.Mutex
	cond *sync.Cond //Used to block or wake go routines

	activeReaders  int  //Number of readers currently holding file
	activeWriter   bool //Only 1 writter is allowed
	waitingWriters int  //Number of writers waiting

	version int32
}

type RequestEntry struct {
	response  interface{}
	timestamp time.Time
}

type server struct {
	mu sync.Mutex

	files      map[int32]*FileMeta // FD → metadata
	handleToFD map[string]int32    // handle → FD
	table      map[string]*FileEntry
	nextFD     int32
	requests   map[string]*RequestEntry
	rootDir    string
}

func newServer() *server {
	os.MkdirAll(ServerStorageDir, 0755)

	s := &server{
		files:    make(map[int32]*FileMeta),
		table:    make(map[string]*FileEntry),
		nextFD:   1,
		requests: make(map[string]*RequestEntry),
		rootDir:  ServerStorageDir,
	}

	go s.cleanupRequests()
	go s.cleanupLeases()
	s.startTempFileGC()

	return s
}

func NewFileEntry() *FileEntry {
	fe := &FileEntry{}
	fe.cond = sync.NewCond(&fe.mu)
	return fe
}

func (fe *FileEntry) CanRead() bool {
	return !fe.activeWriter && fe.waitingWriters == 0
}

func (fe *FileEntry) CanWrite() bool {
	return !fe.activeWriter && fe.activeReaders == 0
}

func (fe *FileEntry) AcquireRead() {
	fe.mu.Lock()
	for !fe.CanRead() {
		fe.cond.Wait()
	}
	fe.activeReaders++
	fe.mu.Unlock()
}

func (fe *FileEntry) ReleaseRead() {
	fe.mu.Lock()
	if fe.activeReaders > 0 {
		fe.activeReaders--
	}
	fe.cond.Broadcast()
	fe.mu.Unlock()
}

func (fe *FileEntry) AcquireWrite() {
	fe.mu.Lock()
	fe.waitingWriters++

	for !fe.CanWrite() {
		fe.cond.Wait()
	}

	fe.waitingWriters--
	fe.activeWriter = true
	fe.mu.Unlock()
}

func (fe *FileEntry) ReleaseWrite() {
	fe.mu.Lock()
	fe.activeWriter = false
	fe.cond.Broadcast()
	fe.mu.Unlock()
}

func (s *server) cleanupRequests() {
	for {
		time.Sleep(5 * time.Minute)

		s.mu.Lock()
		for id, entry := range s.requests {
			if time.Since(entry.timestamp) > RequestCacheTTL {
				delete(s.requests, id)
			}
		}
		s.mu.Unlock()
	}
}

func (s *server) cleanupLeases() {
	for {
		time.Sleep(time.Minute)

		s.mu.Lock()
		for fd, meta := range s.files {
			if time.Since(meta.lastSeen) > LeaseTimeout {
				filename := meta.filename
				mode := meta.mode
				fileHandle := meta.file

				// Remove from tracking
				delete(s.files, fd)

				// Search and remove corresponding handle
				for handleID, mappedFD := range s.handleToFD {
					if mappedFD == fd {
						delete(s.handleToFD, handleID)
						break
					}
				}

				// Release the file entry lock based on mode
				entry := s.getFileEntry(filename)
				if mode == ReadMode {
					entry.ReleaseRead()
				} else {
					entry.ReleaseWrite()
				}

				fileHandle.Close()
				fmt.Println("Lease expired FD:", fd)
			}
		}
		s.mu.Unlock()
	}
}

func sanitizePath(p string) (string, error) {
	safe := filepath.Clean(p)
	if filepath.IsAbs(safe) || strings.HasPrefix(safe, "..") {
		return "", fmt.Errorf("invalid path")
	}
	return safe, nil
}

func (s *server) getFileEntry(name string) *FileEntry {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, ok := s.table[name]
	if !ok {
		entry = &FileEntry{version: 1}
		entry.cond = sync.NewCond(&entry.mu)
		s.table[name] = entry
	}
	return entry
}

func (s *server) Create(ctx context.Context, req *pb.CreateRequest) (*pb.OpenResponse, error) {
	// Request cache
	s.mu.Lock()
	if entry, ok := s.requests[req.RequestId]; ok &&
		time.Since(entry.timestamp) < RequestCacheTTL {
		resp := entry.response.(*pb.OpenResponse)
		s.mu.Unlock()
		return resp, nil
	}
	s.mu.Unlock()

	// Sanitize path
	safe, err := sanitizePath(req.Filename)
	if err != nil {
		return nil, err
	}

	// Only output/
	if !strings.HasPrefix(safe, "output/") {
		return nil, status.Errorf(codes.PermissionDenied, "only output/")
	}

	full := filepath.Join(s.rootDir, safe)
	entry := s.getFileEntry(safe)

	// Optimized: Use AcquireWrite to ensure atomicity and handle waiting queues
	entry.AcquireWrite()

	// Create directory -- Ensure all parent dirctory exists
	if err := os.MkdirAll(filepath.Dir(full), 0755); err != nil {
		entry.ReleaseWrite()
		return nil, err
	}

	// Create file
	file, err := os.OpenFile(full, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0666)
	if err != nil {
		entry.ReleaseWrite()
		if os.IsExist(err) {
			return nil, status.Errorf(codes.AlreadyExists, "file exists")
		}
		return nil, err
	}

	// Register file
	s.mu.Lock()
	if len(s.files) >= MaxOpenFiles {
		s.mu.Unlock()
		file.Close()
		os.Remove(full)
		entry.ReleaseWrite()
		return nil, status.Errorf(codes.ResourceExhausted, "too many open files")
	}

	fd := s.nextFD
	s.nextFD++

	client := "unknown"
	if p, ok := peer.FromContext(ctx); ok {
		client = p.Addr.String()
	}

	// Set version to 0 for new file as per your logic
	entry.mu.Lock()
	entry.version = 0
	currentVersion := entry.version
	entry.mu.Unlock()

	s.files[fd] = &FileMeta{
		file:     file,
		filename: safe,
		version:  currentVersion,
		clientID: client,
		mode:     WriteMode,
		lastSeen: time.Now(),
	}

	resp := &pb.OpenResponse{
		Fd:      fd,
		Version: currentVersion,
		Message: "file created",
	}

	s.requests[req.RequestId] = &RequestEntry{
		response:  resp,
		timestamp: time.Now(),
	}
	s.mu.Unlock()

	return resp, nil
}

func (s *server) Delete(ctx context.Context, req *pb.FileRequest) (*pb.DeleteResponse, error) {
	// Request cache
	s.mu.Lock()
	if entry, ok := s.requests[req.RequestId]; ok &&
		time.Since(entry.timestamp) < RequestCacheTTL {
		resp := entry.response.(*pb.DeleteResponse)
		s.mu.Unlock()
		return resp, nil
	}
	s.mu.Unlock()

	// Path sanitize
	safe, err := sanitizePath(req.Filename)
	if err != nil {
		return nil, err
	}

	// Prevent deleting input files
	if strings.HasPrefix(safe, "input/") {
		return nil, status.Errorf(codes.PermissionDenied, "cannot delete input files")
	}

	full := filepath.Join(s.rootDir, safe)
	entry := s.getFileEntry(safe)

	// Optimized: Use AcquireWrite to ensure no one else is reading/writing during delete
	entry.AcquireWrite()
	defer entry.ReleaseWrite()

	// Delete file
	if err := os.Remove(full); err != nil && !os.IsNotExist(err) {
		return nil, status.Errorf(codes.Internal, err.Error())
	}

	s.mu.Lock()
	// Cleanup metadata
	delete(s.table, safe)

	resp := &pb.DeleteResponse{Message: "deleted"}
	s.requests[req.RequestId] = &RequestEntry{
		response:  resp,
		timestamp: time.Now(),
	}
	s.mu.Unlock()

	return resp, nil
}

// Reading the Content file
func readFullFile(f *os.File) ([]byte, error) {
	_, err := f.Seek(0, 0)
	if err != nil {
		return nil, err
	}

	data, err := io.ReadAll(f)
	if err != nil {
		return nil, err
	}

	// reset pointer again
	_, err = f.Seek(0, 0)
	if err != nil {
		return nil, err
	}

	return data, nil
}

// Opens file and send the content to the client
// Thus Open + Close working together
func (s *server) Open(ctx context.Context, req *pb.FileRequest) (*pb.OpenResponse, error) {
	// Cache
	s.mu.Lock()
	if entry, ok := s.requests[req.RequestId]; ok &&
		time.Since(entry.timestamp) < RequestCacheTTL {
		resp := entry.response.(*pb.OpenResponse)
		s.mu.Unlock()
		return resp, nil
	}
	s.mu.Unlock()

	safe, err := sanitizePath(req.Filename)
	if err != nil {
		return nil, err
	}

	full := filepath.Join(s.rootDir, safe)
	entry := s.getFileEntry(safe)
	mode := FileMode(req.Mode)

	// INPUT FILE (ONLY 1 READER)
	if strings.HasPrefix(safe, "input/") {
		entry.AcquireRead() // Using optimized helper

		file, err := os.OpenFile(full, os.O_RDONLY, 0666)
		if err != nil {
			entry.ReleaseRead()
			return nil, status.Errorf(codes.NotFound, "not found")
		}

		data, err := readFullFile(file)
		if err != nil {
			file.Close()
			entry.ReleaseRead()
			return nil, status.Errorf(codes.Internal, "read failed")
		}

		s.mu.Lock()
		if len(s.files) >= MaxOpenFiles {
			s.mu.Unlock()
			file.Close()
			entry.ReleaseRead()
			return nil, status.Errorf(codes.ResourceExhausted, "too many open files")
		}

		fd := s.nextFD
		s.nextFD++

		client := "unknown"
		if p, ok := peer.FromContext(ctx); ok {
			client = p.Addr.String()
		}

		s.files[fd] = &FileMeta{
			file:     file,
			filename: safe,
			version:  entry.version,
			clientID: client,
			mode:     ReadMode,
			lastSeen: time.Now(),
		}

		resp := &pb.OpenResponse{
			Fd:      fd,
			Version: entry.version,
			Data:    data,
			Message: "opened input",
		}

		s.requests[req.RequestId] = &RequestEntry{
			response:  resp,
			timestamp: time.Now(),
		}
		s.mu.Unlock()
		return resp, nil
	}

	// OUTPUT FILE
	if mode == ReadMode {
		entry.AcquireRead()
	} else {
		entry.AcquireWrite()
	}

	flags := os.O_RDONLY
	if mode == WriteMode {
		flags = os.O_RDWR
	}

	file, err := os.OpenFile(full, flags, 0666)
	if err != nil {
		if mode == ReadMode {
			entry.ReleaseRead()
		} else {
			entry.ReleaseWrite()
		}
		return nil, status.Errorf(codes.NotFound, "not found")
	}

	data, err := readFullFile(file)
	if err != nil {
		file.Close()
		if mode == ReadMode {
			entry.ReleaseRead()
		} else {
			entry.ReleaseWrite()
		}
		return nil, status.Errorf(codes.Internal, "read failed")
	}

	s.mu.Lock()
	if len(s.files) >= MaxOpenFiles {
		s.mu.Unlock()
		file.Close()
		if mode == ReadMode {
			entry.ReleaseRead()
		} else {
			entry.ReleaseWrite()
		}
		return nil, status.Errorf(codes.ResourceExhausted, "too many open files")
	}

	fd := s.nextFD
	s.nextFD++

	client := "unknown"
	if p, ok := peer.FromContext(ctx); ok {
		client = p.Addr.String()
	}

	s.files[fd] = &FileMeta{
		file:     file,
		filename: safe,
		version:  entry.version,
		clientID: client,
		mode:     mode, // Corrected: Use the requested mode, not hardcoded ReadMode
		lastSeen: time.Now(),
	}

	resp := &pb.OpenResponse{
		Fd:      fd,
		Version: entry.version,
		Data:    data,
		Message: "opened file",
	}

	s.requests[req.RequestId] = &RequestEntry{
		response:  resp,
		timestamp: time.Now(),
	}
	s.mu.Unlock()

	return resp, nil
}

// Close + Write AFS Style
func (s *server) Close(ctx context.Context, req *pb.CloseRequest) (*pb.CloseResponse, error) {
	// Request cache
	s.mu.Lock()
	if entry, ok := s.requests[req.RequestId]; ok &&
		time.Since(entry.timestamp) < RequestCacheTTL {
		resp := entry.response.(*pb.CloseResponse)
		s.mu.Unlock()
		return resp, nil
	}

	meta, ok := s.files[req.Fd]
	if !ok {
		s.mu.Unlock()
		return &pb.CloseResponse{Message: "already closed"}, nil
	}

	filename := meta.filename
	mode := meta.mode
	fileHandle := meta.file
	s.mu.Unlock()

	entry := s.getFileEntry(filename)

	// Lock file entry for state validation
	entry.mu.Lock()

	// Prevent invalid write
	if req.Dirty && mode != WriteMode {
		entry.mu.Unlock()
		return nil, status.Errorf(codes.PermissionDenied, "not opened in write mode")
	}

	// Version conflict
	if req.Dirty && req.Version != entry.version {
		entry.mu.Unlock()
		return nil, status.Errorf(codes.Aborted, "conflict")
	}
	entry.mu.Unlock()

	// ATOMIC WRITE
	if req.Dirty {
		full := filepath.Join(s.rootDir, filename)
		tmp := full + ".tmp"

		// 1 Write to temp file
		f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "temp open failed")
		}

		if _, err := f.Write(req.Data); err != nil {
			f.Close()
			return nil, status.Errorf(codes.Internal, "write failed")
		}

		// 2 Flush file data
		if err := f.Sync(); err != nil {
			f.Close()
			return nil, status.Errorf(codes.Internal, "sync failed")
		}
		f.Close()

		// 3 Atomic rename
		if err := os.Rename(tmp, full); err != nil {
			return nil, status.Errorf(codes.Internal, "rename failed")
		}

		// Sync directory (VERY IMPORTANT)
		dir, err := os.Open(s.rootDir)
		if err == nil {
			dir.Sync()
			dir.Close()
		}

		// Update version ONLY after success
		entry.mu.Lock()
		entry.version++
		entry.mu.Unlock()
	}

	// RELEASE LOCK STATE using helpers
	if mode == ReadMode {
		entry.ReleaseRead()
	} else {
		entry.ReleaseWrite()
	}

	// Close file AFTER releasing entry locks
	fileHandle.Close()

	// RESPONSE + CLEANUP
	entry.mu.Lock()
	finalVersion := entry.version
	entry.mu.Unlock()

	resp := &pb.CloseResponse{
		Message: "closed",
		Version: finalVersion,
	}

	s.mu.Lock()
	delete(s.files, req.Fd)
	s.requests[req.RequestId] = &RequestEntry{
		response:  resp,
		timestamp: time.Now(),
	}
	s.mu.Unlock()

	return resp, nil
}

// .tmp files are incomplete and present after crash this should be removed
func (s *server) cleanupTempFiles() {
	files, err := os.ReadDir(s.rootDir)
	if err != nil {
		return
	}

	now := time.Now()

	for _, file := range files {
		name := file.Name()

		// Only .tmp files
		if !strings.HasSuffix(name, ".tmp") {
			continue
		}

		fullPath := filepath.Join(s.rootDir, name)
		info, err := file.Info()
		if err != nil {
			continue
		}

		// Skip recent files (avoid race with active writes)
		if now.Sub(info.ModTime()) < 2*time.Minute {
			continue
		}

		// Extract original filename
		original := strings.TrimSuffix(name, ".tmp")
		entry := s.getFileEntry(original)

		entry.mu.Lock()
		// Skip if file is currently in use
		if entry.activeReaders > 0 || entry.activeWriter {
			entry.mu.Unlock()
			continue
		}
		entry.mu.Unlock()

		// Safe to delete
		os.Remove(fullPath)
	}
}

// Call this function on server for .tmp files clean up
func (s *server) startTempFileGC() {
	go func() {
		ticker := time.NewTicker(1 * time.Minute)
		defer ticker.Stop()

		for range ticker.C {
			s.cleanupTempFiles()
		}
	}()
}
