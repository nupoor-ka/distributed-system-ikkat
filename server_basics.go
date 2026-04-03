package ikkat

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	pb "distributed-system-ikkat/filesystem"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	MaxOpenFiles     = 1000             //Limits how many files your server can keep open at once
	RequestCacheTTL  = 5 * time.Minute  //defines how long a cached request stays valid.
	LeaseTimeout     = 10 * time.Minute //A client holds access rights for 10 minutes before it expires
	ServerStorageDir = "storage"        //Directory path where server stores files
)

type ClientState struct {
	lastSeen time.Time
	mode     FileMode
}

type FileMeta struct {
	mu       sync.Mutex
	file     *os.File // Pointer to the actual open file
	filename string
	version  int32
	clients  map[string]ClientState
}

type FilePrimeSet map[uint64]struct{} //////////

type FileEntry struct {
	mu             sync.Mutex
	FD             int32
	cond           *sync.Cond //Used to block or wake go routines
	activeReaders  int        //Number of readers currently holding file
	activeWriter   bool       //Only 1 writter is allowed
	waitingWriters int        //Number of writers waiting
	version        int32
}

type RequestEntry struct {
	response  interface{}
	timestamp time.Time
}

type FileKey struct {
	clientID string
	filename string
}

type server struct {
	pb.UnimplementedFileServiceServer
	pb.UnimplementedReplicationServiceServer
	pb.UnimplementedRecoveryServiceServer
	pb.UnimplementedHeartbeatServiceServer
	mu            sync.Mutex
	id            string
	role          Role
	primaryID     string
	servers       map[string]ServerInfo // cluster info
	files         map[int32]*FileMeta   // file system (runtime)
	table         map[string]*FileEntry
	nextFD        int32
	requests      map[string]*RequestEntry // request cache (idempotency)
	openMap       map[FileKey]int32        // lookup fd using filename and client_id
	rootDir       string
	log           []LogEntry // replication
	commitIndex   int
	lastApplied   int
	lastHeartbeat time.Time                // failure detection
	logFilePath   string                   // persistence
	filesPrimes   map[string]*FilePrimeSet // file to file prime set, only needed by primary, if a server is primary, created on boot
}

// Building version table after crash
func (s *server) saveVersion(filename string, version int32) {
	metaPath := filepath.Join(s.rootDir, filename+".meta")
	data := []byte(fmt.Sprintf("%d", version))
	os.WriteFile(metaPath, data, 0644)
}

func (s *server) loadVersion(filename string) int32 {
	metaPath := filepath.Join(s.rootDir, filename+".meta")
	data, err := os.ReadFile(metaPath)
	if err != nil {
		return 1 // default version
	}
	v, err := strconv.Atoi(string(data))
	if err != nil {
		return 1
	}
	return int32(v)
}

func (s *server) rebuildVersionTable() {

	files, err := os.ReadDir(s.rootDir)
	if err != nil {
		log.Println("Error reading directory:", err)
		return
	}

	for _, f := range files {
		name := f.Name()
		fullPath := filepath.Join(s.rootDir, name)

		// ======================
		// HANDLE TEMP FILES
		// ======================
		if strings.HasSuffix(name, ".tmp") {

			// Strategy: DELETE incomplete writes
			os.Remove(fullPath)

			log.Println("Removed stale temp file:", name)
			continue
		}

		// Skip meta files
		if strings.HasSuffix(name, ".meta") {
			continue
		}

		// ======================
		// LOAD VERSION
		// ======================
		version := s.loadVersion(name)

		// ======================
		// REBUILD ENTRY
		// ======================
		entry := &FileEntry{
			version: version,
		}

		// Reset locks (important after crash)
		entry.activeReaders = 0
		entry.activeWriter = false

		s.table[name] = entry

		log.Println("Recovered file:", name, "version:", version)
	}
}

//////////////////////////////////////////////////////////////////////////

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

// For input files -- because there is no writer
func (fe *FileEntry) AcquireReadNoPriority() {
	fe.mu.Lock()
	for fe.activeWriter {
		fe.cond.Wait()
	}
	fe.activeReaders++
	fe.mu.Unlock()
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
		type expiredLease struct {
			clientID string
			filename string
			mode     FileMode
			file     *os.File
		}
		var expired []expiredLease
		s.mu.Lock() // collect expired leases
		for fd, meta := range s.files {

			meta.mu.Lock()

			for clientID, client := range meta.clients {
				if time.Since(client.lastSeen) > LeaseTimeout {

					// collect expired lease
					expired = append(expired, expiredLease{
						clientID: clientID,
						filename: meta.filename,
						mode:     client.mode,
						file:     meta.file,
					})

					// remove client from meta
					delete(meta.clients, clientID)

					// remove from openMap
					key := FileKey{
						clientID: clientID,
						filename: meta.filename,
					}
					delete(s.openMap, key)

					fmt.Println("Lease expired client:", clientID, "FD:", fd)
				}
			}

			meta.mu.Unlock()
		}
		s.mu.Unlock()

		// ======================
		// RELEASE LOCKS OUTSIDE
		// ======================
		for _, e := range expired {

			entry := s.getFileEntry(e.filename)

			if e.mode == ReadMode {
				entry.ReleaseRead()
			} else if e.mode == WriteMode || e.mode == ReadWriteMode {
				entry.ReleaseWrite()
			}
		}
	}
}
func (s *server) FilterUniquePrimes(primes []uint64, primeSet FilePrimeSet) []uint64 {
	var unique []uint64
	for _, p := range primes {
		if _, exists := primeSet[p]; !exists {
			primeSet[p] = struct{}{} // persists automatically
			unique = append(unique, p)
		}
	}
	return unique
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

	clientID := req.ClientId

	// Set version to 0 for new file as per your logic
	entry.mu.Lock()
	entry.version = 0
	currentVersion := entry.version
	entry.mu.Unlock()

	meta := &FileMeta{
		file:     file,
		filename: safe,
		version:  currentVersion,
		clients:  make(map[string]ClientState),
		// primeSet: make(FilePrimeSet), // if using
	}

	meta.clients[clientID] = ClientState{
		lastSeen: time.Now(),
		mode:     FileMode(req.Mode),
	}

	s.files[fd] = meta

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

func (s *server) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
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

	// Check file existence
	info, err := os.Stat(full)

	// Idempotent delete
	if err != nil && os.IsNotExist(err) {
		resp := &pb.DeleteResponse{Message: "File is already deleted"}

		s.mu.Lock()
		s.requests[req.RequestId] = &RequestEntry{
			response:  resp,
			timestamp: time.Now(),
		}
		s.mu.Unlock()

		return resp, nil
	}

	// Disallow Directory delete
	if err == nil && info.IsDir() {
		return nil, status.Errorf(codes.InvalidArgument, "cannot delete directory")
	}

	// Do NOT recreate entry
	s.mu.Lock()
	entry, exists := s.table[safe]
	s.mu.Unlock()

	if !exists {
		resp := &pb.DeleteResponse{Message: "File is already deleted"}

		s.mu.Lock()
		s.requests[req.RequestId] = &RequestEntry{
			response:  resp,
			timestamp: time.Now(),
		}
		s.mu.Unlock()

		return resp, nil
	}

	entry.mu.Lock()

	// Check if file is currently in use
	if entry.activeReaders > 0 || entry.activeWriter {
		entry.mu.Unlock()
		return nil, status.Errorf(
			codes.FailedPrecondition,
			"file is currently in use by another client",
		)
	}

	// Lock as writer
	entry.activeWriter = true
	entry.mu.Unlock()

	// Ensure release
	defer func() {
		entry.mu.Lock()
		entry.activeWriter = false
		entry.cond.Broadcast()
		entry.mu.Unlock()
	}()

	// Delete file
	if err := os.Remove(full); err != nil && !os.IsNotExist(err) {
		return nil, status.Error(codes.Internal, err.Error())
	}

	s.mu.Lock()
	// Cleanup metadata
	delete(s.table, safe)

	resp := &pb.DeleteResponse{Message: "File is deleted"}
	s.requests[req.RequestId] = &RequestEntry{
		response:  resp,
		timestamp: time.Now(),
	}
	s.mu.Unlock()

	return resp, nil
}

// Opens file
func (s *server) Open(ctx context.Context, req *pb.FileRequest) (*pb.OpenResponse, error) {

	// ---------- SAFETY: INIT MAPS ----------
	s.mu.Lock()
	if s.requests == nil {
		s.requests = make(map[string]*RequestEntry)
	}
	if s.files == nil {
		s.files = make(map[int32]*FileMeta)
	}
	if s.openMap == nil { // 🔥 ADD THIS
		s.openMap = make(map[FileKey]int32)
	}
	s.mu.Unlock()

	// ---------- CACHE CHECK ----------
	s.mu.Lock()
	if entry, ok := s.requests[req.RequestId]; ok &&
		time.Since(entry.timestamp) < RequestCacheTTL {
		resp := entry.response.(*pb.OpenResponse)
		s.mu.Unlock()
		return resp, nil
	}
	s.mu.Unlock()

	// ---------- SANITIZE PATH ----------
	safe, err := sanitizePath(req.Filename)
	if err != nil {
		return nil, err
	}

	// Normalize BEFORE using
	safe = filepath.ToSlash(safe)
	safe = strings.TrimSpace(safe)
	safe = strings.TrimPrefix(safe, "/")

	full := filepath.Join(s.rootDir, safe)

	entry := s.getFileEntry(safe)
	mode := FileMode(req.Mode)

	log.Println("Opening file:", full)

	// ---------- INPUT FILE ----------
	if strings.HasPrefix(safe, "input/") {

		if mode != ReadMode {
			return nil, status.Errorf(codes.PermissionDenied, "input files are read-only")
		}

		entry.AcquireReadNoPriority()

		file, err := os.OpenFile(full, os.O_RDONLY, 0666)
		if err != nil {
			entry.ReleaseRead()
			log.Println("ERROR opening input:", full, err)
			return nil, status.Errorf(codes.NotFound, "input file not found")
		}

		s.mu.Lock()
		defer s.mu.Unlock()

		if len(s.files) >= MaxOpenFiles {
			file.Close()
			entry.ReleaseRead()
			return nil, status.Errorf(codes.ResourceExhausted, "too many open files")
		}

		fd := s.nextFD
		s.nextFD++

		clientID := req.ClientId

		meta := &FileMeta{
			file:     file,
			filename: safe,
			version:  entry.version,
			clients:  make(map[string]ClientState),
		}

		meta.clients[clientID] = ClientState{
			lastSeen: time.Now(),
			mode:     mode,
		}

		s.files[fd] = meta

		key := FileKey{
			clientID: clientID,
			filename: safe,
		}
		s.openMap[key] = fd

		log.Println("OPEN STORE:",
			clientID,
			"||", safe, "||",
		)

		resp := &pb.OpenResponse{
			Fd:      fd,
			Version: entry.version,
			Message: "opened input file",
		}

		s.requests[req.RequestId] = &RequestEntry{
			response:  resp,
			timestamp: time.Now(),
		}

		return resp, nil
	}

	// ---------- OUTPUT FILE ----------
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
		log.Println("ERROR opening output:", full, err)
		return nil, status.Errorf(codes.NotFound, "output file not found")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.files) >= MaxOpenFiles {
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

	clientID := req.ClientId

	meta := &FileMeta{
		file:     file,
		filename: safe,
		version:  entry.version,
		clients:  make(map[string]ClientState),
	}

	meta.clients[clientID] = ClientState{
		lastSeen: time.Now(),
		mode:     mode,
	}

	s.files[fd] = meta

	key := FileKey{
		clientID: clientID,
		filename: safe,
	}
	s.openMap[key] = fd

	log.Println("OPEN STORE:",
		clientID,
		"||", safe, "||",
	)

	resp := &pb.OpenResponse{
		Fd:      fd,
		Version: entry.version,
		Message: "opened output file",
	}

	s.requests[req.RequestId] = &RequestEntry{
		response:  resp,
		timestamp: time.Now(),
	}

	return resp, nil
}

// Close + Write AFS Style
func (s *server) Close(stream pb.FileService_CloseServer) error {

	var meta *FileMeta
	var filename string
	var entry *FileEntry
	var tmpFile *os.File
	var reqID string
	var mode FileMode
	var dirty bool
	var clientID string
	var tempPrimeSet FilePrimeSet

	for {
		req, err := stream.Recv()
		// EOF = end of stream
		if err == io.EOF {
			break
		}
		if err != nil {
			return status.Errorf(codes.Internal, "recv failed")
		}

		// ======================
		// FIRST CHUNK INIT
		// ======================
		if meta == nil {

			reqID = req.RequestId
			dirty = req.Dirty

			// Cache check
			s.mu.Lock()
			if entryCache, ok := s.requests[req.RequestId]; ok &&
				time.Since(entryCache.timestamp) < RequestCacheTTL {
				resp := entryCache.response.(*pb.CloseResponse)
				s.mu.Unlock()
				return stream.SendAndClose(resp)
			}

			m, ok := s.files[req.Fd]
			if !ok {
				s.mu.Unlock()
				return status.Errorf(codes.NotFound, "file not open")
			}

			meta = m
			filename = meta.filename

			client, ok := meta.clients[clientID]
			if !ok {
				// Retry-safe (already closed case)

				if entryCache, ok := s.requests[req.RequestId]; ok &&
					time.Since(entryCache.timestamp) < RequestCacheTTL {
					resp := entryCache.response.(*pb.CloseResponse)
					s.mu.Unlock()
					return stream.SendAndClose(resp)
				}

				entry := s.getFileEntry(meta.filename)
				entry.mu.Lock()
				version := entry.version
				entry.mu.Unlock()

				resp := &pb.CloseResponse{
					Message: "already closed",
					Version: version,
				}

				s.requests[req.RequestId] = &RequestEntry{
					response:  resp,
					timestamp: time.Now(),
				}

				s.mu.Unlock()
				return stream.SendAndClose(resp)
			}

			mode = client.mode
			s.mu.Unlock()

			entry = s.getFileEntry(filename)

			// ======================
			// ONLY IF DIRTY -> WRITE FLOW
			// ======================
			if dirty {

				entry.AcquireWrite()

				// Validate
				if mode != WriteMode {
					entry.ReleaseWrite()
					return status.Errorf(codes.PermissionDenied, "not opened in write mode")
				}

				if req.Version != entry.version {
					entry.ReleaseWrite()
					return status.Errorf(codes.Aborted, "conflict")
				}

				// Create temp file
				full := filepath.Join(s.rootDir, filename)
				tmp := full + ".tmp"

				f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
				if err != nil {
					entry.ReleaseWrite()
					return status.Errorf(codes.Internal, "temp open failed")
				}

				tmpFile = f
			}
		}

		// ======================
		// PROCESS CHUNKS ONLY IF DIRTY
		// ======================
		if dirty {

			s.mu.Lock()
			primeSetPtr, ok := s.filesPrimes[filename]
			if !ok || primeSetPtr == nil { // primeset doesn't exist yet
				tempPrimeSet = make(FilePrimeSet)
			} else { // it does exist, make it of the required size
				tempPrimeSet = make(FilePrimeSet, len(*primeSetPtr))
				for k, v := range *primeSetPtr {
					tempPrimeSet[k] = v
				}
			}
			s.mu.Unlock()

			nums := parseNumbers(req.Data)
			unique := s.FilterUniquePrimes(nums, tempPrimeSet)

			for _, p := range unique {
				line := fmt.Sprintf("%d\n", p)
				if _, err := tmpFile.WriteString(line); err != nil {
					tmpFile.Close()
					entry.ReleaseWrite()
					return status.Errorf(codes.Internal, "write failed")
				}
			}
		}
	}

	var newVersion int32

	//only check if dirty
	if dirty && tmpFile == nil {
		return status.Errorf(codes.InvalidArgument, "no data received")
	}

	if dirty {
		tmpFile.Sync()
		tmpFile.Close()

		full := filepath.Join(s.rootDir, filename)
		tmp := full + ".tmp"

		if err := os.Rename(tmp, full); err != nil {
			entry.ReleaseWrite()
			return status.Errorf(codes.Internal, "rename failed")
		}
		s.mu.Lock() // commit tempprime to server file prime set
		*s.filesPrimes[filename] = tempPrimeSet
		s.mu.Unlock()
		dir, err := os.Open(s.rootDir) // sync dir
		if err == nil {
			dir.Sync()
			dir.Close()
			entry.version++ // update version
			newVersion = entry.version

			s.saveVersion(filename, newVersion)

			entry.ReleaseWrite()

		} else {
			entry.mu.Lock()
			newVersion = entry.version
			entry.mu.Unlock()
		}
	} else {
		// No write -> just return version
		entry.mu.Lock()
		newVersion = entry.version
		entry.mu.Unlock()
	}

	// ======================
	// CLOSE FILE HANDLE
	// ======================
	meta.file.Close()

	// ======================
	// CLEANUP FD
	// ======================
	s.mu.Lock()
	delete(meta.clients, clientID)
	s.mu.Unlock()

	// ======================
	// RESPONSE
	// ======================
	resp := &pb.CloseResponse{
		Message: "closed",
		Version: newVersion,
	}

	// Cache response
	s.mu.Lock()
	s.requests[reqID] = &RequestEntry{
		response:  resp,
		timestamp: time.Now(),
	}
	s.mu.Unlock()

	return stream.SendAndClose(resp)
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

		if !strings.HasSuffix(name, ".tmp") {
			continue
		}

		fullPath := filepath.Join(s.rootDir, name)

		info, err := file.Info()
		if err != nil {
			continue
		}

		// Skip recent files
		if now.Sub(info.ModTime()) < 2*time.Minute {
			continue
		}

		original := strings.TrimSuffix(name, ".tmp")

		entry := s.getFileEntry(original)

		entry.mu.Lock()
		inUse := entry.activeReaders > 0 || entry.activeWriter
		entry.mu.Unlock()

		if inUse {
			continue
		}

		// Safe delete
		if err := os.Remove(fullPath); err != nil {
			fmt.Println("cleanup failed:", err)
		}
	}
}

// Call this function on server for .tmp files clean up
func (s *server) startCleanupRoutine() {
	go func() {
		for {
			time.Sleep(1 * time.Minute)
			s.cleanupTempFiles()
		}
	}()
}

// Server side read
func (s *server) Read(req *pb.ReadRequest, stream pb.FileService_ReadServer) error {

	ctx := stream.Context()

	// Get client ID
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return status.Errorf(codes.Unauthenticated, "missing metadata")
	}
	clientIDs := md["client-id"]
	if len(clientIDs) == 0 {
		return status.Errorf(codes.Unauthenticated, "client-id missing")
	}
	clientID := clientIDs[0]

	// Sanitize path
	safe, err := sanitizePath(req.Filename)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid path")
	}

	safe = filepath.ToSlash(safe)
	safe = strings.TrimSpace(safe)
	safe = strings.TrimPrefix(safe, "/")

	// Build struct key
	key := FileKey{
		clientID: clientID,
		filename: safe,
	}

	log.Println("READ LOOKUP:",
		clientID,
		"||", safe, "||",
	)

	// Lookup FD using openMap
	s.mu.Lock()
	fd, ok := s.openMap[key]
	if !ok {
		s.mu.Unlock()
		return status.Errorf(codes.PermissionDenied, "file not opened by client")
	}

	meta, ok := s.files[fd]
	s.mu.Unlock()

	if !ok {
		return status.Errorf(codes.Internal, "file metadata missing")
	}

	client, ok := meta.clients[clientID]
	if !ok {
		return status.Errorf(codes.PermissionDenied, "client not registered")
	}

	mode := client.mode
	if mode != ReadMode && mode != WriteMode && mode != ReadWriteMode {
		return status.Errorf(codes.PermissionDenied, "read not allowed")
	}

	// Lock file meta
	meta.mu.Lock()
	defer meta.mu.Unlock()

	// Per-client lease check
	client, exists := meta.clients[clientID]
	if !exists {
		return status.Errorf(codes.PermissionDenied, "client not registered for this file")
	}

	if time.Since(client.lastSeen) > 10*time.Second {
		s.mu.Lock()
		delete(s.openMap, key)
		s.mu.Unlock()

		delete(meta.clients, clientID)

		return status.Errorf(codes.PermissionDenied, "lease expired")
	}

	// Update lease
	client.lastSeen = time.Now()
	meta.clients[clientID] = client

	file := meta.file

	// Reset pointer -> IMPORTANT for retry
	_, err = file.Seek(0, 0)
	if err != nil {
		return status.Errorf(codes.Internal, "seek failed")
	}

	buf := make([]byte, ChunkSize)

	for {
		n, err := file.Read(buf)

		if n > 0 {
			chunk := make([]byte, n)
			copy(chunk, buf[:n])

			err := stream.Send(&pb.ReadResponse{
				Data: chunk,
			})
			if err != nil {
				return status.Errorf(codes.Internal, "stream send failed")
			}
		}

		if err == io.EOF {
			break
		}
		if err != nil {
			return status.Errorf(codes.Internal, "read failed")
		}
	}

	return nil
}

func (s *server) Write(stream pb.FileService_WriteServer) error {

	var meta *FileMeta
	var filename string
	var entry *FileEntry
	var tmpFile *os.File
	var reqID string
	var mode FileMode
	var dirty bool
	var clientID string
	var tempPrimeSet FilePrimeSet

	for {
		req, err := stream.Recv()

		// EOF = end of stream
		if err == io.EOF {
			break
		}
		if err != nil {
			return status.Errorf(codes.Internal, "recv failed")
		}

		// ======================
		// FIRST CHUNK INIT
		// ======================
		if meta == nil {

			// Extract clientID once
			ctx := stream.Context()
			md, ok := metadata.FromIncomingContext(ctx)
			if !ok {
				return status.Errorf(codes.Unauthenticated, "missing metadata")
			}
			clientIDs := md["client-id"]
			if len(clientIDs) == 0 {
				return status.Errorf(codes.Unauthenticated, "client-id missing")
			}
			clientID = clientIDs[0]

			reqID = req.RequestId
			dirty = req.Dirty

			// Cache check
			s.mu.Lock()
			if entryCache, ok := s.requests[req.RequestId]; ok &&
				time.Since(entryCache.timestamp) < RequestCacheTTL {
				resp := entryCache.response.(*pb.WriteResponse)
				s.mu.Unlock()
				return stream.SendAndClose(resp)
			}

			m, ok := s.files[req.Fd]
			if !ok {
				s.mu.Unlock()
				return status.Errorf(codes.NotFound, "file not open")
			}

			meta = m
			filename = meta.filename
			client, ok := meta.clients[clientID]
			if !ok {
				s.mu.Unlock()
				return status.Errorf(codes.PermissionDenied, "client not registered")
			}

			mode = client.mode
			s.mu.Unlock()

			entry = s.getFileEntry(filename)

			// ======================
			// LEASE CHECK (NEW)
			// ======================
			meta.mu.Lock()

			client, exists := meta.clients[clientID]
			if !exists {
				return status.Errorf(codes.PermissionDenied, "client not registered for this file")
			}

			if time.Since(client.lastSeen) > 10*time.Second {
				key := FileKey{
					clientID: clientID,
					filename: meta.filename,
				}
				s.mu.Lock()
				delete(s.openMap, key)
				s.mu.Unlock()

				meta.mu.Lock()
				delete(meta.clients, clientID)
				meta.mu.Unlock()

				return status.Errorf(codes.PermissionDenied, "lease expired")
			}

			// Refresh lease
			client.lastSeen = time.Now()
			meta.clients[clientID] = client
			meta.mu.Unlock()

			// ======================
			// ONLY IF DIRTY -> WRITE FLOW
			// ======================
			if dirty {

				entry.AcquireWrite()

				// Validate
				if mode != WriteMode {
					entry.ReleaseWrite()
					return status.Errorf(codes.PermissionDenied, "not opened in write mode")
				}

				if req.Version != entry.version {
					entry.ReleaseWrite()
					return status.Errorf(codes.Aborted, "conflict")
				}

				// Create temp file
				full := filepath.Join(s.rootDir, filename)
				tmp := full + ".tmp"

				f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
				if err != nil {
					entry.ReleaseWrite()
					return status.Errorf(codes.Internal, "temp open failed")
				}

				tmpFile = f
			}
		}

		// ======================
		// PROCESS CHUNK ONLY IF DIRTY
		// ======================
		if dirty {

			// Refresh lease per chunk
			meta.mu.Lock()
			client, ok := meta.clients[clientID]
			if ok {
				client.lastSeen = time.Now()
				meta.clients[clientID] = client
			}
			meta.mu.Unlock()
			s.mu.Lock()
			primeSetPtr, ok := s.filesPrimes[filename]
			if !ok || primeSetPtr == nil {
				tempPrimeSet = make(FilePrimeSet)
			} else {
				tempPrimeSet = make(FilePrimeSet, len(*primeSetPtr))
				for k, v := range *primeSetPtr {
					tempPrimeSet[k] = v
				}
			}
			s.mu.Unlock()

			nums := parseNumbers(req.Data)
			unique := s.FilterUniquePrimes(nums, tempPrimeSet)

			for _, p := range unique {
				line := fmt.Sprintf("%d\n", p)
				if _, err := tmpFile.WriteString(line); err != nil {
					tmpFile.Close()
					entry.ReleaseWrite()
					return status.Errorf(codes.Internal, "write failed")
				}
			}
		}
	}

	// ======================
	// FINALIZE ONLY IF DIRTY
	// ======================
	var newVersion int32

	if dirty {

		if tmpFile == nil {
			return status.Errorf(codes.InvalidArgument, "no data received")
		}

		tmpFile.Sync()
		tmpFile.Close()

		full := filepath.Join(s.rootDir, filename)
		tmp := full + ".tmp"

		if err := os.Rename(tmp, full); err != nil {
			entry.ReleaseWrite()
			return status.Errorf(codes.Internal, "rename failed")
		}
		// Commit tempPrimeSet -> meta.primeSet
		s.mu.Lock()
		*s.filesPrimes[filename] = tempPrimeSet
		s.mu.Unlock()

		// Sync directory
		dir, err := os.Open(s.rootDir)
		if err == nil {
			dir.Sync()
			dir.Close()
		}

		// Update version
		entry.version++
		newVersion = entry.version
		s.saveVersion(filename, newVersion)

		entry.ReleaseWrite()

	} else {
		// No write -> just return current version
		entry.mu.Lock()
		newVersion = entry.version
		entry.mu.Unlock()
	}

	// ======================
	// FINAL RESPONSE
	// ======================
	resp := &pb.WriteResponse{
		Message: "write successful",
		Version: newVersion,
	}

	// Cache response
	s.mu.Lock()
	s.requests[reqID] = &RequestEntry{
		response:  resp,
		timestamp: time.Now(),
	}
	s.mu.Unlock()

	return stream.SendAndClose(resp)
}

// respond to client who is checking if the version in their cache is the same as the latest on the server
func (s *server) TestAuth(ctx context.Context, req *pb.TestAuthRequest) (*pb.TestAuthResponse, error) {
	safe, err := sanitizePath(req.Filename)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid path")
	} // doesn't look at cache, doesn't need to, not a big overhead
	entry, ok := s.table[safe]
	if !ok {
		return nil, status.Error(codes.NotFound, "file not found")
	}
	entry.mu.Lock()                                    // acquire lock on file entry
	version := entry.version                           // check version
	entry.mu.Unlock()                                  // release lock
	return &pb.TestAuthResponse{Version: version}, nil // just returning version in response
}
