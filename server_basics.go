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
	mode     pb.FileMode
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
		entry.cond = sync.NewCond(&entry.mu) //////
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
			mode     pb.FileMode
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

			if e.mode == pb.FileMode_READ {
				entry.ReleaseRead()
			} else if e.mode == pb.FileMode_WRITE || e.mode == pb.FileMode(2) {
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
	if entry.cond == nil {
		entry.cond = sync.NewCond(&entry.mu)
	}
	return entry
}

func (s *server) Create(ctx context.Context, req *pb.CreateRequest) (*pb.OpenResponse, error) {
	// Request cache
	if s.role != Primary {
		if s.primaryID == "" {
			return nil, status.Errorf(codes.Unavailable, "no leader elected yet")
		}
		leader, ok := s.servers[s.primaryID]
		if !ok {
			return nil, status.Errorf(codes.Internal, "leader info missing")
		}
		return nil, status.Errorf(codes.FailedPrecondition, "not leader: %s", leader.Address)
	}
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
	safe = filepath.ToSlash(safe)
	safe = strings.TrimSpace(safe)
	safe = strings.TrimPrefix(safe, "/")

	// Only output/
	if !(strings.HasPrefix(safe, "output/") || strings.HasPrefix(safe, "output\\")) {
		// log.Println("file path after sanitization", safe) ////////
		return nil, status.Errorf(codes.PermissionDenied, "only output/")
	}

	full := filepath.Join(s.rootDir, safe)
	entry := s.getFileEntry(safe)

	entry.AcquireWrite()
	defer entry.ReleaseWrite()

	// Create directory -- Ensure all parent dirctory exists
	if err := os.MkdirAll(filepath.Dir(full), 0755); err != nil {
		// entry.ReleaseWrite()
		return nil, err
	}

	// Create file
	file, err := os.OpenFile(full, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0666)
	if err != nil {
		// entry.ReleaseWrite()
		if os.IsExist(err) {
			return nil, status.Errorf(codes.AlreadyExists, "file exists")
		}
		return nil, err
	}

	// Register file
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.files) >= MaxOpenFiles {
		s.mu.Unlock()
		file.Close()
		os.Remove(full)
		// entry.ReleaseWrite()
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
		mode:     pb.FileMode(req.Mode),
	}

	s.files[fd] = meta

	key := FileKey{
		clientID: clientID,
		filename: safe,
	}
	s.openMap[key] = fd

	resp := &pb.OpenResponse{
		Fd:      fd,
		Version: currentVersion,
		Message: "file created",
	}

	s.requests[req.RequestId] = &RequestEntry{
		response:  resp,
		timestamp: time.Now(),
	}

	return resp, nil
}

func (s *server) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	// Request cache
	if s.role != Primary {
		if s.primaryID == "" {
			return nil, status.Errorf(codes.Unavailable, "no leader elected yet")
		}
		leader, ok := s.servers[s.primaryID]
		if !ok {
			return nil, status.Errorf(codes.Internal, "leader info missing")
		}
		return nil, status.Errorf(codes.FailedPrecondition, "not leader: %s", leader.Address)
	}
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
	// s.mu.Lock()
	// if s.requests == nil {
	// 	s.requests = make(map[string]*RequestEntry)
	// }
	// if s.files == nil {
	// 	s.files = make(map[int32]*FileMeta)
	// }
	// if s.openMap == nil { // ADD THIS
	// 	s.openMap = make(map[FileKey]int32)
	// }
	// s.mu.Unlock()
	if s.role != Primary {
		if s.primaryID == "" {
			return nil, status.Errorf(codes.Unavailable, "no leader elected yet")
		}
		leader, ok := s.servers[s.primaryID]
		if !ok {
			return nil, status.Errorf(codes.Internal, "leader info missing")
		}
		return nil, status.Errorf(codes.FailedPrecondition, "not leader: %s", leader.Address)
	}
	s.mu.Lock() // cache check
	if entry, ok := s.requests[req.RequestId]; ok &&
		time.Since(entry.timestamp) < RequestCacheTTL {
		resp := entry.response.(*pb.OpenResponse)
		s.mu.Unlock()
		return resp, nil
	}
	s.mu.Unlock()

	safe, err := sanitizePath(req.Filename) // sanitize path
	if err != nil {
		return nil, err
	}
	safe = filepath.ToSlash(safe)
	safe = strings.TrimSpace(safe)
	safe = strings.TrimPrefix(safe, "/")

	full := filepath.Join(s.rootDir, safe)

	entry := s.getFileEntry(safe)
	mode := pb.FileMode(req.Mode)

	log.Println("Opening file:", full)

	// ---------- INPUT FILE ----------
	if strings.HasPrefix(safe, "input/") {

		if mode != pb.FileMode_READ {
			return nil, status.Errorf(codes.PermissionDenied, "input files are read-only")
		}

		entry.AcquireReadNoPriority()
		defer entry.ReleaseRead()

		file, err := os.OpenFile(full, os.O_RDONLY, 0666)
		if err != nil {
			// entry.ReleaseRead()
			log.Println("ERROR opening input:", full, err)
			return nil, status.Errorf(codes.NotFound, "input file not found")
		}

		s.mu.Lock()
		defer s.mu.Unlock()

		if len(s.files) >= MaxOpenFiles {
			file.Close()
			// entry.ReleaseRead()
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
	// if mode == pb.FileMode_READ {
	// 	entry.AcquireRead()

	// } else {
	// 	entry.AcquireWrite()
	// }

	flags := os.O_RDONLY
	if mode == pb.FileMode_WRITE {
		flags = os.O_RDWR
	}

	file, err := os.OpenFile(full, flags, 0666)
	if err != nil {
		// if mode == pb.FileMode_READ {
		// 	entry.ReleaseRead()
		// } else {
		// 	entry.ReleaseWrite()
		// }
		log.Println("ERROR opening output:", full, err)
		return nil, status.Errorf(codes.NotFound, "output file not found")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.files) >= MaxOpenFiles {
		file.Close()
		// if mode == pb.FileMode_READ {
		// 	entry.ReleaseRead()
		// } else {
		// 	entry.ReleaseWrite()
		// }
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

func getClientIDFromContext(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}

	ids := md["clientid"] // this is []string
	if len(ids) == 0 {
		return ""
	}

	return ids[0] // ✅ actual string
}

// Close + Write AFS Style
func (s *server) Close(ctx context.Context, req *pb.CloseRequest) (*pb.CloseResponse, error) {
	// reached server, sanity check
	if s.role != Primary {
		if s.primaryID == "" {
			return nil, status.Errorf(codes.Unavailable, "no leader elected yet")
		}
		leader, ok := s.servers[s.primaryID]
		if !ok {
			return nil, status.Errorf(codes.Internal, "leader info missing")
		}
		return nil, status.Errorf(codes.FailedPrecondition, "not leader: %s", leader.Address)
	}
	var meta *FileMeta
	var filename string
	var entry *FileEntry
	var tmpFile *os.File
	var mode pb.FileMode
	var dirty bool
	var tempPrimeSet FilePrimeSet
	var logEntry LogEntry

	// Get clientID
	// clientID, _ := metadata.FromIncomingContext(ctx)
	clientID := getClientIDFromContext(ctx)

	log.Println("which clientid can server see", clientID)

	reqID := req.RequestId
	dirty = req.Dirty

	// ======================
	// INIT
	// ======================
	s.mu.Lock()

	if entryCache, ok := s.requests[reqID]; ok &&
		time.Since(entryCache.timestamp) < RequestCacheTTL {
		resp := entryCache.response.(*pb.CloseResponse)
		s.mu.Unlock()
		return resp, nil
	}
	m, ok := s.files[req.Fd]
	if !ok {
		s.mu.Unlock()
		return nil, status.Errorf(codes.NotFound, "file not open")
	}

	meta = m
	filename = meta.filename
	client, ok := meta.clients[clientID]
	if !ok {
		entry := s.getFileEntry(filename)
		entry.mu.Lock()
		version := entry.version
		entry.mu.Unlock()

		resp := &pb.CloseResponse{
			Message: "already closed",
			Version: version,
		}

		s.requests[reqID] = &RequestEntry{
			response:  resp,
			timestamp: time.Now(),
		}

		s.mu.Unlock()
		return resp, nil
	}

	mode = client.mode
	s.mu.Unlock()

	entry = s.getFileEntry(filename)

	// ======================
	// WRITE FLOW
	// ======================
	if dirty {
		// log.Println("file is open and dirty") ///
		entry.AcquireWrite()
		// log.Println("acquired write") ///
		defer entry.ReleaseWrite()

		if mode != pb.FileMode_WRITE {
			return nil, status.Errorf(codes.PermissionDenied, "not opened in write mode")
		}
		// log.Printf("1") ///
		if req.Version != entry.version {
			return nil, status.Errorf(codes.Aborted, "conflict")
		}

		///////////////////////////////////////////////////////////////////////////////////////

		logEntry := LogEntry{
			Index:    len(s.log) + 1,
			Op:       "WRITE",
			Filename: filename,
			Content:  req.Data, // full client data
			Version:  entry.version + 1,
		}

		s.mu.Lock()
		s.log = append(s.log, logEntry)
		err := s.appendToDisk(logEntry)
		s.mu.Unlock()

		if err != nil {
			return nil, status.Errorf(codes.Internal, "log persist failed")
		}
		ackCount := 1
		commitIndex := logEntry.Index

		for _, peer := range s.servers {
			if peer.ID == s.id {
				continue
			}

			if sendAppendEntry(s.id, peer, logEntry, commitIndex) {
				ackCount++
			}
		}

		if ackCount < (len(s.servers)/2 + 1) {
			return nil, status.Errorf(codes.Unavailable, "failed to reach majority")
		}

		/////////////////////////////////////////////////////////////////////////////////////
		full := filepath.Join(s.rootDir, filename)
		tmp := full + ".tmp"
		// log.Printf("2") ///
		f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "temp open failed")
		}

		tmpFile = f
		// log.Printf("4") ///

		s.mu.Lock()
		if s.filesPrimes == nil {
			s.filesPrimes = make(map[string]*FilePrimeSet)
		}
		s.mu.Unlock()
		// Load prime set
		s.mu.Lock()
		primeSetPtr, ok := s.filesPrimes[filename]
		if !ok || primeSetPtr == nil {
			tempPrimeSet = make(FilePrimeSet)
		} else {
			tempPrimeSet = *primeSetPtr
		}
		// log.Printf("3") ///
		s.mu.Unlock()

		log.Println("Processing size:", len(req.Data))

		s.mu.Lock()
		nums := parseNumbers(req.Data)
		s.FilterUniquePrimes(nums, tempPrimeSet)
		s.mu.Unlock()

		for p := range tempPrimeSet {
			line := fmt.Sprintf("%d\n", p)
			if _, err := tmpFile.WriteString(line); err != nil {
				tmpFile.Close()
				return nil, status.Errorf(codes.Internal, "write failed")
			}
		}

		tmpFile.Sync()

		// meta.file.Close() // moved this, got pointer error
		// ======================
		// COPY TMP → ORIGINAL
		// ======================
		tmpFile.Close()

		src, err := os.Open(tmp)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to open tmp file: %v", err)
		}

		dst, err := os.OpenFile(full, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			src.Close()
			return nil, status.Errorf(codes.Internal, "failed to open destination: %v", err)
		}

		_, err = io.Copy(dst, src)
		if err != nil {
			src.Close()
			dst.Close()
			return nil, status.Errorf(codes.Internal, "copy failed: %v", err)
		}

		err = dst.Sync()
		if err != nil {
			src.Close()
			dst.Close()
			return nil, status.Errorf(codes.Internal, "sync failed: %v", err)
		}

		// ✅ CLOSE BEFORE DELETE
		src.Close()
		dst.Close()

		// ✅ NOW DELETE
		err = os.Remove(tmp)
		if err != nil {
			log.Println("warning: failed to remove tmp file:", err)
		}

		s.mu.Lock()
		if ptr, ok := s.filesPrimes[filename]; ok && ptr != nil {
			*ptr = tempPrimeSet
		} else {
			// initialize if missing
			newSet := make(FilePrimeSet)
			for k, v := range tempPrimeSet {
				newSet[k] = v
			}
			s.filesPrimes[filename] = &newSet
		}
		s.mu.Unlock()
	}

	// ======================
	// VERSION
	// ======================
	var newVersion int32

	entry.mu.Lock()
	if dirty {
		entry.version++
		s.saveVersion(filename, entry.version)
		// creates .meta file to store current version file
	}
	newVersion = entry.version
	entry.mu.Unlock()

	s.mu.Lock()
	s.commitIndex = logEntry.Index
	s.applyCommitted()
	s.mu.Unlock()
	// ======================
	// CLEANUP
	// ======================
	// meta.file.Close()
	log.Println("DEBUG meta:", meta)
	if meta == nil {
		log.Println(" meta is NIL")
	}
	if meta != nil && meta.clients == nil {
		log.Println("meta.clients is NIL")
	}

	s.mu.Lock()
	if meta != nil && meta.clients != nil {
		delete(meta.clients, clientID)
	} else {
		log.Println("skip delete: meta or clients nil")
	}
	s.mu.Unlock()

	resp := &pb.CloseResponse{
		Message: "closed",
		Version: newVersion,
	}

	s.mu.Lock()
	s.requests[reqID] = &RequestEntry{
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
	if s.role != Primary {
		if s.primaryID == "" {
			return status.Errorf(codes.Unavailable, "no leader elected yet")
		}
		leader, ok := s.servers[s.primaryID]
		if !ok {
			return status.Errorf(codes.Internal, "leader info missing")
		}
		return status.Errorf(codes.FailedPrecondition, "not leader: %s", leader.Address)
	}
	ctx := stream.Context()

	// Get client ID
	// md, ok := metadata.FromIncomingContext(ctx)
	// if !ok {
	// 	return status.Errorf(codes.Unauthenticated, "missing metadata")
	// }
	// clientIDs := md["client-id"]
	// if len(clientIDs) == 0 {
	// 	return status.Errorf(codes.Unauthenticated, "client-id missing")
	// }
	// clientID := clientIDs[0]
	clientID := getClientIDFromContext(ctx)

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
	if mode != pb.FileMode_READ && mode != pb.FileMode_WRITE && mode != pb.FileMode(2) {
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

func (s *server) Write(ctx context.Context, req *pb.WriteRequest) (*pb.WriteResponse, error) {
	if s.role != Primary {
		if s.primaryID == "" {
			return nil, status.Errorf(codes.Unavailable, "no leader elected yet")
		}
		leader, ok := s.servers[s.primaryID]
		if !ok {
			return nil, status.Errorf(codes.Internal, "leader info missing")
		}
		return nil, status.Errorf(codes.FailedPrecondition, "not leader: %s", leader.Address)
	}
	var meta *FileMeta
	var filename string
	var entry *FileEntry
	var tmpFile *os.File
	var mode pb.FileMode
	var dirty bool
	var tempPrimeSet FilePrimeSet
	var logEntry LogEntry

	clientID := getClientIDFromContext(ctx)

	reqID := req.RequestId
	dirty = req.Dirty
	fullData := req.Data

	// ======================
	// INIT (same as Close)
	// ======================
	s.mu.Lock()

	if entryCache, ok := s.requests[reqID]; ok &&
		time.Since(entryCache.timestamp) < RequestCacheTTL {
		resp := entryCache.response.(*pb.WriteResponse)
		s.mu.Unlock()
		return resp, nil
	}

	m, ok := s.files[req.Fd]
	if !ok {
		s.mu.Unlock()
		return nil, status.Errorf(codes.NotFound, "file not open")
	}

	meta = m
	filename = meta.filename

	client, ok := meta.clients[clientID]
	if !ok {
		entry := s.getFileEntry(filename)
		entry.mu.Lock()
		version := entry.version
		entry.mu.Unlock()

		resp := &pb.WriteResponse{
			Message: "already closed",
			Version: version,
		}

		s.requests[reqID] = &RequestEntry{
			response:  resp,
			timestamp: time.Now(),
		}

		s.mu.Unlock()
		return resp, nil
	}

	mode = client.mode
	s.mu.Unlock()

	entry = s.getFileEntry(filename)

	// ======================
	// WRITE FLOW (copied)
	// ======================
	if dirty {

		entry.AcquireWrite()
		defer entry.ReleaseWrite()

		if mode != pb.FileMode_WRITE {
			return nil, status.Errorf(codes.PermissionDenied, "not opened in write mode")
		}

		if req.Version != entry.version {
			return nil, status.Errorf(codes.Aborted, "conflict")
		}

		logEntry = LogEntry{
			Index:    len(s.log) + 1,
			Op:       "WRITE",
			Filename: filename,
			Content:  fullData,
			Version:  entry.version + 1,
		}

		s.mu.Lock()
		s.log = append(s.log, logEntry)
		err := s.appendToDisk(logEntry)
		s.mu.Unlock()

		if err != nil {
			return nil, status.Errorf(codes.Internal, "log persist failed")
		}

		ackCount := 1
		commitIndex := logEntry.Index

		for _, peer := range s.servers {
			if peer.ID == s.id {
				continue
			}

			if sendAppendEntry(s.id, peer, logEntry, commitIndex) {
				ackCount++
			}
		}

		if ackCount < (len(s.servers)/2 + 1) {
			return nil, status.Errorf(codes.Unavailable, "failed to reach majority")
		}

		full := filepath.Join(s.rootDir, filename)
		tmp := full + ".tmp"

		f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "temp open failed")
		}

		tmpFile = f

		// prime set logic (same)
		s.mu.Lock()
		if s.filesPrimes == nil {
			s.filesPrimes = make(map[string]*FilePrimeSet)
		}
		s.mu.Unlock()

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

		nums := parseNumbers(fullData)
		unique := s.FilterUniquePrimes(nums, tempPrimeSet)

		for _, p := range unique {
			line := fmt.Sprintf("%d\n", p)
			if _, err := tmpFile.WriteString(line); err != nil {
				tmpFile.Close()
				return nil, status.Errorf(codes.Internal, "write failed")
			}
		}

		tmpFile.Sync()
		tmpFile.Close()

		// ===== COPY (same as Close) =====
		src, err := os.Open(tmp)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to open tmp file: %v", err)
		}

		dst, err := os.OpenFile(full, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			src.Close()
			return nil, status.Errorf(codes.Internal, "failed to open destination: %v", err)
		}

		_, err = io.Copy(dst, src)
		if err != nil {
			src.Close()
			dst.Close()
			return nil, status.Errorf(codes.Internal, "copy failed: %v", err)
		}

		err = dst.Sync()
		if err != nil {
			src.Close()
			dst.Close()
			return nil, status.Errorf(codes.Internal, "sync failed: %v", err)
		}

		src.Close()
		dst.Close()

		err = os.Remove(tmp)
		if err != nil {
			log.Println("warning: failed to remove tmp file:", err)
		}

		// update prime set
		s.mu.Lock()
		if ptr, ok := s.filesPrimes[filename]; ok && ptr != nil {
			*ptr = tempPrimeSet
		} else {
			newSet := make(FilePrimeSet)
			for k, v := range tempPrimeSet {
				newSet[k] = v
			}
			s.filesPrimes[filename] = &newSet
		}
		s.mu.Unlock()
	}

	// ======================
	// VERSION + APPLY
	// ======================
	var newVersion int32

	entry.mu.Lock()
	if dirty {
		entry.version++
		s.saveVersion(filename, entry.version)
	}
	newVersion = entry.version
	entry.mu.Unlock()

	s.mu.Lock()
	s.commitIndex = logEntry.Index
	s.applyCommitted()
	s.mu.Unlock()

	// ======================
	// RESPONSE
	// ======================
	resp := &pb.WriteResponse{
		Message: "write successful",
		Version: newVersion,
	}

	s.mu.Lock()
	s.requests[reqID] = &RequestEntry{
		response:  resp,
		timestamp: time.Now(),
	}
	s.mu.Unlock()

	return resp, nil
}

// respond to client who is checking if the version in their cache is the same as the latest on the server
func (s *server) TestAuth(ctx context.Context, req *pb.TestAuthRequest) (*pb.TestAuthResponse, error) {
	if s.role != Primary {
		if s.primaryID == "" {
			return nil, status.Errorf(codes.Unavailable, "no leader elected yet")
		}
		leader, ok := s.servers[s.primaryID]
		if !ok {
			return nil, status.Errorf(codes.Internal, "leader info missing")
		}
		return nil, status.Errorf(codes.FailedPrecondition, "not leader: %s", leader.Address)
	}
	safe, err := sanitizePath(req.Filename)
	safe = filepath.ToSlash(safe)
	safe = strings.TrimSpace(safe)
	safe = strings.TrimPrefix(safe, "/")
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
