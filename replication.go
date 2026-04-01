package main

import (
	"bufio"
	"bytes"
	"context"
	pb "distributed-system-ikkat/filesystem"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// File Meta is same for all server
// primeSet is local cache of each server

type Replication struct {
	Filename string
	Version  int32
	Content  []byte //zFor recovery --- data of file
}

// Used for communication, server election
type ServerInfo struct {
	ID        string
	Address   string //localhost::8080
	Timestamp int64
	Alive     bool
}

// Used for recovery, consistency
type PersistentState struct {
	log []LogEntry
}

type LogEntry struct {
	Index    int    //Position in log
	Op       string // "WRITE", "DELETE"
	Filename string
	Content  []byte //This is data for operation
	Version  int32
}

type Heartbeat struct {
	PrimaryID string
	Timestamp int64
}

type Role int

const (
	Primary Role = iota
	Backup
)

type server1 struct {
	mu sync.Mutex

	id        string
	role      Role
	primaryID string
	// cluster info
	servers map[string]ServerInfo
	// file system (runtime)
	files  map[int32]*FileMeta
	table  map[string]*FileEntry
	nextFD int32
	// request cache (idempotency)
	requests map[string]*RequestEntry
	// lookup
	openMap map[FileKey]int32
	rootDir string
	// replication
	log         []LogEntry
	commitIndex int
	lastApplied int
	// failure detection
	lastHeartbeat time.Time
	// persistence
	logFilePath string
}

// for recovery only
// UpdateMessage is used for recovery and synchronization of out-of-date replicas, while normal replication is handled using log-based AppendEntries with majority acknowledgment
type UpdateMessage struct {
	IsFullSync bool
	LogEntries []LogEntry    // incremental sync
	FullFiles  []Replication // full snapshot
}

// Leader StartHeartbeat()
// -> every 2 sec
// -> send heartbeat to all followers
// Follower Receive heartbeat
// -> update lastHeartbeat
// Failure
// Receive heartbeat
// -> update lastHeartbeat
// Periodic Heartbeat message to backup server by leader

// Folower side
// revival works only if: recovered server receives heartbeat from current leader
func (s *server1) SendHeartbeat(
	ctx context.Context,
	req *pb.Heartbeat,
) (*pb.HeartbeatResponse, error) {

	s.mu.Lock()
	defer s.mu.Unlock()

	srv, exists := s.servers[req.PrimaryId]

	if !exists {
		s.servers[req.PrimaryId] = ServerInfo{
			ID:        req.PrimaryId,
			Timestamp: req.Timestamp,
			Alive:     true,
		}
	} else {
		srv.Alive = true
		s.servers[req.PrimaryId] = srv
	}

	//Correct leader comparison
	if s.primaryID != "" {
		current := s.servers[s.primaryID]
		incoming := s.servers[req.PrimaryId]

		if incoming.Timestamp < current.Timestamp {
			return &pb.HeartbeatResponse{Success: false}, nil
		}
	}

	s.primaryID = req.PrimaryId
	s.lastHeartbeat = time.Now()

	return &pb.HeartbeatResponse{Success: true}, nil
}

// Leader -> Follower
func (s *server1) sendHeartbeat(peer ServerInfo) {

	conn, _ := grpc.Dial(peer.Address, grpc.WithInsecure())
	defer conn.Close()

	client := pb.NewHeartbeatServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	resp, err := client.SendHeartbeat(ctx, &pb.Heartbeat{
		PrimaryId: s.id,
		Timestamp: time.Now().Unix(),
	})

	if err != nil {
		return
	}

	//  If rejected step down
	// Leader must step down if follower rejects
	if !resp.Success {
		fmt.Println("Another leader exists, stepping down")

		s.mu.Lock()
		s.role = Backup
		s.mu.Unlock()
	}
}
func (s *server1) StartHeartbeat() {

	for {
		time.Sleep(2 * time.Second)

		s.mu.Lock()
		isLeader := s.id == s.primaryID
		s.mu.Unlock()

		if !isLeader {
			continue
		}

		for _, srv := range s.servers {
			if srv.ID == s.id {
				continue
			}

			// No goroutine explosion (simple + safe)
			s.sendHeartbeat(srv)
		}
	}
}

// If Hearbeat stops
func (s *server1) MonitorPrimary() {

	for {
		time.Sleep(3 * time.Second)

		s.mu.Lock()

		// If I am leader -> skip
		if s.id == s.primaryID {
			s.mu.Unlock()
			continue
		}

		// Check timeout
		if time.Since(s.lastHeartbeat) > 5*time.Second {

			fmt.Println("Primary failed -> electing new leader")

			oldLeader := s.primaryID

			srv := s.servers[oldLeader]
			srv.Alive = false
			s.servers[oldLeader] = srv

			newLeader := electPrimary(s.servers)
			s.primaryID = newLeader

			if s.id == newLeader {
				fmt.Println("I am new primary")
			}
		}

		s.mu.Unlock()
	}
}

// Backup keep tack of var lastHeartbeat time.Time
func (s *server1) OnHeartbeat(hb Heartbeat) {
	s.primaryID = hb.PrimaryID
	s.lastHeartbeat = time.Now()
}

// Elect Primary
func electPrimary(servers map[string]ServerInfo) string {
	min := int64(math.MaxInt64)
	var leader string

	for _, s := range servers {
		if !s.Alive {
			continue
		}

		if s.Timestamp < min {
			min = s.Timestamp
			leader = s.ID
		}
	}
	return leader
}

// Checking primary is alive
func (s *server1) CheckPrimaryAlive() {

	s.mu.Lock()
	defer s.mu.Unlock()

	// If I am leader -> nothing to do
	if s.id == s.primaryID {
		return
	}

	// If heartbeat still fresh -> leader alive
	if time.Since(s.lastHeartbeat) <= 5*time.Second {
		return
	}

	log.Println("Primary failed. Electing new leader...")

	// Mark old leader as dead
	oldLeader := s.primaryID
	if srv, ok := s.servers[oldLeader]; ok {
		srv.Alive = false
		s.servers[oldLeader] = srv
	}

	// Elect new leader ONLY among alive servers
	newLeader := electPrimary(s.servers)

	if newLeader == "" {
		log.Println("No alive servers available")
		return
	}

	s.primaryID = newLeader

	if s.id == newLeader {
		s.role = Primary
		log.Println("I am the new primary")
	} else {
		s.role = Backup
	}
}

// Appending logs in log file
// Logs will be in json format
// {"Index":1,"Op":"WRITE","Filename":"file.txt","Content":"...","Version":1}
func (s *server1) appendToDisk(entry LogEntry) error {

	f, err := os.OpenFile(s.logFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}

	_, err = f.Write(append(data, '\n'))
	if err != nil {
		return err
	}

	// Ensure durability
	return f.Sync()
}

// Recover from logs
func (s *server1) recoverFromLog() error {

	f, err := os.Open(s.logFilePath)
	if err != nil {
		return err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)

	for scanner.Scan() {
		var entry LogEntry

		err := json.Unmarshal(scanner.Bytes(), &entry)
		if err != nil {
			continue
		}

		s.log = append(s.log, entry)
		s.apply(entry) // rebuild state
	}

	return scanner.Err()
}

// RebuildPrimeSet reconstructs the derived primeSet from file contents after recovery or replication.
func (s *server1) RebuildPrimeSet(meta *FileMeta) error {

	filePath := filepath.Join(s.rootDir, meta.filename)

	data, err := os.ReadFile(filePath)
	if err != nil {
		return err
	}

	primes := parseNumbers(data)

	newSet := make(map[uint64]bool)
	for _, p := range primes {
		newSet[p] = true
	}

	meta.mu.Lock()
	defer meta.mu.Unlock()

	meta.primeSet = newSet

	return nil
}

func (s *server1) getOrCreateFileMeta(filename string) *FileMeta {
	if entry, ok := s.table[filename]; ok {
		if meta, exists := s.files[entry.FD]; exists {
			return meta
		}
	}

	fd := s.nextFD
	s.nextFD++

	fullPath := filepath.Join(s.rootDir, filename)
	file, _ := os.OpenFile(fullPath, os.O_CREATE|os.O_RDWR, 0644)

	meta := &FileMeta{
		file:     file,
		filename: filename,
		version:  0,
		clients:  make(map[string]ClientState),
		primeSet: make(map[uint64]bool),
	}

	s.files[fd] = meta
	s.table[filename] = &FileEntry{
		FD:      fd,
		version: 0,
	}

	return meta
}

// This is called when log entry is commited
func (s *server1) apply(entry LogEntry) {

	fm := s.getOrCreateFileMeta(entry.Filename)

	fm.mu.Lock()
	defer fm.mu.Unlock()

	path := filepath.Join(s.rootDir, entry.Filename)

	switch entry.Op {

	case "WRITE":
		err := os.WriteFile(path, entry.Content, 0644)
		if err != nil {
			log.Println("apply write failed:", err)
			return
		}

	case "DELETE":
		err := os.Remove(path)
		if err != nil && !os.IsNotExist(err) {
			log.Println("apply delete failed:", err)
			return
		}
	}

	// Update version AFTER success
	fm.version = entry.Version

	// 🔁 Rebuild derived state
	s.RebuildPrimeSet(fm)
}

// Used when : Follower is behind OR restarted
// missing log , full state
// Recovery only
func (s *server1) ApplyUpdate(msg *pb.UpdateMessage) error {

	s.mu.Lock()

	if msg.IsFullSync {

		s.log = nil
		s.commitIndex = 0
		s.lastApplied = 0

		for _, f := range msg.FullFiles {

			path := filepath.Join(s.rootDir, f.Filename)

			err := os.WriteFile(path, f.Content, 0644)
			if err != nil {
				s.mu.Unlock()
				return err
			}

			meta := s.getOrCreateFileMeta(f.Filename)
			meta.version = f.Version

			// unlock before expensive rebuild
			s.mu.Unlock()
			s.RebuildPrimeSet(meta)
			s.mu.Lock()
		}

		s.mu.Unlock()
		return nil
	}

	// Incremental logs
	for _, e := range msg.LogEntries {

		entry := LogEntry{
			Index:    int(e.Index),
			Op:       e.Op,
			Filename: e.Filename,
			Content:  e.Content,
			Version:  e.Version,
		}

		if entry.Index <= len(s.log) {
			continue
		}

		if entry.Index != len(s.log)+1 {
			s.mu.Unlock()
			return fmt.Errorf("log gap")
		}

		s.log = append(s.log, entry)

		err := s.appendToDisk(entry)
		if err != nil {
			log.Println("persist failed:", err)
		}
	}

	s.mu.Unlock()
	return nil
}

// Called when leader sends a log entry
// Leader -> Follower: "store this operation"
// Follower: appends to log persists waits for commit
// AppendEntry -> append + persist ONLY
// Apply -> only after commit
// Recovery -> separate flow
func (s *server1) AppendEntries(
	ctx context.Context,
	req *pb.AppendEntriesRequest,
) (*pb.AppendEntriesResponse, error) {

	s.mu.Lock()
	defer s.mu.Unlock()

	// 1. Append entries
	for _, e := range req.Entries {

		entry := LogEntry{
			Index:    int(e.Index),
			Op:       e.Op,
			Filename: e.Filename,
			Content:  e.Content,
			Version:  e.Version,
		}

		// Skip duplicates
		if entry.Index <= len(s.log) {
			continue
		}

		// Optional: detect gaps
		if entry.Index != len(s.log)+1 {
			log.Println("log gap detected")
			return &pb.AppendEntriesResponse{Success: false}, nil
		}

		s.log = append(s.log, entry)

		err := s.appendToDisk(entry)
		if err != nil {
			log.Println("persist failed:", err)
			return &pb.AppendEntriesResponse{Success: false}, nil
		}
	}

	// 2. Update commit index
	if int(req.LeaderCommit) > s.commitIndex {
		s.commitIndex = int(req.LeaderCommit)
	}

	// 3. Apply committed entries
	s.applyCommitted()

	return &pb.AppendEntriesResponse{
		Success: true,
	}, nil
}

// Only commited logs entry are appended.
func (s *server1) applyCommitted() {

	for s.lastApplied < s.commitIndex {

		s.lastApplied++

		// Get the log entry (index starts from 1)
		entry := s.log[s.lastApplied-1]

		// Apply to file system
		s.apply(entry)
	}
}

func sendAppendEntry(selfID string, peer ServerInfo, entry LogEntry, commitIndex int) bool {
	conn, err := grpc.NewClient(peer.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return false
	}
	defer conn.Close()

	client := pb.NewReplicationServiceClient(conn)

	req := &pb.AppendEntriesRequest{
		LeaderId:     selfID, // FIXED
		LeaderCommit: int32(commitIndex),
		Entries: []*pb.LogEntry{
			{
				Index:    int32(entry.Index),
				Op:       entry.Op,
				Filename: entry.Filename,
				Content:  entry.Content,
				Version:  entry.Version,
			},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	resp, err := client.AppendEntries(ctx, req)
	if err != nil {
		return false
	}

	return resp.Success
}

type WriteRequest struct {
	Filename string
	Data     []byte
}

func (s *server1) HandleWrite(req WriteRequest) error {

	s.mu.Lock()

	meta := s.getOrCreateFileMeta(req.Filename)

	entry := LogEntry{
		Index:    len(s.log) + 1,
		Op:       "WRITE", //  FIXED
		Filename: req.Filename,
		Content:  req.Data,
		Version:  meta.version + 1, //  FIXED
	}

	s.log = append(s.log, entry)

	err := s.appendToDisk(entry)
	if err != nil {
		s.mu.Unlock()
		return err
	}

	s.mu.Unlock()

	// -------- Replication --------
	ackCount := 1
	commitIndex := entry.Index //  FIXED (no race)

	var mu sync.Mutex
	var wg sync.WaitGroup

	for _, peer := range s.servers {
		if peer.ID == s.id {
			continue
		}

		wg.Add(1)
		go func(p ServerInfo) {
			defer wg.Done()

			if sendAppendEntry(s.id, p, entry, commitIndex) {
				mu.Lock()
				ackCount++
				mu.Unlock()
			}
		}(peer)
	}

	wg.Wait()

	// -------- Commit --------
	if ackCount >= (len(s.servers)/2 + 1) {

		s.mu.Lock()
		s.commitIndex = entry.Index
		s.applyCommitted()
		s.mu.Unlock()

		return nil
	}

	return fmt.Errorf("failed to reach majority")
}

// Crash -> restart -> empty memory
// Follower -> RequestRecovery()
// Leader -> sends FullFiles
// Follower -> ApplyUpdate()
// After that Normal AppendEntries resumes
func (s *server1) RequestRecovery(
	ctx context.Context,
	req *pb.RecoveryRequest,
) (*pb.UpdateMessage, error) {

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.role != Primary {
		return nil, status.Errorf(codes.FailedPrecondition, "not leader")
	}

	var files []*pb.Replication

	for filename, entry := range s.table { // FIXED

		full := filepath.Join(s.rootDir, filename)

		data, err := os.ReadFile(full)
		if err != nil {
			continue
		}

		files = append(files, &pb.Replication{
			Filename: filename,
			Version:  entry.version,
			Content:  data,
		})
	}

	return &pb.UpdateMessage{
		IsFullSync: true,
		FullFiles:  files,
	}, nil
}

func (s *server1) RecoverFromLeader() error {

	s.mu.Lock()
	leader, ok := s.servers[s.primaryID]
	s.mu.Unlock()

	if !ok {
		return fmt.Errorf("leader not found")
	}

	conn, err := grpc.Dial(leader.Address, grpc.WithInsecure())
	if err != nil {
		return err
	}
	defer conn.Close()

	client := pb.NewRecoveryServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.RequestRecovery(ctx, &pb.RecoveryRequest{
		ServerId: s.id,
	})
	if err != nil {
		return err
	}

	return s.ApplyUpdate(resp)
}

// On server  startup
func (s *server1) Start() {
	go func() {
		time.Sleep(2 * time.Second)
		s.RecoverFromLeader()
	}()
}

// Call this when server starts
func (s *server1) Init() {
	s.lastHeartbeat = time.Now()
}

// Main function
func main() {

	// ----------- 1. Parse CLI args -----------
	id := flag.String("id", "", "server id")
	port := flag.String("port", "5000", "port")
	flag.Parse()

	if *id == "" {
		log.Fatal("Server ID required")
	}

	address := "localhost:" + *port

	// ----------- 2. Setup directories -----------
	rootDir := "./data_" + *id
	logFile := "./log_" + *id + ".txt"

	os.MkdirAll(rootDir, os.ModePerm)

	// ----------- 3. Initialize server -----------
	s := &server1{
		id:          *id,
		role:        Backup, // use enum
		primaryID:   "",
		servers:     make(map[string]ServerInfo),
		files:       make(map[int32]*FileMeta),
		table:       make(map[string]*FileEntry),
		requests:    make(map[string]*RequestEntry),
		openMap:     make(map[FileKey]int32),
		rootDir:     rootDir,
		logFilePath: logFile,
	}

	s.Init() //  important

	// ----------- 4. Load cluster config -----------
	s.servers = map[string]ServerInfo{
		"1": {ID: "1", Address: "localhost:5001", Timestamp: 100, Alive: true},
		"2": {ID: "2", Address: "localhost:5002", Timestamp: 200, Alive: true},
		"3": {ID: "3", Address: "localhost:5003", Timestamp: 300, Alive: true},
	}

	// ----------- 5. Recover from disk log -----------
	err := s.recoverFromLog()
	if err != nil {
		log.Println("Recovery error:", err)
	}

	// rebuild derived state
	s.RecoverFromLeader()

	// ----------- 6. Elect primary -----------
	primaryID := electPrimary(s.servers)
	s.primaryID = primaryID

	if s.id == primaryID {
		s.role = Primary
	} else {
		s.role = Backup
	}

	log.Printf("Server %s started as %v\n", s.id, s.role)

	// ----------- 7. gRPC server setup -----------
	lis, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()

	//  MUST register all services
	pb.RegisterReplicationServiceServer(grpcServer, s)
	pb.RegisterHeartbeatServiceServer(grpcServer, s)
	pb.RegisterRecoveryServiceServer(grpcServer, s)

	// ----------- 8. Background processes -----------

	// Leader heartbeats
	go s.StartHeartbeat()

	// Failure detection + election
	go s.MonitorPrimary()

	// Recovery from leader (if backup)
	go func() {
		time.Sleep(2 * time.Second)

		if s.role == Backup {
			err := s.RecoverFromLeader()
			if err != nil {
				log.Println("Recovery from leader failed:", err)
			} else {
				log.Println("Recovery from leader successful")
			}
		}
	}()

	// ----------- 9. Start server -----------
	log.Println("Listening on", address)

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

// Client -> Leader
// -> append log
// -> send AppendEntries
// -> wait majority ACK
// -> commitIndex update
// -> apply locally
// -> followers apply via LeaderCommit
// //Write Updated
func (s *server1) Write_Rep(stream pb.FileService_WriteServer) error {

	var meta *FileMeta
	var filename string
	var entry *FileEntry
	var tmpFile *os.File
	var reqID string
	var mode FileMode
	var dirty bool
	var clientID string
	var tempPrimeSet map[uint64]bool

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

			meta.mu.Lock()
			tempPrimeSet = make(map[uint64]bool, len(meta.primeSet))
			for k, v := range meta.primeSet {
				tempPrimeSet[k] = v
			}
			meta.mu.Unlock()

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
		var newPrimes []uint64

		meta.mu.Lock()
		for p := range tempPrimeSet {
			if !meta.primeSet[p] {
				newPrimes = append(newPrimes, p)
			}
		}
		meta.mu.Unlock()
		var buffer bytes.Buffer

		for _, p := range newPrimes {
			fmt.Fprintln(&buffer, p)
		}

		data := buffer.Bytes()
		// Commit tempPrimeSet -> meta.primeSet
		meta.mu.Lock()
		meta.primeSet = tempPrimeSet
		meta.mu.Unlock()

		// Sync directory
		dir, err := os.Open(s.rootDir)
		if err == nil {
			dir.Sync()
			dir.Close()
		}

		// Step 1: Prepare entry
		s.mu.Lock()

		meta := s.getOrCreateFileMeta(filename)

		newVersion := meta.version + 1

		logEntry := LogEntry{
			Index:    len(s.log) + 1,
			Op:       "WRITE",
			Filename: filename,
			Content:  data, // IMPORTANT
			Version:  newVersion,
		}

		// Append + persist
		s.log = append(s.log, logEntry)
		err = s.appendToDisk(logEntry)
		s.mu.Unlock()

		if err != nil {
			entry.ReleaseWrite()
			return err
		}

		// -------- Replication --------
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

		// -------- Majority check --------
		if ackCount < (len(s.servers)/2 + 1) {
			entry.ReleaseWrite()
			return status.Errorf(codes.Unavailable, "failed to reach majority")
		}

		// -------- Commit --------
		s.mu.Lock()

		s.commitIndex = logEntry.Index
		s.applyCommitted() // IMPORTANT

		// Now update version safely
		meta.version = newVersion

		s.mu.Unlock()

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

func (s *server1) Close_Rep(stream pb.FileService_CloseServer) error {

	var meta *FileMeta
	var filename string
	var entry *FileEntry
	var tmpFile *os.File
	var reqID string
	var mode FileMode
	var dirty bool
	var clientID string
	var tempPrimeSet map[uint64]bool

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

			meta.mu.Lock()
			tempPrimeSet = make(map[uint64]bool, len(meta.primeSet))
			for k, v := range meta.primeSet {
				tempPrimeSet[k] = v
			}
			meta.mu.Unlock()

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
		var newPrimes []uint64

		meta.mu.Lock()
		for p := range tempPrimeSet {
			if !meta.primeSet[p] {
				newPrimes = append(newPrimes, p)
			}
		}
		meta.mu.Unlock()
		var buffer bytes.Buffer

		for _, p := range newPrimes {
			fmt.Fprintln(&buffer, p)
		}

		data := buffer.Bytes()
		// Commit tempPrimeSet -> meta.primeSet
		meta.mu.Lock()
		meta.primeSet = tempPrimeSet
		meta.mu.Unlock()

		// Sync directory
		dir, err := os.Open(s.rootDir)
		if err == nil {
			dir.Sync()
			dir.Close()

			// Step 1: Prepare entry
			s.mu.Lock()

			meta := s.getOrCreateFileMeta(filename)

			newVersion := meta.version + 1

			logEntry := LogEntry{
				Index:    len(s.log) + 1,
				Op:       "WRITE",
				Filename: filename,
				Content:  data, // IMPORTANT
				Version:  newVersion,
			}

			// Append + persist
			s.log = append(s.log, logEntry)
			err := s.appendToDisk(logEntry)
			s.mu.Unlock()

			if err != nil {
				entry.ReleaseWrite()
				return err
			}

			// -------- Replication --------
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

			// -------- Majority check --------
			if ackCount < (len(s.servers)/2 + 1) {
				entry.ReleaseWrite()
				return status.Errorf(codes.Unavailable, "failed to reach majority")
			}

			// -------- Commit --------
			s.mu.Lock()

			s.commitIndex = logEntry.Index
			s.applyCommitted() // IMPORTANT

			// Now update version safely
			meta.version = newVersion

			s.mu.Unlock()

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
