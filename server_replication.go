package ikkat

import (
	"bufio"
	"context"
	pb "distributed-system-ikkat/filesystem"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"google.golang.org/grpc"                      // normal connecting
	"google.golang.org/grpc/codes"                // error codes
	"google.golang.org/grpc/credentials/insecure" // no credentials for security rn
	"google.golang.org/grpc/status"
)

// File Meta is same for all server
// primeSet is local cache of each server

const (
	HeartBeatTime        = 2 * time.Second // primary sends a HeartBeat message at every this interval
	PrimaryFailedTimeout = 5 * time.Second // if backups receive no heartbeat from primary for this long, primary is assumed to have failed
)

type Replication struct {
	Filename string
	Version  int32
	Content  []byte // for recovery, data of file
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

// UpdateMessage is used for recovery and synchronization of out-of-date replicas, while normal replication is handled using log-based AppendEntries with majority acknowledgment
type UpdateMessage struct {
	IsFullSync bool
	LogEntries []LogEntry    // incremental sync
	FullFiles  []Replication // full snapshot
}

// revival works only if: recovered server receives heartbeat from current leader

// send a message with primaryID and timestamp
func (s *server) SendHeartbeat(ctx context.Context, req *pb.Heartbeat) (*pb.HeartbeatResponse, error) {
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
func (s *server) sendHeartbeat(peer ServerInfo) {
	conn, _ := grpc.NewClient(peer.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
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
	if !resp.Success { // if rejected by followers, leader must step down
		fmt.Println("Another leader exists, stepping down")

		s.mu.Lock()
		s.role = Backup
		s.mu.Unlock()
	}
}

// for primary to send a heartbeat message to all backups every 2 seconds
func (s *server) StartHeartbeat() {
	for { // for infinity
		time.Sleep(HeartBeatTime)
		s.mu.Lock()
		isLeader := s.id == s.primaryID // if this is the leader
		s.mu.Unlock()
		if !isLeader {
			continue
		}
		for _, srv := range s.servers {
			if srv.ID == s.id { // don't send it to yourself
				continue
			} // send a heartbeat message to the rest
			s.sendHeartbeat(srv) // no goroutine explosion?
		}
	}
}

// If Hearbeat stops
func (s *server) MonitorPrimary() {
	for {
		time.Sleep(3 * time.Second)
		s.mu.Lock()
		if s.id == s.primaryID { // only backups do this
			s.mu.Unlock()
			continue
		}
		if time.Since(s.lastHeartbeat) > PrimaryFailedTimeout { // primary hasn't contacted since some time
			fmt.Println("Primary failed, electing new leader")
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

// Backup keep track of var lastHeartbeat time.Time and also notes primary
func (s *server) OnHeartbeat(hb Heartbeat) {
	s.primaryID = hb.PrimaryID
	s.lastHeartbeat = time.Now()
}

// Elect Primary
func electPrimary(servers map[string]ServerInfo) string {
	min := int64(math.MaxInt64)
	var leader string
	for _, s := range servers { // check all servers
		if !s.Alive { // if this one is alive
			continue
		}
		if s.Timestamp < min { // choose the one with the smallest timestamp
			min = s.Timestamp
			leader = s.ID
		}
	}
	return leader
}

// Checking primary is alive
func (s *server) CheckPrimaryAlive() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.id == s.primaryID { // primary doesn't do this
		return
	}
	if time.Since(s.lastHeartbeat) <= PrimaryFailedTimeout { // primary recently sent a message, alive
		return
	}
	log.Println("Primary failed. Electing new leader...")
	oldLeader := s.primaryID
	if srv, ok := s.servers[oldLeader]; ok {
		srv.Alive = false // marking prev primary dead
		s.servers[oldLeader] = srv
	}
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

// appending json logs in log file
// {"Index":1,"Op":"WRITE","Filename":"file.txt","Content":"...","Version":1}
func (s *server) appendToDisk(entry LogEntry) error {
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
	return f.Sync() // ensures durability, flushes buffered file data to disk immediately
}

// Recover from logs
func (s *server) recoverFromLog() error {
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
func (s *server) RebuildPrimeSet(meta *FileMeta) error {
	if s.role != Primary { // only needed for primary
		return nil
	}
	filePath := filepath.Join(s.rootDir, meta.filename)
	data, err := os.ReadFile(filePath)
	if err != nil {
		return err
	}
	primes := parseNumbers(data)
	newSet := make(FilePrimeSet)
	for _, p := range primes {
		newSet[p] = struct{}{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.filesPrimes[meta.filename] = &newSet
	return nil
}

func (s *server) getOrCreateFileMeta(filename string) *FileMeta {
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
		// primeSet: make(map[uint64]bool),
	}
	s.files[fd] = meta
	s.table[filename] = &FileEntry{
		FD:      fd,
		version: 0,
	}
	return meta
}

// This is called when log entry is commited
func (s *server) apply(entry LogEntry) {
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
	fm.version = entry.Version // update version after success
	s.RebuildPrimeSet(fm)      // rebuild derived state
}

// Used when : Follower is behind OR restarted
// missing log , full state
// Recovery only
func (s *server) ApplyUpdate(msg *pb.UpdateMessage) error {
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
			s.mu.Unlock() // unlock before expensive rebuild
			s.RebuildPrimeSet(meta)
			s.mu.Lock()
		}
		s.mu.Unlock()
		return nil
	}
	for _, e := range msg.LogEntries { // incremental logs
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
func (s *server) AppendEntries(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
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
func (s *server) applyCommitted() {
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

func (s *server) HandleWrite(req WriteRequest) error {
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
func (s *server) RequestRecovery(ctx context.Context, req *pb.RecoveryRequest) (*pb.UpdateMessage, error) {
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

func (s *server) RecoverFromLeader() error {
	s.mu.Lock()
	leader, ok := s.servers[s.primaryID]
	s.mu.Unlock()
	if !ok {
		return fmt.Errorf("leader not found")
	}
	conn, err := grpc.NewClient(leader.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
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
func (s *server) Start() {
	go func() {
		time.Sleep(2 * time.Second)
		s.RecoverFromLeader()
	}()
}

// Call this when server starts
func (s *server) Init() {
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
	s := &server{
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
	go s.cleanupRequests()
	go s.cleanupLeases()
	go s.startCleanupRoutine()
	s.rebuildVersionTable()
	// s.cleanupTempFiles()
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
// func (s *server) Write_Rep(stream pb.FileService_WriteServer) error {
// 	var meta *FileMeta
// 	var filename string
// 	var entry *FileEntry
// 	var tmpFile *os.File
// 	var reqID string
// 	var mode pb.FileMode
// 	var dirty bool
// 	var clientID string
// 	var tempPrimeSet FilePrimeSet
// 	for {
// 		req, err := stream.Recv()
// 		if err == io.EOF {
// 			break
// 		}
// 		if err != nil {
// 			return status.Errorf(codes.Internal, "recv failed")
// 		}
// 		if meta == nil {
// 			// Extract clientID once
// 			ctx := stream.Context()
// 			md, ok := metadata.FromIncomingContext(ctx)
// 			if !ok {
// 				return status.Errorf(codes.Unauthenticated, "missing metadata")
// 			}
// 			clientIDs := md["client-id"]
// 			if len(clientIDs) == 0 {
// 				return status.Errorf(codes.Unauthenticated, "client-id missing")
// 			}
// 			clientID = clientIDs[0]
// 			reqID = req.RequestId
// 			dirty = req.Dirty
// 			// Cache check
// 			s.mu.Lock()
// 			if entryCache, ok := s.requests[req.RequestId]; ok &&
// 				time.Since(entryCache.timestamp) < RequestCacheTTL {
// 				resp := entryCache.response.(*pb.WriteResponse)
// 				s.mu.Unlock()
// 				return stream.SendAndClose(resp)
// 			}
// 			m, ok := s.files[req.Fd]
// 			if !ok {
// 				s.mu.Unlock()
// 				return status.Errorf(codes.NotFound, "file not open")
// 			}
// 			meta = m
// 			filename = meta.filename
// 			client, ok := meta.clients[clientID]
// 			if !ok {
// 				s.mu.Unlock()
// 				return status.Errorf(codes.PermissionDenied, "client not registered")
// 			}
// 			mode = client.mode
// 			s.mu.Unlock()
// 			entry = s.getFileEntry(filename)
// 			meta.mu.Lock()
// 			client, exists := meta.clients[clientID]
// 			if !exists {
// 				return status.Errorf(codes.PermissionDenied, "client not registered for this file")
// 			}
// 			if time.Since(client.lastSeen) > LeaseTimeout {
// 				key := FileKey{
// 					clientID: clientID,
// 					filename: meta.filename,
// 				}
// 				s.mu.Lock()
// 				delete(s.openMap, key)
// 				s.mu.Unlock()
// 				meta.mu.Lock()
// 				delete(meta.clients, clientID)
// 				meta.mu.Unlock()
// 				return status.Errorf(codes.PermissionDenied, "lease expired")
// 			}
// 			// Refresh lease
// 			client.lastSeen = time.Now()
// 			meta.clients[clientID] = client
// 			meta.mu.Unlock()
// 			if dirty { // write only if dirty
// 				entry.AcquireWrite()
// 				// Validate
// 				if mode != pb.FileMode_WRITE {
// 					entry.ReleaseWrite()
// 					return status.Errorf(codes.PermissionDenied, "not opened in write mode")
// 				}
// 				if req.Version != entry.version {
// 					entry.ReleaseWrite()
// 					return status.Errorf(codes.Aborted, "conflict")
// 				}
// 				// Create temp file
// 				full := filepath.Join(s.rootDir, filename)
// 				tmp := full + ".tmp"
// 				f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
// 				if err != nil {
// 					entry.ReleaseWrite()
// 					return status.Errorf(codes.Internal, "temp open failed")
// 				}
// 				tmpFile = f
// 			}
// 		}
// 		if dirty {
// 			// Refresh lease per chunk
// 			meta.mu.Lock()
// 			client, ok := meta.clients[clientID]
// 			if ok {
// 				client.lastSeen = time.Now()
// 				meta.clients[clientID] = client
// 			}
// 			meta.mu.Unlock()
// 			s.mu.Lock()
// 			tempPrimeSet = make(FilePrimeSet, len(*s.filesPrimes[filename]))
// 			for k, v := range *s.filesPrimes[filename] {
// 				tempPrimeSet[k] = v
// 			}
// 			s.mu.Unlock()
// 			nums := parseNumbers(req.Data)
// 			unique := s.FilterUniquePrimes(nums, tempPrimeSet)
// 			for _, p := range unique {
// 				line := fmt.Sprintf("%d\n", p)
// 				if _, err := tmpFile.WriteString(line); err != nil {
// 					tmpFile.Close()
// 					entry.ReleaseWrite()
// 					return status.Errorf(codes.Internal, "write failed")
// 				}
// 			}
// 		}
// 	}
// 	var newVersion int32
// 	if dirty {
// 		if tmpFile == nil {
// 			return status.Errorf(codes.InvalidArgument, "no data received")
// 		}
// 		tmpFile.Sync()
// 		tmpFile.Close()
// 		full := filepath.Join(s.rootDir, filename)
// 		tmp := full + ".tmp"
// 		if err := os.Rename(tmp, full); err != nil {
// 			entry.ReleaseWrite()
// 			return status.Errorf(codes.Internal, "rename failed")
// 		}
// 		var newPrimes []uint64
// 		s.mu.Lock()
// 		for p := range tempPrimeSet {
// 			if _, exists := (*s.filesPrimes[filename])[p]; !exists {
// 				newPrimes = append(newPrimes, p)
// 			}
// 		}
// 		s.mu.Unlock()
// 		var buffer bytes.Buffer
// 		for _, p := range newPrimes {
// 			fmt.Fprintln(&buffer, p)
// 		}
// 		data := buffer.Bytes()
// 		s.mu.Lock() // commit tempPrimeSet to server filePrimes
// 		*s.filesPrimes[filename] = tempPrimeSet
// 		s.mu.Unlock()
// 		dir, err := os.Open(s.rootDir) // sync dir
// 		if err == nil {
// 			dir.Sync()
// 			dir.Close()
// 		}
// 		s.mu.Lock() // make entry
// 		meta := s.getOrCreateFileMeta(filename)
// 		newVersion := meta.version + 1
// 		logEntry := LogEntry{
// 			Index:    len(s.log) + 1,
// 			Op:       "WRITE",
// 			Filename: filename,
// 			Content:  data, // IMPORTANT
// 			Version:  newVersion,
// 		}
// 		// Append + persist
// 		s.log = append(s.log, logEntry)
// 		err = s.appendToDisk(logEntry)
// 		s.mu.Unlock()
// 		if err != nil {
// 			entry.ReleaseWrite()
// 			return err
// 		}
// 		ackCount := 1 // replication
// 		commitIndex := logEntry.Index
// 		for _, peer := range s.servers {
// 			if peer.ID == s.id {
// 				continue
// 			}
// 			if sendAppendEntry(s.id, peer, logEntry, commitIndex) {
// 				ackCount++
// 			}
// 		}
// 		if ackCount < (len(s.servers)/2 + 1) { // majority check
// 			entry.ReleaseWrite()
// 			return status.Errorf(codes.Unavailable, "failed to reach majority")
// 		}
// 		s.mu.Lock() // commit
// 		s.commitIndex = logEntry.Index
// 		s.applyCommitted() // IMPORTANT
// 		// Now update version safely
// 		meta.version = newVersion
// 		s.mu.Unlock()
// 		entry.ReleaseWrite()
// 	} else {
// 		// No write -> just return current version
// 		entry.mu.Lock()
// 		newVersion = entry.version
// 		entry.mu.Unlock()
// 	}
// 	resp := &pb.WriteResponse{
// 		Message: "write successful",
// 		Version: newVersion,
// 	}
// 	// Cache response
// 	s.mu.Lock()
// 	s.requests[reqID] = &RequestEntry{
// 		response:  resp,
// 		timestamp: time.Now(),
// 	}
// 	s.mu.Unlock()
// 	return stream.SendAndClose(resp)
// }

// // func (s *server) Close_Rep(stream pb.FileService_CloseServer) error {
// // 	var meta *FileMeta
// // 	var filename string
// // 	var entry *FileEntry
// 	var tmpFile *os.File
// 	var reqID string
// 	var mode pb.FileMode
// 	var dirty bool
// 	var clientID string
// 	var tempPrimeSet FilePrimeSet
// 	for { // as long as you keep receiving chunks
// 		req, err := stream.Recv()
// 		if err == io.EOF { // stream ended, done receiving
// 			break
// 		}
// 		if err != nil {
// 			return status.Errorf(codes.Internal, "receive failed")
// 		}
// 		if meta == nil { // haven't set meta yet meaning if this is the first chunk, meaning the initial request
// 			reqID = req.RequestId
// 			dirty = req.Dirty
// 			s.mu.Lock() // checking request cache
// 			if entryCache, ok := s.requests[req.RequestId]; ok &&
// 				time.Since(entryCache.timestamp) < RequestCacheTTL {
// 				resp := entryCache.response.(*pb.CloseResponse)
// 				s.mu.Unlock()
// 				return stream.SendAndClose(resp) // send same response if request has already been carried out
// 			}
// 			m, ok := s.files[req.Fd]
// 			if !ok {
// 				s.mu.Unlock()
// 				return status.Errorf(codes.NotFound, "file not open")
// 			}
// 			meta = m
// 			filename = meta.filename
// 			client, ok := meta.clients[clientID]
// 			if !ok {
// 				// Retry-safe (already closed case)
// 				if entryCache, ok := s.requests[req.RequestId]; ok &&
// 					time.Since(entryCache.timestamp) < RequestCacheTTL {
// 					resp := entryCache.response.(*pb.CloseResponse)
// 					s.mu.Unlock()
// 					return stream.SendAndClose(resp)
// 				}

// 				entry := s.getFileEntry(filename)
// 				entry.mu.Lock()
// 				version := entry.version
// 				entry.mu.Unlock()
// 				resp := &pb.CloseResponse{
// 					Message: "already closed",
// 					Version: version,
// 				}
// 				s.requests[req.RequestId] = &RequestEntry{
// 					response:  resp,
// 					timestamp: time.Now(),
// 				}
// 				s.mu.Unlock()
// 				return stream.SendAndClose(resp)
// 			}
// 			mode = client.mode
// 			s.mu.Unlock()
// 			entry = s.getFileEntry(filename)
// 			if dirty { // write only if dirty
// 				entry.AcquireWrite()
// 				// Validate
// 				if mode != pb.FileMode_WRITE {
// 					entry.ReleaseWrite()
// 					return status.Errorf(codes.PermissionDenied, "not opened in write mode")
// 				}
// 				if req.Version != entry.version {
// 					entry.ReleaseWrite()
// 					return status.Errorf(codes.Aborted, "conflict")
// 				}
// 				// Create temp file
// 				full := filepath.Join(s.rootDir, filename)
// 				tmp := full + ".tmp"
// 				f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
// 				if err != nil {
// 					entry.ReleaseWrite()
// 					return status.Errorf(codes.Internal, "temp open failed")
// 				}
// 				tmpFile = f
// 			}
// 		}
// 		if dirty { // process chunks only if dirty
// 			s.mu.Lock()
// 			tempPrimeSet = make(FilePrimeSet, len(*s.filesPrimes[filename]))
// 			for k, v := range *s.filesPrimes[filename] {
// 				tempPrimeSet[k] = v
// 			}
// 			s.mu.Unlock()
// 			nums := parseNumbers(req.Data)
// 			unique := s.FilterUniquePrimes(nums, tempPrimeSet)

// 			for _, p := range unique {
// 				line := fmt.Sprintf("%d\n", p)
// 				if _, err := tmpFile.WriteString(line); err != nil {
// 					tmpFile.Close()
// 					entry.ReleaseWrite()
// 					return status.Errorf(codes.Internal, "write failed")
// 				}
// 			}
// 		}
// 	}
// 	var newVersion int32
// 	//only check if dirty
// 	if dirty && tmpFile == nil {
// 		return status.Errorf(codes.InvalidArgument, "no data received")
// 	}
// 	if dirty { // if the file has been changed
// 		tmpFile.Sync()
// 		tmpFile.Close()
// 		full := filepath.Join(s.rootDir, filename)
// 		tmp := full + ".tmp"
// 		if err := os.Rename(tmp, full); err != nil {
// 			entry.ReleaseWrite()
// 			return status.Errorf(codes.Internal, "rename failed")
// 		}
// 		var newPrimes []uint64
// 		s.mu.Lock()
// 		for p := range tempPrimeSet {
// 			if _, exists := (*s.filesPrimes[filename])[p]; !exists {
// 				newPrimes = append(newPrimes, p)
// 			}
// 		}
// 		s.mu.Unlock()
// 		var buffer bytes.Buffer
// 		for _, p := range newPrimes {
// 			fmt.Fprintln(&buffer, p)
// 		}
// 		data := buffer.Bytes()
// 		s.mu.Lock() // commit temp to filePrimes
// 		*s.filesPrimes[filename] = tempPrimeSet
// 		s.mu.Unlock()
// 		// Sync directory
// 		dir, err := os.Open(s.rootDir)
// 		if err == nil {
// 			dir.Sync()
// 			dir.Close()
// 			// Step 1: Prepare entry
// 			s.mu.Lock()
// 			meta := s.getOrCreateFileMeta(filename)
// 			newVersion := meta.version + 1
// 			logEntry := LogEntry{
// 				Index:    len(s.log) + 1,
// 				Op:       "WRITE",
// 				Filename: filename,
// 				Content:  data, // IMPORTANT
// 				Version:  newVersion,
// 			}
// 			// Append + persist
// 			s.log = append(s.log, logEntry)
// 			err := s.appendToDisk(logEntry)
// 			s.mu.Unlock()

// 			if err != nil {
// 				entry.ReleaseWrite()
// 				return err
// 			}
// 			ackCount := 1 // replication
// 			commitIndex := logEntry.Index
// 			for _, peer := range s.servers {
// 				if peer.ID == s.id {
// 					continue
// 				}
// 				if sendAppendEntry(s.id, peer, logEntry, commitIndex) {
// 					ackCount++
// 				}
// 			}
// 			if ackCount < (len(s.servers)/2 + 1) { // majority check
// 				entry.ReleaseWrite()
// 				return status.Errorf(codes.Unavailable, "failed to reach majority")
// 			}
// 			s.mu.Lock() // commit
// 			s.commitIndex = logEntry.Index
// 			s.applyCommitted() // IMPORTANT
// 			// Now update version safely
// 			meta.version = newVersion
// 			s.mu.Unlock()
// 			entry.ReleaseWrite()
// 		} else {
// 			entry.mu.Lock()
// 			newVersion = entry.version
// 			entry.mu.Unlock()
// 		}
// 	} else {
// 		entry.mu.Lock()
// 		newVersion = entry.version
// 		entry.mu.Unlock()
// 	}
// 	meta.file.Close() // close fd
// 	s.mu.Lock()       // clean up fd
// 	delete(meta.clients, clientID)
// 	s.mu.Unlock()
// 	resp := &pb.CloseResponse{ // resp to client on closing
// 		Message: "closed",
// 		Version: newVersion,
// 	}
// 	s.mu.Lock()
// 	s.requests[reqID] = &RequestEntry{ // store in request cache
// 		response:  resp,
// 		timestamp: time.Now(),
// 	}
// 	s.mu.Unlock()
// 	err := stream.SendAndClose(resp) // send resp, close stream, done closing the file
// 	return err
// }
