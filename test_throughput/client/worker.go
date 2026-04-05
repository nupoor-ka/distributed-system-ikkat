package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	ikkat "distributed-system-ikkat"
)

func main() {
	servers := []string{
		"localhost:5001",
		"localhost:5002",
		"localhost:5003",
	}
	start := time.Now()
	fmt.Printf("Connecting to server") // connecting to server
	c, conn, err := ikkat.DialClient(servers)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	outputName := "output/output_1.txt"
	workerID := flag.Int("worker", 1, "worker id")
	inputName := flag.String("input", "input/input_dataset_1.txt", "input file")
	flag.Parse()
	clientID := fmt.Sprintf("worker-%d", *workerID)
	fmt.Println("Step 1: Opening input file for reading...") // open input file in read mode
	if _, err := c.Open(ctx, *inputName, 0, clientID); err != nil {
		log.Printf("Open read failed: %v", err)
	}
	fmt.Println("Step 2: Processing primes...") // running prime finding application
	rawContent, err := c.Read(ctx, *inputName, clientID)
	if err != nil {
		log.Fatalf("Read failed: %v", err)
	}
	nums := strings.Fields(string(rawContent))
	var primes []string
	for _, s := range nums {
		n, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			continue
		}
		if ikkat.IsPrime(n) {
			primes = append(primes, s)
		}
	}
	primeResult := strings.Join(primes, "\n")
	fmt.Printf("   Found %d primes\n", len(primes))            // number of primes found
	fmt.Println("Step 3: Creating and opening output file...") // creating, opening output file
	if _, err := c.Create(ctx, outputName, clientID); err != nil {
		log.Printf("Create output failed: %v", err) // could be that file already exists, no issue
	}
	if _, err := c.Open(ctx, outputName, 1, clientID); err != nil {
		log.Fatalf("Open output failed: %v", err)
	}
	fmt.Println("Step 4: Writing primes to output...")
	if err := c.AppendFile(outputName, []byte(primeResult)); err != nil { // write to file
		log.Fatalf("Failed to write output: %v", err)
	}
	fmt.Println("Step 5: Closing output file (committing to cluster)...") // close file, this will commit to server
	if err := c.Close(ctx, outputName, clientID); err != nil {
		log.Fatalf("Close output failed: %v", err)
	}
	elapsed := time.Since(start)
	fmt.Printf("Worker %d finished in %v\n", *workerID, elapsed)
	fmt.Printf("Worker %d throughput: %.2f ops/sec\n",
		*workerID,
		float64(len(primes))/elapsed.Seconds(),
	)
}
