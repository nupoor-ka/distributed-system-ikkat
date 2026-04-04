package main

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	ikkat "distributed-system-ikkat"
)

func AllUniqueLines(data []byte) (bool, []string) { // to test if final output file only has uniques
	lines := strings.Split(string(data), "\n")
	seen := make(map[string]bool)
	var duplicates []string
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if seen[line] {
			duplicates = append(duplicates, line)
		} else {
			seen[line] = true
		}
	}
	return len(duplicates) == 0, duplicates
}

func main() {
	serverAddr := "localhost:5001"
	fmt.Printf("Connecting to server at %s...\n", serverAddr) // connecting to server
	c, conn, err := ikkat.DialClient(serverAddr)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	inputName := "input/input_dataset_2.txt"
	outputName := "output/output_1.txt"
	client_id := "tester3"
	fmt.Println("Step 1: Opening input file for reading...") // open input file in read mode
	if _, err := c.Open(ctx, inputName, 0, client_id); err != nil {
		log.Printf("Open read failed: %v", err)
	}
	fmt.Println("Step 2: Processing primes...") // running prime finding application
	rawContent, err := c.Read(ctx, inputName, client_id)
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
	fmt.Printf("   Found %d primes\n", len(primeResult))       // number of primes found
	fmt.Println("Step 3: Creating and opening output file...") // creating, opening output file
	if _, err := c.Create(ctx, outputName, client_id); err != nil {
		log.Printf("Create output failed: %v", err) // could be that file already exists, no issue
	}
	if _, err := c.Open(ctx, outputName, 1, client_id); err != nil {
		log.Fatalf("Open output failed: %v", err)
	}
	fmt.Println("Step 4: Writing primes to output...")
	if err := c.AppendFile(outputName, []byte(primeResult)); err != nil { // write to file
		log.Fatalf("Failed to write output: %v", err)
	}
	fmt.Println("Step 5: Closing output file (committing to cluster)...") // close file, this will commit to server
	if err := c.Close(ctx, outputName, client_id); err != nil {
		log.Fatalf("Close output failed: %v", err)
	}
	if _, err := c.Open(ctx, outputName, 0, client_id); err != nil { // open the output file again
		log.Fatalf("Final open failed: %v", err)
	}
	finalData, err := c.Read(ctx, outputName, client_id) // read output file
	if err != nil {
		log.Fatalf("Final read failed: %v", err)
	}
	unique, dups := AllUniqueLines(finalData)
	if unique {
		fmt.Println("All numbers are unique. Deduplication works.")
	} else {
		fmt.Printf("Duplicates found: %v\n", dups)
	}
	if string(finalData) == primeResult {
		fmt.Println("\nSUCCESS: File content is consistent across the distributed system!")
	} else {
		fmt.Printf("\nFAILURE: Content mismatch. Expected [%s] but got [%s]\n", primeResult, string(finalData))
	}
}
