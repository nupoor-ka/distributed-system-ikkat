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
	serverAddr := "localhost:5001" // Update this to your server's address

	// 1. Connect to Server using your DialClient function
	fmt.Printf("Connecting to server at %s...\n", serverAddr)
	c, conn, err := ikkat.DialClient(serverAddr)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	inputName := "input/input_dataset_2.txt"
	outputName := "output/output_1.txt"
	client_id := "tester2"

	// 1. Send Open request for input file in READ mode (Mode 0)
	fmt.Println("Step 1: Opening input file for reading...")
	if _, err := c.Open(ctx, inputName, 0, client_id); err != nil {
		log.Printf("Open read failed: %v", err)
	}

	// 2. Run Prime Finding application on input file
	fmt.Println("Step 2: Processing primes...")
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
	primeResult += "14461\n7019\n15073\n8209\n6323\n14759\n8461\n15187\n4363" // adding duplicates on purpose to test deduplication
	fmt.Printf("   Found %d primes\n", len(primeResult))                      // no need to print them, we have faith

	// 3. Send Open request for output file in WRITE mode
	fmt.Println("Step 3: Creating and opening output file...")
	if _, err := c.Create(ctx, outputName, client_id); err != nil {
		log.Printf("Create output failed: %v", err) // could be that file already exists, no issue
	}
	if _, err := c.Open(ctx, outputName, 1, client_id); err != nil {
		log.Fatalf("Open output failed: %v", err)
	}

	// 4. Append to the output file using custom Write function
	fmt.Println("Step 4: Writing primes to output...")
	if err := c.AppendFile(outputName, []byte(primeResult)); err != nil {
		log.Fatalf("Failed to write output: %v", err)
	}

	// 5. Send Close request for the output file
	// This triggers the commit to the distributed cluster
	fmt.Println("Step 5: Closing output file (committing to cluster)...")
	if err := c.Close(ctx, outputName, client_id); err != nil {
		log.Fatalf("Close output failed: %v", err)
	}

	// Verify the version via your TestAuth gRPC method
	// authResp, err := c.TestAuth(ctx, &pb.TestAuthRequest{Filename: outputName})
	// if err != nil {
	// 	log.Fatalf("TestAuth failed: %v", err)
	// }

	// Open again in READ mode (0) to verify the content
	// Note: your Open function internally calls testAuth to check consistency
	if _, err := c.Open(ctx, outputName, 0, client_id); err != nil {
		log.Fatalf("Final open failed: %v", err)
	}

	// Use your custom Read function to fetch the committed data
	finalData, err := c.Read(ctx, outputName, client_id)
	if err != nil {
		log.Fatalf("Final read failed: %v", err)
	}

	fmt.Printf("   Final Content: %s\n", string(finalData))
	// fmt.Printf("   Final Version: %d\n", authResp.Version)

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
