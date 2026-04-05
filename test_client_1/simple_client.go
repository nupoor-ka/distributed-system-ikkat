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

func main() {
	servers := []string{
		"localhost:5001",
		"localhost:5002",
		"localhost:5003",
	}

	// 1. Connect to Server using your DialClient function
	fmt.Printf("Connecting to server")
	c, conn, err := ikkat.DialClient(servers)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	inputName := "input/input_dataset_1.txt"
	outputName := "output/output_1.txt"
	client_id := "tester"

	// 2. Send Create request for input file
	fmt.Println("Step 2: Creating input file...")
	if _, err := c.Create(ctx, inputName, client_id); err != nil {
		log.Printf("Note: Create input returned error (might already exist): %v", err)
	}

	// 3. Send Open request for input file in WRITE mode (Mode 1)
	// We do this just to populate the file if it's empty for the test
	fmt.Println("Step 3: Opening input file for writing initial data...")
	if _, err := c.Open(ctx, inputName, 1, client_id); err != nil {
		log.Printf("Note: Open input returned error, cannot open input files in write mode: %v", err)
	}

	// 4. Send Open request for input file in READ mode (Mode 0)
	fmt.Println("Step 4: Opening input file for reading...")
	if _, err := c.Open(ctx, inputName, 0, client_id); err != nil {
		log.Printf("Open read failed: %v", err)
	}

	// 5. Run Prime Finding application on input file
	fmt.Println("Step 5: Processing primes...")
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
		// Using your ikkat.IsPrime(uint64) function
		if ikkat.IsPrime(n) {
			primes = append(primes, s)
		}
	}
	primeResult := strings.Join(primes, "\n")
	fmt.Printf("   Found %d primes\n", len(primeResult)) // no need to print them, we have faith

	// 6. Send Open request for output file in WRITE mode
	fmt.Println("Step 6: Creating and opening output file...")
	if _, err := c.Create(ctx, outputName, client_id); err != nil {
		log.Printf("Create output failed: %v", err)
	}
	if _, err := c.Open(ctx, outputName, 1, client_id); err != nil {
		log.Fatalf("Open output failed: %v", err)
	}

	// 7. Write to the output file using your custom Write function
	fmt.Println("Step 7: Writing primes to output...")
	if err := c.WriteFile(outputName, []byte(primeResult)); err != nil {
		log.Fatalf("Failed to write output: %v", err)
	}

	// 8. Send Close request for the output file
	// This triggers the commit to the distributed cluster
	fmt.Println("Step 8: Closing output file (committing to cluster)...")
	if err := c.Close(ctx, outputName, client_id); err != nil {
		log.Fatalf("Close output failed: %v", err)
	}

	// 9. Send Read request for verification + TestAuth check
	fmt.Println("Step 9: Verifying data with TestAuth...")

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

	if string(finalData) == primeResult {
		fmt.Println("\nSUCCESS: File content is consistent across the distributed system!")
	} else {
		fmt.Printf("\nFAILURE: Content mismatch. Expected [%s] but got [%s]\n", primeResult, string(finalData))
	}
}
