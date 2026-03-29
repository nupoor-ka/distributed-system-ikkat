package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"strconv"

	pb "distributed-system-ikkat/filesystem"
)

func main() {
	ctx := context.Background()
	client, err := NewClient("localhost:50051") // one client
	if err != nil {
		log.Fatal(err)
	}
	inFd, err := client.Open(ctx, "input_dataset_001.txt", "READ")
	if err != nil {
		log.Fatal(err)
	}

	// STEP 2 — Read locally cached file
	data, err := client.ReadLocal(inFd)
	if err != nil {
		log.Fatal(err)
	}

	client.Close(ctx, inFd)

	// STEP 3 — Find primes
	primes := []int{}

	scanner := bufio.NewScanner(data)

	for scanner.Scan() {
		num, _ := strconv.Atoi(scanner.Text())

		if isPrime(num) {
			primes = append(primes, num)
		}
	}

	// STEP 4 — Create output file
	outFd, err := client.Create(ctx, "primes.txt")
	if err != nil {
		log.Fatal(err)
	}

	// STEP 5 — Write primes locally
	for _, p := range primes {
		line := fmt.Sprintf("%d\n", p)

		err := client.WriteLocal(outFd, []byte(line))
		if err != nil {
			log.Fatal(err)
		}
	}

	// STEP 6 — Close triggers flush to server
	err = client.Close(ctx, outFd)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Test 1 PASSED")
}