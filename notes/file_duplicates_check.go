package main

import (
	"bufio"
	"fmt"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run notes/file_duplicates_check.go <file_path>")
		return
	}
	filePath := os.Args[1]
	file, err := os.Open(filePath)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	seen := make(map[string]int)
	duplicateCount := 0
	for scanner.Scan() {
		line := scanner.Text()
		seen[line]++
		if seen[line] > 1 {
			duplicateCount++
		}
	}
	if err := scanner.Err(); err != nil {
		fmt.Println("Error reading file:", err)
		return
	}
	fmt.Printf("Total duplicate entries: %d\n", duplicateCount)
	// fmt.Println("\nDuplicated values:")
	// for value, count := range seen {
	// 	if count > 1 {
	// 		fmt.Printf("%s -> %d times\n", value, count)
	// 	}
	// }
}
