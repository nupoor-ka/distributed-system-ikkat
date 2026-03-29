package main

import (
	// "context"
	"fmt"
	"io"
	"math/bits"
	"os"
	"path/filepath"
	"strings"
	// "sync"
	// "time"
	// pb "distributed-system-ikkat/filesystem"
	// "google.golang.org/grpc/codes"
	// "google.golang.org/grpc/status"
)

type FileMode int

const (
	ReadMode FileMode = iota
	WriteMode
	ReadWriteMode
)

func sanitizePath(p string) (string, error) {
	safe := filepath.Clean(p)
	if filepath.IsAbs(safe) || strings.HasPrefix(safe, "..") || strings.HasPrefix(safe, "../") {
		return "", fmt.Errorf("invalid path")
	}
	return safe, nil
}

// Function to real local cache file by client
func readLocalFile(path string, offset int64, size int64) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	// move to offset
	_, err = f.Seek(offset, io.SeekStart) // offset, whence
	if err != nil {
		return nil, err
	}
	buf := make([]byte, size)
	n, err := f.Read(buf)
	if err != nil && err != io.EOF {
		return nil, err
	}
	return buf[:n], nil
}

// Function to write local cache file by client
func writeLocalFile(path string, data []byte, offset int64) error {

	// Open file (create if not exists)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644) // trunc in case old file longer than new one
	if err != nil {
		return err
	}
	defer f.Close()

	// Move to offset
	_, err = f.Seek(offset, io.SeekStart)
	if err != nil {
		return err
	}

	// Write data at offset
	_, err = f.Write(data)
	if err != nil {
		return err
	}

	return nil
}

// Checking Prime Number using
// fast modular exponentiation: (base^exp) % mod
func modExp(base, exp, mod uint64) uint64 {
	result := uint64(1)
	base = base % mod

	for exp > 0 {
		if exp&1 == 1 {
			result = mulMod(result, base, mod)
		}
		base = mulMod(base, base, mod)
		exp >>= 1
	}
	return result
}

func mulMod(a, b, mod uint64) uint64 {
	hi, lo := bits.Mul64(a, b)
	_, rem := bits.Div64(hi, lo, mod)
	return rem
}

// Miller-Rabin test for one base
func check(a, d, n uint64, r int) bool {
	x := modExp(a, d, n)

	if x == 1 || x == n-1 {
		return true
	}

	for i := 0; i < r-1; i++ {
		x = mulMod(x, x, n)
		if x == n-1 {
			return true
		}
	}
	return false
}

// Deterministic Miller-Rabin for 64-bit integers
func isPrime(n uint64) bool {
	if n < 2 {
		return false
	}

	// small primes check
	smallPrimes := []uint64{2, 3, 5, 7, 11, 13, 17}
	for _, p := range smallPrimes {
		if n%p == 0 {
			return n == p
		}
	}

	// write n-1 = d * 2^r
	d := n - 1
	r := 0
	for d%2 == 0 {
		d /= 2
		r++
	}

	// test bases
	for _, a := range smallPrimes {
		if a >= n {
			continue
		}
		if !check(a, d, n, r) {
			return false
		}
	}

	return true
}

//nums := []uint64{10, 11, 13, 15, 17, 19, 20}
