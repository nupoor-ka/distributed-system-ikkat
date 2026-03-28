package main

import (
	// "context"
	"fmt"
	"io"
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
