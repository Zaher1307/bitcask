// Package sio(Safe I/O) provides read and write operations on files rubost against
// short count problem.
package sio

import (
	"io/fs"
	"os"
)

// maxAttempts defines the total number of attempts done by read
// or write functions to handle short count problem.
const maxAttempts = 5

// File represents the file with safe i/o functions.
type File struct {
	File *os.File
}

// OpenFile Create a new sio file object with the given flag and permissions.
// Return error on system failures.
func OpenFile(name string, flag int, perm fs.FileMode) (*File, error) {
	file, err := os.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}

	f := &File{
		File: file,
	}

	return f, nil
}

// Open opens an new file with the given name with readonly permission.
// Return error on system failures.
func Open(name string) (*File, error) {
	file, err := os.Open(name)
	if err != nil {
		return nil, err
	}

	f := &File{
		File: file,
	}

	return f, nil
}

// ReadAt reads the data from the given position with length
// equal to the length of the given buffer.
// Return the number of read bytes.
// Return error on system failures.
func (f *File) ReadAt(b []byte, off int64) (int, error) {
	attempts := 0
	n, err := f.File.ReadAt(b, off)
	for i := n; err != nil; i += n {
		if attempts == maxAttempts {
			return 0, err
		}
		off += int64(i)
		n, err = f.File.ReadAt(b[i:], int64(off))
	}

	return len(b), nil
}

// Write writes the given buffer to the file.
// Return the number of written bytes.
// Return error on system failures.
func (f *File) Write(b []byte) (int, error) {
	n, err := f.File.Write(b)

	attempts := 0
	for i := n; err != nil; i += n {
		if attempts == maxAttempts {
			return 0, err
		}
		n, err = f.File.Write(b[i:])
		attempts++
	}

	return len(b), nil
}
