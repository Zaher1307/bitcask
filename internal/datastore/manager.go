// Package datastore provides functionality to contorol the datastore directory.
package datastore

import (
	"errors"
	"fmt"
	"os"
	"path"

	"github.com/zaher1307/bitcask/internal/recfmt"
	"github.com/zaher1307/bitcask/internal/sio"
	"github.com/gofrs/flock"
)

const (
	// ExclusiveLock is an option to make the datastore lock exclusive.
	ExclusiveLock LockMode = 0
	// SharedLock is an option to make the datastore lock shared.
	SharedLock LockMode = 1

	// TompStone is a special value to mark the deleted values.
	TompStone = "8890fc70294d02dbde257989e802451c2276be7fb177c3ca4399dc4728e4e1e0"

	// lockFile is the name of the file used to lock the datastore directory.
	lockFile = ".lck"
)

var (
	// errAccessDenied happens when a bitcask process tries to access to the datastore
	// when the directory is locked.
	errAccessDenied = errors.New("access denied: datastore is locked")

	// ErrKeyNotExist happens when accessing value does not exist.
	ErrKeyNotExist = errors.New("key does not exist")
)

type (
	// LockMode represents the lock mode of the directory.
	LockMode int

	// DataStore represents and contains the metadata of the datastore directory.
	DataStore struct {
		path string
		lock LockMode
		flck *flock.Flock
	}
)

// NewDataStore creates new datastore object with the given path and lock mode.
// Return an error on system failures or when access to the directory is denied.
func NewDataStore(dataStorePath string, lock LockMode) (*DataStore, error) {
	d := &DataStore{
		path: dataStorePath,
		lock: lock,
	}

	dir, dirErr := os.Open(dataStorePath)
	defer dir.Close()

	if dirErr == nil {
		acquired, err := d.openDataStoreDir()
		if err != nil {
			return nil, err
		}
		if !acquired {
			return nil, errAccessDenied
		}
	} else if os.IsNotExist(dirErr) && lock == ExclusiveLock {
		err := d.createDataStoreDir()
		if err != nil {
			return nil, err
		}
	} else {
		return nil, dirErr
	}

	return d, nil
}

// NewAppendFile creates new append files object with the given path, flags and type.
func NewAppendFile(dataStorePath string, fileFlags int, appendType AppendType) *AppendFile {
	a := &AppendFile{
		filePath:   dataStorePath,
		fileFlags:  fileFlags,
		appendType: appendType,
	}

	return a
}

// createDataStoreDir creates a new directory to be a datastore directory
// and acquires the necessary lock.
func (d *DataStore) createDataStoreDir() error {
	err := os.MkdirAll(d.path, os.FileMode(0777))
	if err != nil {
		return err
	}

	_, err = d.acquireFileLock()
	if err != nil {
		return err
	}

	return nil
}

// openDataStoreDir tries to open an existing datastore directory
// and acquires the necessary lock.
// return true if it could acquire the lock.
// return error on system failures.
func (d *DataStore) openDataStoreDir() (bool, error) {
	acquired, err := d.acquireFileLock()
	if err != nil {
		return false, err
	}

	return acquired, nil
}

// acquireFileLock tries to acquire a file lock on the datastore directory
// with the desired datastore lock mode.
// return true if it managed to acquire the lock, and false otherwise.
// return error on system failures.
func (d *DataStore) acquireFileLock() (bool, error) {
	var err error
	var ok bool

	d.flck = flock.New(path.Join(d.path, lockFile))
	switch d.lock {
	case ExclusiveLock:
		ok, err = d.flck.TryLock()
	case SharedLock:
		ok, err = d.flck.TryRLock()
	}

	if err != nil {
		return false, err
	}

	return ok, nil
}

// ReadValueFromFile parses the valued corresponding to the given key.
// Return the parsed value and a non-nil error if values is not exist
// or on system failures.
func (d *DataStore) ReadValueFromFile(fileId, key string, valuePos, valueSize uint32) (string, error) {
	bufsz := recfmt.DataFileRecHdr + uint32(len(key)) + valueSize
	buf := make([]byte, bufsz)

	f, err := sio.Open(path.Join(d.path, fileId))
	if err != nil {
		return "", err
	}
	defer f.File.Close()

	f.ReadAt(buf, int64(valuePos))
	data, _, err := recfmt.ExtractDataFileRec(buf)
	if err != nil {
		return "", err
	}

	if data.Value == TompStone {
		return "", errors.New(fmt.Sprintf("%s: %s", data.Key, ErrKeyNotExist))
	}

	return data.Value, nil
}

// Path returns the path of the datastore directory.
func (d *DataStore) Path() string {
	return d.path
}

// Close frees the acquired lock on the datastore directory.
func (d *DataStore) Close() {
	d.flck.Unlock()
}
