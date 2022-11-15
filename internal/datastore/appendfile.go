package datastore

import (
	"fmt"
	"os"
	"path"
	"time"

	"github.com/zaher1307/bitcask/internal/recfmt"
	"github.com/zaher1307/bitcask/internal/sio"
)

const (
	// Merge represents that the file type is a merge file.
	Merge AppendType = 0
	// Merge represents that the file type is an active file.
	Active AppendType = 1

	// maxFileSize represents the maximum size for each file.
	maxFileSize = 10 * 1024
)

type (
	// AppendType represents the type of the append file.
	AppendType int

	// AppendFile contains the metadata about the append file.
	AppendFile struct {
		fileWrapper *sio.File
		hintWrapper *sio.File
		fileName    string
		filePath    string
		fileFlags   int
		appendType  AppendType
		currentPos  int
		currentSize int
	}
)

// WriteData writes a data record to the given append file.
// Return the position of the written data.
// Return error on system failures.
func (a *AppendFile) WriteData(key, value string, tstamp int64) (int, error) {
	rec := recfmt.CompressDataFileRec(key, value, tstamp)

	if a.fileWrapper == nil || len(rec)+a.currentSize > maxFileSize {
		err := a.newAppendFile()
		if err != nil {
			return 0, err
		}
	}

	n, err := a.fileWrapper.Write(rec)
	if err != nil {
		return 0, err
	}

	writePos := a.currentPos
	a.currentPos += n
	a.currentSize += n

	return writePos, nil
}

// WriteData writes a hint record to the hint file
// associated with the given append file.
// Return error on system failures.
func (a *AppendFile) WriteHint(key string, rec recfmt.KeyDirRec) error {
	buf := recfmt.CompressHintFileRec(key, rec)
	_, err := a.hintWrapper.Write(buf)
	if err != nil {
		return err
	}

	return nil
}

// newAppendFile creates new append file.
// create a hint file associated with it if the file type is merge.
// return error on system failures.
func (a *AppendFile) newAppendFile() error {
	if a.fileWrapper != nil {
		err := a.fileWrapper.File.Close()
		if err != nil {
			return err
		}
		if a.appendType == Merge {
			err := a.hintWrapper.File.Close()
			if err != nil {
				return err
			}
		}
	}

	tstamp := time.Now().UnixMicro()
	fileName := fmt.Sprintf("%d.data", tstamp)
	file, err := sio.OpenFile(path.Join(a.filePath, fileName), a.fileFlags, os.FileMode(0666))
	if err != nil {
		return err
	}

	if a.appendType == Merge {
		hintName := fmt.Sprintf("%d.hint", tstamp)
		hint, err := sio.OpenFile(path.Join(a.filePath, hintName), a.fileFlags, os.FileMode(0666))
		if err != nil {
			return err
		}
		a.hintWrapper = hint
	}

	a.fileWrapper = file
	a.fileName = fileName
	a.currentPos = 0
	a.currentSize = 0

	return nil
}

// Name returns the name of the append file.
func (a *AppendFile) Name() string {
	return a.fileName
}

// Sync flushes the data written to the append file to the disk.
func (a *AppendFile) Sync() error {
	if a.fileWrapper != nil {
		return a.fileWrapper.File.Sync()
	}

	return nil
}

// Close closes the append file and its associated hint file if exists.
func (a *AppendFile) Close() {
	if a.fileWrapper != nil {
		a.fileWrapper.File.Close()
		if a.appendType == Merge {
			a.hintWrapper.File.Close()
		}
	}
}
