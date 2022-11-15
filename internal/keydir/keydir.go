// Package keydir provides several mechanisms to build the keydir map of bitcask datastores.
package keydir

import (
	"fmt"
	"os"
	"path"
	"strings"

	"github.com/zaher1307/bitcask/internal/recfmt"
	"github.com/zaher1307/bitcask/internal/sio"
)

const (
	// PrivateKeyDir specifies that the keydir is owned by a writer process and will not be shared.
	PrivateKeyDir KeyDirPrivacy = 0
	// SharedKeyDir specifies that the keydir is owned by a reader proccess and will
	// available writers to used it instead of parsing the whole datastore files.
	SharedKeyDir KeyDirPrivacy = 1

	// keyDirFile is the name of the file used to share the keydir map.
	keyDirFile = "keydir"

	// data represents that the file is a data file.
	data fileType = 0
	// hint represents that the file is a hint file.
	hint fileType = 1
)

type (
	// fileType specifies whether the file is a data or hint file.
	fileType int

	// KeyDirPrivacy specifies whether the keydir is private or shared.
	KeyDirPrivacy int

	// KeyDir represents the map used by the bitcask.
	KeyDir map[string]recfmt.KeyDirRec
)

// New creates a new keydir map from the given datastore.
// Select the convenient mechanism of building the keydir.
// Share the built keydir map if shared privacy is specified.
// Return an error on system failures.
func New(dataStorePath string, privacy KeyDirPrivacy) (KeyDir, error) {
	k := KeyDir{}

	okay, err := k.keyDirFileBuild(dataStorePath)
	if err != nil {
		return nil, err
	}
	if okay {
		return k, nil
	}

	err = k.dataStoreFilesBuild(dataStorePath)
	if err != nil {
		return nil, err
	}

	if privacy == SharedKeyDir {
		k.share(dataStorePath)
	}

	return k, nil
}

// keyDirFileBuild tries to build the keydir from the shared keydir file.
// return false if there is no keydir or the existing keydir is old.
// return an error on system failures.
func (k KeyDir) keyDirFileBuild(dataStorePath string) (bool, error) {
	data, err := os.ReadFile(path.Join(dataStorePath, keyDirFile))
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}

	okay, err := isOld(dataStorePath)
	if err != nil || !okay {
		return false, nil
	}

	i := 0
	n := len(data)
	for i < n {
		key, rec, recLen := recfmt.ExtractKeyDirRec(data[i:])
		k[key] = rec
		i += recLen
	}

	return true, nil
}

// isOld specifies whether the keydir file contains the data
// that represents the current state of the datastore directory.
// if the keydir is old this means that write operations happened
// so this file is not representing the current state and should
// be ignored when building the current keydir.
func isOld(dataStorePath string) (bool, error) {
	dataStoreStat, err := os.Stat(dataStorePath)
	if err != nil {
		return false, err
	}

	keydirStat, err := os.Stat(path.Join(dataStorePath, "keydir"))
	if err != nil {
		return false, err
	}

	return keydirStat.ModTime().Before(dataStoreStat.ModTime()), nil
}

// dataStoreFilesBuild is another mechanism of building the keydir.
// it uses the current data and hint files to build it.
// it prefer the hint files on data files.
// return and error on system failures.
func (k KeyDir) dataStoreFilesBuild(dataStorePath string) error {
	dataStore, err := os.Open(dataStorePath)
	if err != nil {
		return err
	}
	defer dataStore.Close()
	files, err := dataStore.Readdir(0)
	if err != nil {
		return err
	}

	fileNames := make([]string, 0)
	for _, file := range files {
		if file.Name()[0] != '.' {
			fileNames = append(fileNames, file.Name())
		}
	}

	err = k.parseFiles(dataStorePath, categorizeFiles(fileNames))
	if err != nil {
		return err
	}

	return nil
}

// parseFiles parses the data from the given data and hint files
// to create the keydir map.
// return and error on system failures.
func (k KeyDir) parseFiles(dataStorePath string, files map[string]fileType) error {
	for name, ftype := range files {
		switch ftype {
		case data:
			err := k.parseDataFile(dataStorePath, name)
			if err != nil {
				return err
			}
		case hint:
			err := k.parseHintFile(dataStorePath, name)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// parseDataFile parses the data from a data files.
// return and error on system failures.
func (k KeyDir) parseDataFile(dataStorePath, name string) error {
	data, err := os.ReadFile(path.Join(dataStorePath, name))
	if err != nil {
		return err
	}

	i := 0
	n := len(data)
	for i < n {
		rec, recLen, err := recfmt.ExtractDataFileRec(data[i:])
		if err != nil {
			return err
		}

		old, isExist := k[rec.Key]
		if !isExist || old.Tstamp < rec.Tstamp {
			k[rec.Key] = recfmt.KeyDirRec{
				FileId:    name,
				ValuePos:  uint32(i),
				ValueSize: rec.ValueSize,
				Tstamp:    rec.Tstamp,
			}
		}
		i += int(recLen)
	}

	return nil
}

// parseHintFile parses the data from hint files.
// return and error on system failures.
func (k KeyDir) parseHintFile(dataStorePath, name string) error {
	data, err := os.ReadFile(path.Join(dataStorePath, name))
	if err != nil {
		return err
	}

	i := 0
	n := len(data)
	for i < n {
		key, rec, recLen := recfmt.ExtractHintFileRec(data[i:])
		rec.FileId = fmt.Sprintf("%s.data", strings.Trim(name, ".hint"))
		k[key] = rec
		i += recLen
	}

	return nil
}

// categorizeFiles specifies whether the file is data or hint file.
func categorizeFiles(allFiles []string) map[string]fileType {
	res := make(map[string]fileType)

	hintFiles := make(map[string]int)
	for _, file := range allFiles {
		if strings.HasSuffix(file, ".hint") {
			fileWithoutExt := strings.Trim(file, ".hint")
			hintFiles[fileWithoutExt] = 1
			res[file] = hint
		}
	}

	for _, file := range allFiles {
		if strings.HasSuffix(file, ".data") {
			if _, okay := hintFiles[strings.Trim(file, ".data")]; !okay {
				res[file] = data
			}
		}
	}

	return res
}

// share writes the keydir map data in keydir file to be used by other readers.
// return an error on system failures.
func (k KeyDir) share(dataStorePath string) error {
	flags := os.O_CREATE | os.O_RDWR
	perm := os.FileMode(0666)
	file, err := sio.OpenFile(path.Join(dataStorePath, "keydir"), flags, perm)
	if err != nil {
		return err
	}

	for key, rec := range k {
		buf := recfmt.CompressKeyDirRec(key, rec)
		_, err := file.Write(buf)
		if err != nil {
			return err
		}
	}

	return nil
}
