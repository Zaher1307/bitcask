// Package recfmt(record format) provides functionality to compress and extrct datastore file records.
package recfmt

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

// DataFileRecHdr represents the constant header length of data file records.
const DataFileRecHdr = 18

// errDataCorruption happens whenever a data file record is corrupted.
var errDataCorruption = errors.New("corrution detected: datastore files are corrupted")

// DataRec represents the data parsed from a data file record.
type DataRec struct {
	Key       string
	Value     string
	Tstamp    int64
	KeySize   uint16
	ValueSize uint32
}

// CompressDataFileRec compresses the given data into a data file record.
func CompressDataFileRec(key, value string, tstamp int64) []byte {
	buf := make([]byte, DataFileRecHdr+len(key)+len(value))

	binary.LittleEndian.PutUint64(buf[4:], uint64(tstamp))
	binary.LittleEndian.PutUint16(buf[12:], uint16(len(key)))
	binary.LittleEndian.PutUint32(buf[14:], uint32(len(value)))
	copy(buf[DataFileRecHdr:], []byte(key))
	copy(buf[DataFileRecHdr+len(key):], []byte(value))

	checkSum := crc32.ChecksumIEEE(buf[4:])
	binary.LittleEndian.PutUint32(buf, checkSum)

	return buf
}

// ExtractDataFileRec extracts the data file record into a data record.
// Return the data record and its length in the file.
// Return an error whenever the data is corrupted.
func ExtractDataFileRec(buf []byte) (*DataRec, uint32, error) {
	parsedSum := binary.LittleEndian.Uint32(buf)
	tstamp := binary.LittleEndian.Uint64(buf[4:])
	keySize := binary.LittleEndian.Uint16(buf[12:])
	valueSize := binary.LittleEndian.Uint32(buf[14:])
	key := string(buf[DataFileRecHdr : DataFileRecHdr+keySize])
	valueOffset := uint32(DataFileRecHdr + keySize)
	value := string(buf[valueOffset : valueOffset+valueSize])

	err := validateCheckSum(parsedSum, buf[4:DataFileRecHdr+uint32(keySize)+valueSize])
	if err != nil {
		return nil, 0, err
	}

	return &DataRec{
		Key:       key,
		Value:     value,
		Tstamp:    int64(tstamp),
		KeySize:   keySize,
		ValueSize: valueSize,
	}, DataFileRecHdr + valueSize + uint32(keySize), nil
}

// validateCheckSum runs the validate check on the data.
// return an error if the data is corrupted.
func validateCheckSum(parsedSum uint32, rec []byte) error {
	wantedSum := crc32.ChecksumIEEE(rec)
	if parsedSum != wantedSum {
		return errDataCorruption
	}

	return nil
}
