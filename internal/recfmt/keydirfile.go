package recfmt

import (
	"encoding/binary"
	"strconv"
)

// keyDirFileHdr represents the constant header length of keydir file records.
const keyDirFileHdr = 26

// KeyDirRec represents the data parsed from a keydir file record.
type KeyDirRec struct {
	FileId    string
	ValuePos  uint32
	ValueSize uint32
	Tstamp    int64
}

// CompressKeyDirRec compresses the given data into a keydir file record.
func CompressKeyDirRec(key string, rec KeyDirRec) []byte {
	keySize := len(key)
	buf := make([]byte, keyDirFileHdr+keySize)
	fid, _ := strconv.ParseUint(rec.FileId, 10, 64)
	binary.LittleEndian.PutUint64(buf, fid)
	binary.LittleEndian.PutUint16(buf[8:], uint16(keySize))
	binary.LittleEndian.PutUint32(buf[10:], rec.ValueSize)
	binary.LittleEndian.PutUint32(buf[14:], rec.ValuePos)
	binary.LittleEndian.PutUint64(buf[18:], uint64(rec.Tstamp))
	copy(buf[26:], []byte(key))

	return buf
}

// ExtractKeyDirRec extracts the keydir file record into a keydir record.
// Return the keydir record and its length in the file.
func ExtractKeyDirRec(buf []byte) (string, KeyDirRec, int) {
	fileId := strconv.FormatUint(binary.LittleEndian.Uint64(buf), 10)
	keySize := binary.LittleEndian.Uint16(buf[8:])
	valueSize := binary.LittleEndian.Uint32(buf[10:])
	valuePos := binary.LittleEndian.Uint32(buf[14:])
	tstamp := binary.LittleEndian.Uint64(buf[18:])
	key := string(buf[26 : keySize+26])

	return key, KeyDirRec{
		FileId:    fileId,
		ValuePos:  valuePos,
		ValueSize: valueSize,
		Tstamp:    int64(tstamp),
	}, keyDirFileHdr + int(keySize)
}
