package recfmt

import "encoding/binary"

// HintFileRecHdr represents the constant header length of hint file records.
const HintFileRecHdr = 18

// HintRec represents the data parsed from a hint file record.
type HintRec struct {
	key       string
	keySize   uint16
	tstamp    int64
	valuePos  uint32
	valueSize uint32
}

// CompressHintFileRec compresses the given data into a hint file record.
func CompressHintFileRec(key string, rec KeyDirRec) []byte {
	buf := make([]byte, HintFileRecHdr+len(key))
	binary.LittleEndian.PutUint64(buf, uint64(rec.Tstamp))
	binary.LittleEndian.PutUint16(buf[8:], uint16(len(key)))
	binary.LittleEndian.PutUint32(buf[10:], rec.ValueSize)
	binary.LittleEndian.PutUint32(buf[14:], rec.ValuePos)
	copy(buf[18:], []byte(key))

	return buf
}

// ExtractDataFileRec extracts the hint file record into a hint record.
// Return the hint record and its length in the file.
func ExtractHintFileRec(buf []byte) (string, KeyDirRec, int) {
	tstamp := binary.LittleEndian.Uint64(buf)
	keySize := binary.LittleEndian.Uint16(buf[8:])
	valueSize := binary.LittleEndian.Uint32(buf[10:])
	valuePos := binary.LittleEndian.Uint32(buf[14:])
	key := string(buf[HintFileRecHdr : HintFileRecHdr+keySize])

	return key, KeyDirRec{
		ValuePos:  valuePos,
		ValueSize: valueSize,
		Tstamp:    int64(tstamp),
	}, HintFileRecHdr + int(keySize)
}
