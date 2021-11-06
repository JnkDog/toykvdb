package toykvdb

import "encoding/binary"

// TOBE
const (
	PUT uint16 = iota
	DEL
)

const (
	keySizeBytes   = 4
	valueSizeBytes = 4
	markBytes      = 2
)

// Header size KeySize(4B) + ValueSize(4B) + Mark(2B) = 10B
const entryHeaderSize = keySizeBytes + valueSizeBytes + markBytes

// Entry The storage struct
type Entry struct {
	Key   []byte
	Value []byte
	// unsigned int 32 bits
	KeySize   uint32
	ValueSize uint32
	Mark      uint16
}

func NewEntry(key, value []byte, mark uint16) *Entry {
	return &Entry{
		Key:   key,
		Value: value,
		// header data
		KeySize:   uint32(len(key)),
		ValueSize: uint32(len(value)),
		Mark:      mark,
	}
}

func (e *Entry) GetSize() int64 {
	// bytes size ---  one entry size
	return int64(entryHeaderSize + e.KeySize + e.ValueSize)
}

// Encode entry, return byte array
func (e *Entry) Encode() ([]byte, error) {
	buf := make([]byte, e.GetSize())
	// save header info
	binary.BigEndian.PutUint32(buf[0:4], e.KeySize)
	binary.BigEndian.PutUint32(buf[4:8], e.ValueSize)
	binary.BigEndian.PutUint16(buf[8:10], e.Mark)
	// save key data to buf
	copy(buf[entryHeaderSize:entryHeaderSize+e.KeySize], e.Key)
	// save value data to buf
	copy(buf[entryHeaderSize+e.KeySize:], e.Value)

	return buf, nil
}

func Decode(buf []byte) (*Entry, error) {
	keySize := binary.BigEndian.Uint32(buf[0:4])
	valueSize := binary.BigEndian.Uint32(buf[4:8])
	mark := binary.BigEndian.Uint16(buf[8:10])
	// missing the key and value data
	return &Entry{
		KeySize:   keySize,
		ValueSize: valueSize,
		Mark:      mark,
	}, nil
}
