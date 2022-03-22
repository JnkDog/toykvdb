package toykvdb

import (
	"encoding/binary"
	"hash/crc32"
)

// the bytes that metadata take
const (
	CrcBytes       = 4
	TimestampBytes = 8
	KeySizeBytes   = 4
	ValueSizeBytes = 4
)

const HeaderSizeBytes = CrcBytes + TimestampBytes + KeySizeBytes + ValueSizeBytes

type KVLog struct {
	Key, Value []byte
}

// Entry | CRC 4 | TS 8 | key size 4 | value size 4 | kv log
type Entry struct {
	CRC32         int32 // CRC算法怎么算出来
	Timestamp     int64
	KeySizeByte   int32
	ValueSizeByte int64
	KVLog
}

func NewEntry(timestamp int64, key []byte, value []byte) *Entry {
	return &Entry{
		Timestamp: timestamp,
		KVLog: KVLog{
			key,
			value,
		},
	}
}

func getCRC32(timestamp int64, key []byte, value []byte) int32 {
	keyLogByte := len(key)
	valueLogByte := len(value)
	var IEEETable = crc32.MakeTable(crc32.IEEE)
	buf := make([]byte, TimestampBytes+KeySizeBytes+ValueSizeBytes+keyLogByte+valueLogByte)
	binary.BigEndian.PutUint64(buf, uint64(timestamp))
	binary.BigEndian.PutUint32(buf, uint32(keyLogByte))
	binary.BigEndian.PutUint32(buf, uint32(valueLogByte))
	buf = append(buf, key...)
	buf = append(buf, value...)

	entryCRC32 := crc32.Checksum(buf, IEEETable)

	return int32(entryCRC32)
}
