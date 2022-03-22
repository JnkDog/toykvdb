package toykvdb

import (
	"testing"
	"time"
)

func TestNewEntry(t *testing.T) {
	timestamp := time.Now().UnixMilli()
	key := "ckh"
	value := "fzzfs"
	entry := NewEntry(timestamp, []byte(key), []byte(value))

	if entry != nil {
		t.Logf("%v is created", entry)
	}

}

func TestCRC32(t *testing.T) {
	timestamp := time.Now().UnixMilli()
	key := "ckh"
	value := "fzzfs"
	crc32 := getCRC32(timestamp, []byte(key), []byte(value))
	t.Log(crc32)
}
