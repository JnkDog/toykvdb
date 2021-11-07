package toykvdb

import "os"

const FileName = "toykvdb.data"
const MergeFileName = "toykvdb.data.merge"

type DBFile struct {
	Offset int64
	File   *os.File
}

func newInternal(fileName string) (*DBFile, error) {
	// permission value
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	stat, err := os.Stat(fileName)
	if err != nil {
		return nil, err
	}
	return &DBFile{Offset: stat.Size(), File: file}, nil
}

// DBFile api
func NewDBFile(path string) (*DBFile, error) {
	fileName := path + string(os.PathSeparator) + FileName
	return newInternal(fileName)
}

func NewMergeDBFile(path string) (*DBFile, error) {
	fileName := path + string(os.PathSeparator) + MergeFileName
	return newInternal(fileName)
}

// read data
func (df *DBFile) Read(offset int64) (e *Entry, err error) {
	buf := make([]byte, entryHeaderSize)
	// read from the offset of file and write to buf
	if _, err = df.File.ReadAt(buf, offset); err != nil {
		return
	}

	// full in the header info
	if e, err = Decode(buf); err != nil {
		return
	}

	offset += entryHeaderSize
	if e.KeySize > 0 {
		key := make([]byte, e.KeySize)
		if _, err = df.File.ReadAt(key, offset); err != nil {
			return
		}
		e.Key = key
	}

	offset += int64(e.KeySize)
	if e.ValueSize > 0 {
		value := make([]byte, e.ValueSize)
		if _, err = df.File.ReadAt(value, offset); err != nil {
			return
		}
		e.Value = value
	}

	// why dont add offset ValueSize ?
	// 无所谓最后加不加Value的offest,后期传入的offest自动修正到下一个entry
	// offset += int64(e.KeySize)
	return
}

// write one entry
func (df *DBFile) Write(e *Entry) (err error) {
	enc, err := e.Encode()
	if err != nil {
		return err
	}
	_, err = df.File.WriteAt(enc, df.Offset)
	df.Offset += e.GetSize()
	return
}
