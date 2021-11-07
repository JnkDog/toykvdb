package toykvdb

import (
	"io"
	"os"
	"sync"
)

type ToyKVDB struct {
	indexes map[string]int64
	dbFile  *DBFile
	dirPath string
	mu      sync.RWMutex
}

// Open 开启一个数据库实例
func Open(dirPath string) (*ToyKVDB, error) {
	// 如果数据库目录不存在，则新建一个
	if _, err := os.Stat(dirPath); os.IsNotExist(err) {
		if err := os.MkdirAll(dirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// 加载数据文件
	dbFile, err := NewDBFile(dirPath)
	if err != nil {
		return nil, err
	}

	db := &ToyKVDB{
		dbFile:  dbFile,
		indexes: make(map[string]int64),
		dirPath: dirPath,
	}

	// load index to mem
	db.loadIndexesFromFile()
	return db, nil
}

// Merge 合并数据文件，在rosedb当中是 Reclaim 方法
func (db *ToyKVDB) Merge() error {
	// 没有数据，忽略
	if db.dbFile.Offset == 0 {
		return nil
	}

	var (
		validEntries []*Entry
		// offset from zero
		offset int64
	)

	// 读取原数据文件中的 Entry
	for {
		// offset 变量覆盖？？？
		e, err := db.dbFile.Read(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		// 内存中的索引状态是最新的，直接对比过滤出有效的 Entry
		// ok true or false   ----> 有value,
		// off 在文件中的偏离
		if off, ok := db.indexes[string(e.Key)]; ok && (off == offset) {
			validEntries = append(validEntries, e)
		}
		offset += e.GetSize()
	}

	if len(validEntries) > 0 {
		// 新建临时文件
		mergeDBFile, err := NewMergeDBFile(db.dirPath)
		if err != nil {
			return err
		}
		defer os.Remove(mergeDBFile.File.Name())

		// 重新写入有效的 entry
		for _, entry := range validEntries {
			writeOff := mergeDBFile.Offset
			err := mergeDBFile.Write(entry)
			if err != nil {
				return err
			}

			// 更新索引
			db.indexes[string(entry.Key)] = writeOff
		}

		// 获取文件名
		dbFileName := db.dbFile.File.Name()
		// 关闭文件
		db.dbFile.File.Close()
		// 删除旧的数据文件
		os.Remove(dbFileName)

		// 获取文件名
		mergeDBFileName := mergeDBFile.File.Name()
		// 关闭文件
		mergeDBFile.File.Close()
		// 临时文件变更为新的数据文件
		os.Rename(mergeDBFileName, db.dirPath+string(os.PathSeparator)+FileName)

		db.dbFile = mergeDBFile
	}
	return nil
}

// Put 写入数据
func (db *ToyKVDB) Put(key []byte, value []byte) (err error) {
	if len(key) == 0 {
		return
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	// append only, 追加到自Offset后的一段Entry大小的长度
	// offset entry头部首个byte的
	offset := db.dbFile.Offset
	// 封装成 Entry
	entry := NewEntry(key, value, PUT)
	// 追加到数据文件当中
	err = db.dbFile.Write(entry)

	// 写到内存
	db.indexes[string(key)] = offset
	return
}

// Get 取出数据
func (db *ToyKVDB) Get(key []byte) (val []byte, err error) {
	if len(key) == 0 {
		return
	}

	db.mu.RLock()
	defer db.mu.RUnlock()

	// 从内存当中取出索引信息
	offset, ok := db.indexes[string(key)]
	// key 不存在
	if !ok {
		return
	}

	// 从磁盘中读取数据
	var e *Entry
	e, err = db.dbFile.Read(offset)
	if err != nil && err != io.EOF {
		return
	}
	if e != nil {
		val = e.Value
	}
	// return的值在前面已经定义了
	return
}

// Del 删除数据
func (db *ToyKVDB) Del(key []byte) (err error) {
	if len(key) == 0 {
		return
	}

	db.mu.Lock()
	defer db.mu.Unlock()
	// 从内存当中取出索引信息
	_, ok := db.indexes[string(key)]
	// key 不存在，忽略
	if !ok {
		return
	}

	// 封装成 Entry 并写入
	e := NewEntry(key, nil, DEL)
	err = db.dbFile.Write(e)
	if err != nil {
		return
	}

	// 删除内存中的 key
	delete(db.indexes, string(key))
	return
}

// 从文件当中加载索引
func (db *ToyKVDB) loadIndexesFromFile() {
	if db.dbFile == nil {
		return
	}

	// 内存表中的value，记录一个entry在file中的偏移
	var offset int64
	for {
		e, err := db.dbFile.Read(offset)
		if err != nil {
			// 读取完毕
			if err == io.EOF {
				break
			}
			return
		}

		// 设置索引状态
		// offest is map's value
		db.indexes[string(e.Key)] = offset

		// 是否删除标记
		if e.Mark == DEL {
			// 删除内存中的 key
			delete(db.indexes, string(e.Key))
		}

		offset += e.GetSize()
	}
	return
}
