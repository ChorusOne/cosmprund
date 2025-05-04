package cmd

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/cockroachdb/pebble"
	dbm "github.com/cometbft/cometbft-db"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

const (
	rocksDBMagic  = "\xf7\xcf\xf4\x85\xb7\x41\xe2\x88"
	pebbleDBMagic = "\xf0\x9f\xaa\xb3\xf0\x9f\xaa\xb3" // 🪳🪳
	levelDBMagic  = "\x57\xfb\x80\x8b\x24\x75\x47\xdb"
	magicLen      = 8
)

type DBFormat uint

const (
	Invalid DBFormat = iota
	FormatPebbleDB
	FormatRocksDB
	FormatLevelDB
)

func OpenDB(path, dbName string, format DBFormat) (DBAdapter, error) {
	dbPath := filepath.Join(path, dbName)
	switch format {
	case FormatRocksDB:
		fallthrough
	case FormatPebbleDB:
		db, err := pebble.Open(dbPath, nil)
		if err != nil {
			return nil, err
		}
		return NewPebbleDBAdapter(db), nil
	case FormatLevelDB:
		o := opt.Options{
			DisableSeeksCompaction: true,
		}
		db, err := dbm.NewGoLevelDBWithOpts(dbName, path, &o)
		if err != nil {
			return nil, err
		}
		return NewCometDBAdapter(db), nil
	}
	return nil, fmt.Errorf("db type not supported: %v", format)
}

func GetFormat(path string) (DBFormat, error) {
	magic, err := getMagic(path)
	if err != nil {
		return Invalid, err
	}
	switch string(magic) {
	case rocksDBMagic:
		return FormatRocksDB, nil
	case pebbleDBMagic:
		return FormatPebbleDB, nil
	case levelDBMagic:
		return FormatLevelDB, nil

	}
	return Invalid, fmt.Errorf("db type not supported, magic: %v", magic)
}

// Find a file with .sst or .ldb extension
// read magicLen bytes from the end, return them
func getMagic(path string) ([]byte, error) {
	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()

		if !strings.HasSuffix(name, ".sst") && !strings.HasSuffix(name, ".ldb") {
			continue
		}
		parts := strings.Split(name, ".")
		if len(parts) < 2 {
			continue
		}
		return readMagic(filepath.Join(path, name))
	}

	return nil, fmt.Errorf("No .sst or .ldb files found on %s; cannot infer db format", path)
}

func readMagic(path string) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return nil, err
	}

	fileSize := fileInfo.Size()

	if fileSize < 8 {
		return nil, fmt.Errorf("File is less than 8 bytes in size: %d bytes\n", fileSize)
	}

	_, err = file.Seek(-magicLen, io.SeekEnd)
	if err != nil {
		return nil, err
	}

	lastBytes := make([]byte, magicLen)
	_, err = io.ReadFull(file, lastBytes)
	if err != nil {
		return nil, err
	}
	return lastBytes, nil
}
