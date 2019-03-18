package wal

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"go.uber.org/zap"
)

var logger *zap.SugaredLogger

const (
	chunkSize = 64 << 20 // 64MB
)

func init() {
	logger = zap.NewExample().Sugar()
}

// WAL is a logical representation of the stable storage.
// WAL is either in read mode or append mode but not both.
// A newly created WAL is in append mode, and ready for appending records.
// A just opened WAL is in read mode, and ready for reading records.
// The WAL will be ready for appending after reading out all the previous records.
type WAL struct {
	dir      string
	dirFile  *os.File
	walFiles []*os.File
}

// Create creates a WAL ready for appending records.
func Create(dir string) (*WAL, error) {
	stat, err := os.Stat(dir)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}

	if err == nil { // file or dir exists
		if !stat.IsDir() {
			return nil, os.ErrInvalid
		}

		files, e := ioutil.ReadDir(dir)
		if e != nil {
			return nil, e
		}

		if len(files) != 0 {
			return nil, os.ErrExist
		}

		logger.Infow("using existed directory", "path", dir)
	} else {
		logger.Infow("Create wal directory", "path", dir)
		err = os.MkdirAll(dir, 0700)
		if err != nil {
			return nil, err
		}
	}

	dirFile, err := os.Open(dir)
	if err != nil {
		return nil, err
	}
	err = dirFile.Sync()
	if err != nil {
		return nil, err
	}

	walFilePath := filepath.Join(dir, walName(0, 0))

	f, err := os.OpenFile(walFilePath, os.O_CREATE|os.O_RDONLY, 700)
	if err != nil {
		return nil, err
	}

	err = flock(f)
	if err != nil {
		return nil, err
	}

	err = os.Truncate(walFilePath, chunkSize)
	if err != nil {
		return nil, err
	}

	return &WAL{
		dir:      dir,
		dirFile:  dirFile,
		walFiles: []*os.File{f},
	}, nil
}

func walName(seq uint64, idx uint64) string {
	return fmt.Sprintf("%016x-%016x.wal", seq, idx)
}
