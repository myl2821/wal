package wal

import (
	"encoding/binary"
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
	encoder  *encoder

	curOffset uint32
	curIndex  uint32
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

	f, err := os.OpenFile(walFilePath, os.O_CREATE|os.O_WRONLY, 700)
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

	w := &WAL{
		dir:      dir,
		dirFile:  dirFile,
		walFiles: []*os.File{f},
		encoder:  newEncoder(0, f),
	}

	err = w.writeCrc(0)
	if err != nil {
		return nil, err
	}

	return w, nil
}

func walName(seq uint64, idx uint64) string {
	return fmt.Sprintf("%016x-%016x.wal", seq, idx)
}

func (w *WAL) lastFile() *os.File {
	return w.walFiles[len(w.walFiles)-1]
}

func (w *WAL) writeCrc(crc uint32) error {
	var buf = make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, crc)
	_, err := w.lastFile().Write(buf)
	if err != nil {
		return err
	}

	w.curOffset += 4

	return w.lastFile().Sync()
}

// Append entry into WAL
func (w *WAL) Append(entry *Entry) error {
	body := entry.marshal()

	n, err := w.encoder.encode(body)

	if err != nil {
		return err
	}

	w.curOffset += uint32(n)
	w.curIndex = entry.index

	return nil
}
