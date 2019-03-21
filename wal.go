package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"

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
	sync.Mutex

	dir      string
	walFiles []*os.File
	encoder  *encoder
	decoder  *decoder

	curSeq   uint64
	curIndex uint64
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

	f, err := os.OpenFile(walFilePath, os.O_CREATE|os.O_WRONLY, 0700)
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

	return w.lastFile().Sync()
}

// Append entry into WAL
func (w *WAL) Append(entry *Entry) error {
	if w.encoder == nil {
		return errors.New("Not prepare to read")
	}

	w.Lock()
	defer w.Unlock()

	body := entry.marshal()

	_, err := w.encoder.encode(body)

	if err != nil {
		return err
	}

	w.curIndex = entry.Index

	offset, err := w.encoder.offset()

	if err != nil {
		return err
	}

	if offset > chunkSize {
		return w.cut()
	}

	return nil
}

func Open(dir string) (*WAL, error) {
	files, err := ioutil.ReadDir(dir)
	walFiles := make([]*os.File, 0)
	seqMax := uint64(0)

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		seq, _, err := parseWALName(file.Name())
		if err == nil {
			p := filepath.Join(dir, file.Name())
			f, err := os.Open(p)
			if err != nil {
				for _, f := range walFiles {
					fUnlock(f)
				}
				return nil, err
			}
			flock(f)
			walFiles = append(walFiles, f)

			if seq > seqMax {
				seqMax = seq
			}
		}
	}

	if len(walFiles) == 0 {
		return nil, errors.New("no WAL files")
	}

	decoder, err := newDecoder(walFiles)
	if err != nil {
		return nil, err
	}

	return &WAL{
		dir:      dir,
		walFiles: walFiles,
		decoder:  decoder,
		curSeq:   seqMax,
	}, nil
}

func (w *WAL) ReadAll(start uint64) ([]*Entry, error) {
	entries := make([]*Entry, 0)
	for {
		entry, err := w.decoder.decode()
		if err != nil {
			if err == io.EOF {
				goto prepareToRead
			}
		}

		if entry.Index >= start {
			entries = append(entries, entry)
		}
	}

prepareToRead:

	offset, err := w.decoder.offset()
	if err != nil {
		return nil, err
	}

	w.Close()

	files, err := ioutil.ReadDir(w.dir)
	walFiles := make([]*os.File, 0)
	seqMax := uint64(0)

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		seq, _, err := parseWALName(file.Name())
		if err == nil {
			p := filepath.Join(w.dir, file.Name())
			f, err := os.OpenFile(p, os.O_WRONLY, 0700)
			if err != nil {
				for _, f := range walFiles {
					fUnlock(f)
				}
				return nil, err
			}
			flock(f)
			walFiles = append(walFiles, f)

			if seq > seqMax {
				seqMax = seq
			}
		}
	}

	w.curSeq = seqMax
	walFiles[len(w.walFiles)-1].Seek(offset, os.SEEK_SET)
	w.walFiles = walFiles

	w.encoder = newEncoder(w.decoder.crc, w.walFiles[len(w.walFiles)-1])
	w.decoder = nil

	return entries, nil
}

// Close WAL
func (w *WAL) Close() {
	for _, f := range w.walFiles {
		_ = fUnlock(f)
		_ = f.Close()
	}
}

func (w *WAL) cut() error {
	return nil
}

func parseWALName(str string) (seq uint64, index uint64, err error) {
	if !strings.HasSuffix(str, ".wal") {
		return 0, 0, errors.New("bad WAL name")
	}
	_, err = fmt.Sscanf(str, "%016x-%016x.wal", &seq, &index)
	return seq, index, err
}
