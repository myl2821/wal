package wal

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
)

type decoder struct {
	files []*os.File
	crc   uint32
	table *crc32.Table

	fileIndex int
}

func newDecoder(files []*os.File) (*decoder, error) {
	f := files[0]
	buf := make([]byte, 4)

	n, err := f.Read(buf)
	if err != nil {
		return nil, err
	}

	if 4 != n {
		return nil, io.EOF
	}

	crc := binary.LittleEndian.Uint32(buf)

	return &decoder{
		files:     files,
		crc:       crc,
		table:     crc32.MakeTable(crc32.Castagnoli),
		fileIndex: 0,
	}, nil
}

func (d *decoder) decode() (*Entry, error) {

	f := d.files[d.fileIndex]

	// read length
	buf := make([]byte, 4)
	n, err := f.Read(buf)
	if err != nil {
		if err == io.EOF {

			if d.fileIndex >= len(d.files)-1 {
				return nil, io.EOF
			}

			d.fileIndex++

			buf := make([]byte, 4)

			f = d.files[d.fileIndex]
			n, err := f.Read(buf)
			if err != nil {
				return nil, err
			}

			if 4 != n {
				return nil, io.EOF
			}

			crc := binary.LittleEndian.Uint32(buf)

			if crc != d.crc {
				return nil, errors.New("crc mismatch")
			}

			return d.decode()
		}

		return nil, err
	}

	if n != 4 {
		return nil, io.EOF
	}

	n = int(binary.LittleEndian.Uint32(buf))

	if n == 0 {
		d.files[d.fileIndex].Seek(-4, os.SEEK_CUR)
		return nil, io.EOF
	}

	// read frame
	buf = make([]byte, n)
	nr, err := f.Read(buf)
	if err != nil {
		return nil, err
	}

	if nr != n {
		return nil, io.EOF
	}

	crcStored := binary.LittleEndian.Uint32(buf)
	blob := buf[4:]

	// check crc
	crc := crc32.Update(d.crc, d.table, blob)
	if crc != crcStored {
		return nil, errors.New("CRC Mismatch")
	}

	d.crc = crc
	entry := unmarshal(blob)

	return entry, nil
}

func (d *decoder) offset() (int64, error) {
	return d.files[d.fileIndex].Seek(0, os.SEEK_CUR)
}
