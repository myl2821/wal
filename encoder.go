package wal

import (
	"encoding/binary"
	"hash/crc32"
	"os"
)

type encoder struct {
	crc   uint32
	f     *os.File
	table *crc32.Table
}

func newEncoder(prevCrc uint32, f *os.File) *encoder {
	return &encoder{
		crc:   prevCrc,
		f:     f,
		table: crc32.MakeTable(crc32.Castagnoli),
	}
}

func (e *encoder) encode(payload []byte) (int, error) {
	// encode crc
	crc := crc32.Update(e.crc, e.table, payload)
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, crc)

	// encode length
	buf2 := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf2, uint32(len(payload)+len(buf)))

	buf2 = append(buf2, buf...)
	frame := append(buf2, payload...)

	n, err := e.f.Write(frame)
	if err != nil {
		return 0, err
	}

	err = e.f.Sync()
	if err != nil {
		return 0, err
	}

	e.crc = crc

	return n, err
}

func (e *encoder) offset() (int64, error) {
	return e.f.Seek(0, os.SEEK_CUR)
}
