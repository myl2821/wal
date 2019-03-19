package wal

import (
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

func (e *encoder) encodeToFrame(payload []byte) *frame {
	crc := crc32.Update(e.crc, e.table, payload)

	e.crc = crc
	return &frame{
		crc:     crc,
		payload: payload,
	}
}
