package wal

import (
	"bytes"
	"encoding/binary"
)

type frame struct {
	crc     uint32
	payload []byte
}

func (f *frame) marshal() []byte {
	buffer := new(bytes.Buffer)
	_ = binary.Write(buffer, binary.LittleEndian, f.crc)
	_, _ = buffer.Write(f.payload)
	return buffer.Bytes()
}

func unmarshal(p []byte) *frame {
	crc := binary.LittleEndian.Uint32(p)
	return &frame{
		crc:     crc,
		payload: p[4:],
	}
}
