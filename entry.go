package wal

import (
	"bytes"
	"encoding/binary"
)

type Entry struct {
	index   uint32
	payload []byte
}

func NewEntry(index uint32, payload []byte) *Entry {
	return &Entry{
		index:   index,
		payload: payload,
	}
}

func (e *Entry) marshal() []byte {
	buffer := new(bytes.Buffer)
	_ = binary.Write(buffer, binary.LittleEndian, e.index)
	_, _ = buffer.Write(e.payload)
	return buffer.Bytes()
}

func unmarshal(p []byte) *Entry {
	index := binary.LittleEndian.Uint32(p)
	return &Entry{
		index:   index,
		payload: p[4:],
	}
}
