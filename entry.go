package wal

import (
	"bytes"
	"encoding/binary"
)

type Entry struct {
	Index   uint64
	Payload []byte
}

func NewEntry(index uint64, payload []byte) *Entry {
	return &Entry{
		Index:   index,
		Payload: payload,
	}
}

func (e *Entry) marshal() []byte {
	buffer := new(bytes.Buffer)
	_ = binary.Write(buffer, binary.LittleEndian, e.Index)
	_, _ = buffer.Write(e.Payload)
	return buffer.Bytes()
}

func unmarshal(p []byte) *Entry {
	index := binary.LittleEndian.Uint64(p)
	return &Entry{
		Index:   index,
		Payload: p[8:],
	}
}
