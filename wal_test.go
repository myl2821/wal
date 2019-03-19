package wal_test

import (
	"io"
	"io/ioutil"
	"os"
	"testing"

	"github.com/myl2821/wal"
	"github.com/stretchr/testify/assert"
)

func TestWAL(t *testing.T) {
	p, err := ioutil.TempDir(os.TempDir(), "waltest")
	assert.Nil(t, err)

	defer os.RemoveAll(p)

	// Create
	w, err := wal.Create(p)
	assert.Nil(t, err)

	// Write
	entry := wal.NewEntry(0, []byte("hello"))
	err = w.Append(entry)
	assert.Nil(t, err)

	entry = wal.NewEntry(1, []byte("world"))
	err = w.Append(entry)
	assert.Nil(t, err)

	w.Close()

	// Read
	w, err = wal.Open(p)
	assert.Nil(t, err)

	entry, err = w.Read()
	assert.Nil(t, err)
	assert.Equal(t, uint32(0), entry.Index)
	assert.Equal(t, []byte("hello"), entry.Payload)

	entry, err = w.Read()
	assert.Nil(t, err)
	assert.Equal(t, uint32(1), entry.Index)
	assert.Equal(t, []byte("world"), entry.Payload)

	entry, err = w.Read()
	assert.Nil(t, entry)
	assert.Equal(t, io.EOF, err)
}
