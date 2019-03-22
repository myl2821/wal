package wal_test

import (
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

	entries, err := w.ReadAll(0)
	assert.Nil(t, err)
	assert.Equal(t, uint64(0), entries[0].Index)
	assert.Equal(t, []byte("hello"), entries[0].Payload)
	assert.Equal(t, uint64(1), entries[1].Index)
	assert.Equal(t, []byte("world"), entries[1].Payload)

	// write after read
	entry = wal.NewEntry(2, []byte("123"))
	err = w.Append(entry)
	assert.Nil(t, err)

	w.Close()

	// Read
	w, err = wal.Open(p)
	assert.Nil(t, err)

	entries, err = w.ReadAll(1)
	assert.Equal(t, uint64(1), entries[0].Index)
	assert.Equal(t, []byte("world"), entries[0].Payload)
	assert.Equal(t, uint64(2), entries[1].Index)
	assert.Equal(t, []byte("123"), entries[1].Payload)
}
