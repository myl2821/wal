package wal_test

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/myl2821/wal"
	"github.com/stretchr/testify/assert"
)

func TestCreate(t *testing.T) {
	p, err := ioutil.TempDir(os.TempDir(), "waltest")
	assert.Nil(t, err)

	defer os.RemoveAll(p)

	w, err := wal.Create(p)
	assert.Nil(t, err)

	entry := wal.NewEntry(0, []byte("hello"))
	err = w.Append(entry)

	assert.Nil(t, err)
}
