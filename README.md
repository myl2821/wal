# WAL

Package wal provides an implementation of a write ahead log.

## Usage

A WAL is created at a particular directory and is made up of a number of
segmented WAL files. Inside of each file entries are appended
to it with the Append method:

```

metadata := []byte{}
w, err := wal.Create("/tmp/wal")
...
entry := wal.NewEntry(0, []byte("hello"))
err := w.Append(s, entry)

```

When a user has finished using a WAL it must be closed:

```

w.Close()

```

Each WAL file is a stream of WAL records. A WAL record is a length field with a wal record.
The record contains a CRC and a data payload.

WAL files are placed inside of the directory in the following format:

    $seq-$index.wal

The first WAL file to be created will be 0000000000000000-0000000000000000.wal
indicating an initial sequence of 0 and an initial index of 0. The first
entry written to WAL MUST have index 0.

WAL will cut its current tail wal file if its size exceeds 64MB. This will increment an internal
sequence number and cause a new file to be created.

If the last index saved
was 0x20 and this is the first time cut has been called on this WAL then the sequence will
increment from 0x0 to 0x1. The new file will be: 0000000000000001-0000000000000021.wal.

If a second cut issues 0x10 entries with incremental index later then the file will be called:

    0000000000000002-0000000000000031.wal.


At a later time a WAL can be opened at a particular index.

```

w, err := wal.Open("/tmp/wal", 0x10)

```

Additional items cannot be Saved to this WAL until all of the items from the given
snapshot to the end of the WAL are read first:

```

ents, err := w.ReadAll()

```


