package hasty

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"os"
)

const (
	// recordLenSize is a record length in bytes needed to encode uint32.
	recordLenSize = 4
	// kvDelimeter is a delimiter between key and value in segment's record.
	kvDelimeter = byte('\x00')
)

// segment represents a log file which is stored in SSTable format.
type segment struct {
	// path is a path to the segment file.
	path string
	f    *os.File
	// index is a hash map which is used to index keys on disk.
	// Every key is mapped to a byte offset in the segment file where value is stored.
	index map[string]int64

	encode func(out io.Writer, rec *record) error
}

// openReadonlySegment opens a segment file for reading.
func openReadonlySegment(path string) (*segment, error) {
	s := segment{
		path:  path,
		index: make(map[string]int64),
	}

	var err error
	if s.f, err = os.Open(path); err != nil {
		return nil, err
	}
	return &s, nil
}

// openReadonlySegment opens a segment file for writing.
func openWriteonlySegment(path string) (*segment, error) {
	s := segment{
		path: path,
	}

	var err error
	if s.f, err = os.Create(path); err != nil {
		return nil, err
	}
	return &s, nil
}

// Close closes a segment file which was opened either for reads or writes.
func (s *segment) Close() error {
	return s.f.Close()
}

// Read reads from underlying segment file without decoding bytes.
func (s *segment) Read(p []byte) (n int, err error) {
	return s.f.Read(p)
}

// Write writes into underlying segment file.
// Write can't encode bytes because it doesn't know its structure, so it's callers responsibility to
// encode records and then calling Flush at the end to commit the changes on disk.
func (s *segment) Write(p []byte) (n int, err error) {
	return s.f.Write(p)
}

// Flush commits the current contents of the segment to disk.
func (s *segment) Flush() error {
	return s.f.Sync()
}

// record represents a key-value pair in a segment file.
type record struct {
	// key represents priority to arrange records in priority queue during segment merging.
	// When there are two records with the same key (equal priorities), then their order field is compared.
	key   string
	value []byte
	// order is a segment number used during merging.
	// It is used to return records in the order they were originally added.
	order int
}

// encode prepares the key value pair to be stored in a file.
// First 4 bytes store the length of a record. The rest of bytes are key-value (zero byte is used as a delimeter).
func encode(out io.Writer, rec *record) (err error) {
	blen := recordLen(rec.key, rec.value)
	if err = binary.Write(out, binary.LittleEndian, blen); err != nil {
		return err
	}

	ew := &errWriter{Writer: out}
	ew.Write([]byte(rec.key))
	ew.Write([]byte{kvDelimeter})
	ew.Write(rec.value)
	return ew.err
}

// decode returns key-value from encoded byte slice b.
func decode(b []byte) *record {
	b = b[recordLenSize:]
	i := bytes.IndexByte(b, kvDelimeter)
	if i == -1 {
		return nil
	}

	rec := record{
		key: string(b[0:i]),
		// Skip delimeter and read till the end.
		value: b[i+1:],
	}
	return &rec
}

// recordLen is used to read next record in a segment file.
// Max record len is 4,294,967,295 (4.295 GB).
// For example, start from 0 offset, read key-value pair, move to offset += recordLen(key, value).
func recordLen(key string, value []byte) uint32 {
	return recordLenSize + uint32(len(key)) + 1 + uint32(len(value))
}

// split is a split function used to tokenize the input from segment file.
func split(data []byte, atEOF bool) (advance int, token []byte, err error) {
	for i := 0; i < len(data); i++ {
		if data[i] == kvDelimeter {
			return i + 1, data[:i], nil
		}
	}
	if !atEOF {
		return 0, nil, nil
	}
	// There is one final token to be delivered, which may be the empty string.
	// Returning bufio.ErrFinalToken here tells Scan there are no more tokens after this
	// but does not trigger an error to be returned from Scan itself.
	return 0, data, bufio.ErrFinalToken
}

// errWriter fulfils the io.Writer contract so it can be used to wrap an existing io.Writer.
// errWriter passes writes through to its underlying writer until an error is detected.
// From that point on, it discards any writes and returns the previous error.
// See https://dave.cheney.net/2019/01/27/eliminate-error-handling-by-eliminating-errors.
type errWriter struct {
	io.Writer
	err error
}

func (e *errWriter) Write(buf []byte) (int, error) {
	if e.err != nil {
		return 0, e.err
	}

	var n int
	n, e.err = e.Writer.Write(buf)
	return n, nil
}
