package hastydb

import (
	"bufio"
	"bytes"
	"io"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestMerge(t *testing.T) {
	segments := []string{
		"handbag:8786 handful:40308 handicap:65995 handkerchief:16324 handlebars:3869 handprinted:11150",
		"handcuffs:2729 handful:42307 handicap:67884 handiwork:16912 handkerchief:20952 handprinted:15725",
		"handful:44662 handicap:70836 handiwork:45521 handlebars:3869 handoff:5741 handprinted:33632",
	}
	want := `
handbag:8786
handcuffs:2729
handful:40308
handful:42307
handful:44662
handicap:65995
handicap:67884
handicap:70836
handiwork:16912
handiwork:45521
handkerchief:16324
handkerchief:20952
handlebars:3869
handlebars:3869
handoff:5741
handprinted:11150
handprinted:15725
handprinted:33632`

	streams := make([]*bufio.Scanner, len(segments))
	for i, s := range segments {
		streams[i] = bufio.NewScanner(strings.NewReader(s))
		streams[i].Split(bufio.ScanWords)
	}
	sm := segmentMerger{
		decode: decode,
		encode: encode,
	}

	var out bytes.Buffer
	err := sm.Merge(&out, streams...)
	if err != nil {
		t.Fatal(err)
	}

	if diff := cmp.Diff(want, out.String()); diff != "" {
		t.Errorf(diff)
	}
}

func decode(b []byte) *record {
	kv := strings.Split(string(b), ":")
	return &record{
		key:   kv[0],
		value: kv[1],
	}
}

func encode(out io.Writer, rec *record) (err error) {
	ew := &errWriter{Writer: out}
	ew.Write([]byte("\n"))
	ew.Write([]byte(rec.key))
	ew.Write([]byte(":"))
	ew.Write([]byte(rec.value))
	return ew.err
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
