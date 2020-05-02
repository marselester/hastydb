package hasty

import (
	"io"
	"strings"
)

func plainDecode(b []byte) *record {
	kv := strings.Split(string(b), ":")
	return &record{
		key:   kv[0],
		value: []byte(kv[1]),
	}
}

func plainEncode(out io.Writer, rec *record) (err error) {
	ew := &errWriter{Writer: out}
	ew.Write([]byte("\n"))
	ew.Write([]byte(rec.key))
	ew.Write([]byte(":"))
	ew.Write([]byte(rec.value))
	return ew.err
}
