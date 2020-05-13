package hasty

import (
	"bytes"
	"errors"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestOpenReadonlySegment_error(t *testing.T) {
	tests := map[string]struct {
		path string
		want error
	}{
		"no file":     {"testdata/404segment", os.ErrNotExist},
		"file exists": {"testdata/readsegment", nil},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := openReadonlySegment(tc.path)
			if !errors.Is(err, tc.want) {
				t.Errorf("expected: %v, got: %v", tc.want, err)
			}
		})
	}
}

func TestOpenWriteonlySegment_error(t *testing.T) {
	tests := map[string]struct {
		path string
		want error
	}{
		"no file":     {"testdata/404segment", nil},
		"file exists": {"testdata/readsegment", os.ErrExist},
	}

	t.Cleanup(func() {
		os.Remove("testdata/404segment")
	})

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := openWriteonlySegment(tc.path)
			if !errors.Is(err, tc.want) {
				t.Errorf("expected: %v, got: %v", tc.want, err)
			}
		})
	}
}

func TestEncode(t *testing.T) {
	tests := map[string]struct {
		key   string
		value []byte
		want  []byte
	}{
		"name=Bob": {
			// [110 97 109 101]
			key: "name",
			// [66 111 98]
			value: []byte("Bob"),
			// record len (4 bytes) + key + delimeter (1 byte) + value
			want: []byte{12, 0, 0, 0, 110, 97, 109, 101, 0, 66, 111, 98},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			var out bytes.Buffer
			rec := record{
				key:   tc.key,
				value: tc.value,
			}
			if err := encode(&out, &rec); err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(tc.want, out.Bytes()); diff != "" {
				t.Fatalf(diff)
			}
		})
	}
}

func TestDecode(t *testing.T) {
	tests := map[string]struct {
		b         []byte
		wantKey   string
		wantValue []byte
	}{
		"name=Bob": {
			b:         []byte{12, 0, 0, 0, 110, 97, 109, 101, 0, 66, 111, 98},
			wantKey:   "name",
			wantValue: []byte("Bob"),
		},
	}

	for _, tc := range tests {
		rec := decode(tc.b)
		if rec.key != tc.wantKey {
			t.Errorf("expected key: %q got: %q", tc.wantKey, rec.key)
		}
		if !bytes.Equal(rec.value, tc.wantValue) {
			t.Errorf("expected value: %q got: %q", tc.wantValue, rec.value)
		}
	}
}

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
