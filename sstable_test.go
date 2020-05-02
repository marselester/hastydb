package hasty

import (
	"bufio"
	"bytes"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/marselester/hastydb/internal/index"
)

func TestSSTableWriter(t *testing.T) {
	tests := map[string]struct {
		log  string
		want string
	}{
		"databass.dev": {
			"k2:v1 k4:v2 k1:v3 k2:v4 k3:v5",
			`
k1:v3
k2:v4
k3:v5
k4:v2`,
		},
		"algs4.cs.princeton.edu": {
			"A:1 B:1 C:1 F:1 G:1 I:1 I:2 Z:1 B:2 D:1 H:1 P:1 Q:1 Q:2 A:2 B:3 E:1 F:2 J:1 N:1",
			`
A:2
B:3
C:1
D:1
E:1
F:2
G:1
H:1
I:2
J:1
N:1
P:1
Q:2
Z:1`,
		},
		"dataintensive.net": {
			"handbag:8786 handful:40308 handicap:65995 handkerchief:16324 handlebars:3869 handprinted:11150 " +
				"handcuffs:2729 handful:42307 handicap:67884 handiwork:16912 handkerchief:20952 handprinted:15725 " +
				"handful:44662 handicap:70836 handiwork:45521 handlebars:3869 handoff:5741 handprinted:33632",
			`
handbag:8786
handcuffs:2729
handful:44662
handicap:70836
handiwork:45521
handkerchief:20952
handlebars:3869
handoff:5741
handprinted:33632`,
		},
	}

	sw := sstableWriter{
		encode: plainEncode,
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			mem := index.Memtable{}
			scanner := bufio.NewScanner(strings.NewReader(tc.log))
			scanner.Split(bufio.ScanWords)
			for scanner.Scan() {
				rec := plainDecode(scanner.Bytes())
				mem.Set(rec.key, rec.value)
			}

			var out bytes.Buffer
			err := sw.write(&out, &mem)
			if err != nil {
				t.Fatal(err)
			}

			if diff := cmp.Diff(tc.want, out.String()); diff != "" {
				t.Fatalf(diff)
			}
		})
	}
}

func TestSSTableWriter_segment_write(t *testing.T) {
	tests := map[string]struct {
		log  string
		want string
	}{
		"databass.dev": {
			"k2:v1 k4:v2 k1:v3 k2:v4 k3:v5",
			`
k1:v3
k2:v4
k3:v5
k4:v2`,
		},
		"algs4.cs.princeton.edu": {
			"A:1 B:1 C:1 F:1 G:1 I:1 I:2 Z:1 B:2 D:1 H:1 P:1 Q:1 Q:2 A:2 B:3 E:1 F:2 J:1 N:1",
			`
A:2
B:3
C:1
D:1
E:1
F:2
G:1
H:1
I:2
J:1
N:1
P:1
Q:2
Z:1`,
		},
		"dataintensive.net": {
			"handbag:8786 handful:40308 handicap:65995 handkerchief:16324 handlebars:3869 handprinted:11150 " +
				"handcuffs:2729 handful:42307 handicap:67884 handiwork:16912 handkerchief:20952 handprinted:15725 " +
				"handful:44662 handicap:70836 handiwork:45521 handlebars:3869 handoff:5741 handprinted:33632",
			`
handbag:8786
handcuffs:2729
handful:44662
handicap:70836
handiwork:45521
handkerchief:20952
handlebars:3869
handoff:5741
handprinted:33632`,
		},
	}

	sw := sstableWriter{
		encode: plainEncode,
	}
	segName := "testdata/writesegment"
	t.Cleanup(func() {
		if err := os.Remove(segName); err != nil {
			t.Errorf("failed to remove %q segment: %w", segName, err)
		}
	})

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			seg, err := openWriteonlySegment(segName)
			if err != nil {
				t.Fatal(err)
			}
			seg.encode = plainEncode

			mem := index.Memtable{}
			scanner := bufio.NewScanner(strings.NewReader(tc.log))
			scanner.Split(bufio.ScanWords)
			for scanner.Scan() {
				rec := plainDecode(scanner.Bytes())
				mem.Set(rec.key, rec.value)
			}

			if err = sw.write(seg, &mem); err != nil {
				t.Fatal(err)
			}
			if err = seg.Flush(); err != nil {
				t.Fatal(err)
			}
			if err = seg.Close(); err != nil {
				t.Fatal(err)
			}

			got, err := ioutil.ReadFile(segName)
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(tc.want, string(got)); diff != "" {
				t.Fatalf(diff)
			}
		})
	}
}
