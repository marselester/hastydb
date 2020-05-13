package hasty

import (
	"bufio"
	"bytes"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestSegmentMerger(t *testing.T) {
	tests := map[string]struct {
		segments []string
		want     string
	}{
		"databass.dev": {
			[]string{
				"k2:v1 k4:v2",
				"k1:v3 k2:v4 k3:v5",
			},
			`
k1:v3
k2:v4
k3:v5
k4:v2`,
		},
		"algs4.cs.princeton.edu": {
			[]string{
				"A:1 B:1 C:1 F:1 G:1 I:1 I:2 Z:1",
				"B:2 D:1 H:1 P:1 Q:1 Q:2",
				"A:2 B:3 E:1 F:2 J:1 N:1",
			},
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
			[]string{
				"handbag:8786 handful:40308 handicap:65995 handkerchief:16324 handlebars:3869 handprinted:11150",
				"handcuffs:2729 handful:42307 handicap:67884 handiwork:16912 handkerchief:20952 handprinted:15725",
				"handful:44662 handicap:70836 handiwork:45521 handlebars:3869 handoff:5741 handprinted:33632",
			},
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

	sm := segmentMerger{
		decode: plainDecode,
		encode: plainEncode,
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			streams := make([]*bufio.Scanner, len(tc.segments))
			for i, s := range tc.segments {
				streams[i] = bufio.NewScanner(strings.NewReader(s))
				streams[i].Split(bufio.ScanWords)
			}

			var out bytes.Buffer
			err := sm.mergeStreams(&out, streams...)
			if err != nil {
				t.Fatal(err)
			}

			if diff := cmp.Diff(tc.want, out.String()); diff != "" {
				t.Fatalf(diff)
			}
		})
	}
}

func TestSegmentMerger_mergeStreams(t *testing.T) {
	tests := map[string]struct {
		segments []string
		want     string
	}{
		"databass.dev": {
			[]string{
				"k2:v1 k4:v2",
				"k1:v3 k2:v4 k3:v5",
			},
			`
k1:v3
k2:v4
k3:v5
k4:v2`,
		},
		"algs4.cs.princeton.edu": {
			[]string{
				"A:1 B:1 C:1 F:1 G:1 I:1 I:2 Z:1",
				"B:2 D:1 H:1 P:1 Q:1 Q:2",
				"A:2 B:3 E:1 F:2 J:1 N:1",
			},
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
			[]string{
				"handbag:8786 handful:40308 handicap:65995 handkerchief:16324 handlebars:3869 handprinted:11150",
				"handcuffs:2729 handful:42307 handicap:67884 handiwork:16912 handkerchief:20952 handprinted:15725",
				"handful:44662 handicap:70836 handiwork:45521 handlebars:3869 handoff:5741 handprinted:33632",
			},
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

	sm := segmentMerger{
		decode: plainDecode,
		encode: plainEncode,
	}
	segName := "testdata/mergedsegment"

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			seg, err := openWriteonlySegment(segName)
			if err != nil {
				t.Fatal(err)
			}
			seg.encode = plainEncode
			t.Cleanup(func() {
				if err := os.Remove(segName); err != nil {
					t.Errorf("failed to remove %q segment: %w", segName, err)
				}
			})

			streams := make([]*bufio.Scanner, len(tc.segments))
			for i, s := range tc.segments {
				streams[i] = bufio.NewScanner(strings.NewReader(s))
				streams[i].Split(bufio.ScanWords)
			}

			if err = sm.mergeStreams(seg, streams...); err != nil {
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
