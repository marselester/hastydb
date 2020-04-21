package hastydb

import (
	"bufio"
	"fmt"
	"io"
)

type segmentMerger struct {
	decode func(b []byte) *record
	encode func(out io.Writer, rec *record) error
}

// record represents a key-value pair read from a segment file.
type record struct {
	// key represents priority to arrange records in priority queue.
	// When there are two records with the same key (equal priorities), then their order field is compared.
	key string
	// order is a stream number. It is used to return records in the order they were originally added.
	order int
	value string
}

// Merge merges and compacts multiple sorted streams into one sorted stream using min priority queue.
func (sm *segmentMerger) Merge(out io.Writer, streams ...*bufio.Scanner) error {
	pq := newIndexMinHeap(len(streams))

	// Fill the priority queue with the first records from each stream.
	var rec *record
	var i int
	for i = range streams {
		if !streams[i].Scan() {
			continue
		}

		rec = sm.decode(streams[i].Bytes())
		rec.order = i
		pq.Insert(i, rec)
	}

	var prev *record
	for pq.Size() != 0 {
		// Take the smallest record from the priority queue (the min of all streams).
		i, rec = pq.Min()

		// Keep only last version of a key (segment compaction).
		if prev == nil {
			prev = rec
		}
		if prev.key != rec.key {
			sm.encode(out, prev)
			prev = rec
		}
		prev.value = rec.value

		// Refill the priority queue from the stream where min record was found, unless this stream is exhausted.
		if !streams[i].Scan() {
			continue
		}
		rec = sm.decode(streams[i].Bytes())
		rec.order = i
		pq.Insert(i, rec)
	}
	sm.encode(out, prev)

	for i = range streams {
		if err := streams[i].Err(); err != nil {
			return fmt.Errorf("stream %d failed: %w", i, err)
		}
	}
	return nil
}

// indexMinHeap is a binary heap that allows clients to refer to items on priority queue.
// The number of compares required is proportional to at most log n for "insert" and "remove the minimum" operations.
type indexMinHeap struct {
	// n is number of elements on priority queue.
	n int
	// pq is a binary heap using 1-based indexing.
	pq []int
	// qp is inverse of pq: qp[pq[i]] = pq[qp[i]] = i.
	qp []int
	// items holds items with priorities: items[i] = priority of i.
	items []*record
}

// newIndexMinHeap creates a binary heap of size n to prioritize min items.
func newIndexMinHeap(n int) *indexMinHeap {
	h := indexMinHeap{
		pq:    make([]int, n+1),
		qp:    make([]int, n+1),
		items: make([]*record, n+1),
	}
	for i := 0; i <= n; i++ {
		h.qp[i] = -1
	}
	return &h
}

// Insert adds the new item and associates it with index i.
// Think of it as pq[i] = item.
func (h *indexMinHeap) Insert(i int, item *record) {
	h.n++
	h.qp[i] = h.n
	h.pq[h.n] = i
	h.items[i] = item
	h.swim(h.n)
}

// Min takes the smallest item off the top.
// Note, the first returned value is the index associated with the item.
func (h *indexMinHeap) Min() (int, *record) {
	if h.Size() == 0 {
		return -1, nil
	}

	indexOfMin := h.pq[1]
	min := h.items[indexOfMin]

	h.exchange(1, h.n)
	h.n--
	h.sink(1)

	h.items[indexOfMin] = nil // blank item
	h.qp[indexOfMin] = -1
	h.pq[h.n+1] = -1

	return indexOfMin, min
}

// Size returns size of the heap.
func (h *indexMinHeap) Size() int {
	return h.n
}

func (h *indexMinHeap) greater(i, j int) bool {
	if h.items[h.pq[i]].key > h.items[h.pq[j]].key {
		return true
	}
	if h.items[h.pq[i]].key == h.items[h.pq[j]].key {
		return h.items[h.pq[i]].order > h.items[h.pq[j]].order
	}
	return false
}

func (h *indexMinHeap) exchange(i, j int) {
	swap := h.pq[i]
	h.pq[i] = h.pq[j]
	h.pq[j] = swap
	h.qp[h.pq[i]] = i
	h.qp[h.pq[j]] = j
}

func (h *indexMinHeap) swim(k int) {
	for k > 1 && h.greater(k/2, k) {
		h.exchange(k, k/2)
		k = k / 2
	}
}

func (h *indexMinHeap) sink(k int) {
	for 2*k <= h.n {
		j := 2 * k
		if j < h.n && h.greater(j, j+1) {
			j++
		}
		if !h.greater(k, j) {
			break
		}
		h.exchange(k, j)
		k = j
	}
}
