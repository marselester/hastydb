package hasty

import (
	"context"
	"fmt"
	"io"
	"path/filepath"

	"golang.org/x/sync/semaphore"

	"github.com/marselester/hastydb/internal/index"
)

// newSSTableWriter creates a sstableWriter that can save only one memtable at a time.
func newSSTableWriter(db *DB) *sstableWriter {
	return &sstableWriter{
		db:     db,
		notif:  make(chan struct{}),
		sem:    semaphore.NewWeighted(1),
		encode: encode,
	}
}

// sstableWriter is an actor that is responsible for saving memtable on disk in SSTable format.
type sstableWriter struct {
	db    *DB
	notif chan struct{}
	sem   *semaphore.Weighted

	encode func(out io.Writer, rec *record) error
}

// Run starts the actor which is stopped by cancelling context.
// Note, actor will finish its job before exiting or else database might lose recent changes.
func (w *sstableWriter) Run(ctx context.Context) error {
	for {
		select {
		case <-w.notif:
			if !w.sem.TryAcquire(1) {
				break
			}
			// Flush failure indicates that database can't persist recent changes;
			// it must be restarted and recovered from the WAL.
			if err := w.flush(); err != nil {
				return err
			}
			w.sem.Release(1)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Notify informs the actor to persist the memtable on disk.
// Note, if the memtable is being written on disk, new notifications are ignored.
func (w *sstableWriter) Notify() {
	w.notif <- struct{}{}
}

// flush creates a new memtable and persists the previous memtable on disk.
func (w *sstableWriter) flush() error {
	// New writes go into the new memtable and it also serves reads.
	// Meanwhile the old memtable is being saved on disk,
	// it remains available for reads until it's fully written on disk.
	w.db.memMu.Lock()
	w.db.flushingMemtable = w.db.memtable
	w.db.memtable = &index.Memtable{}
	w.db.memMu.Unlock()

	segPath := filepath.Join(w.db.path, "seg0")
	seg, err := openWriteonlySegment(segPath)
	if err != nil {
		return fmt.Errorf("failed to open %q segment: %w", segPath, err)
	}
	if err = w.write(seg.f, w.db.flushingMemtable); err != nil {
		return fmt.Errorf("failed to write %q segment: %w", segPath, err)
	}
	if err = seg.Close(); err != nil {
		return fmt.Errorf("failed to close %q segment: %w", segPath, err)
	}

	// Add new segment file at the beginning of the database's segments list.
	w.db.segMu.Lock()
	current := w.db.segments.Load().([]*segment)
	ss := make([]*segment, len(current)+1)
	copy(ss[1:], current)
	ss[0] = seg
	w.db.segments.Store(ss)
	w.db.segMu.Unlock()

	if err = w.db.wal.Truncate(); err != nil {
		return fmt.Errorf("failed to truncate WAL: %w", err)
	}

	w.db.memMu.Lock()
	w.db.flushingMemtable = nil
	w.db.memMu.Unlock()

	return nil
}

// write writes memtable on disk in SSTable format.
// SSTable is efficiently created from BST because it maintains keys in sorted order.
func (w *sstableWriter) write(out io.Writer, bst *index.Memtable) (err error) {
	for _, key := range bst.Keys() {
		rec := record{
			key:   key,
			value: bst.Get(key),
		}
		if err = w.encode(out, &rec); err != nil {
			return fmt.Errorf("failed to encode record: %w", err)
		}
	}
	return nil
}
