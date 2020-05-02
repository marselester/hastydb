package hasty

import (
	"context"
	"fmt"
	"io"

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
	w.db.mu.Lock()
	oldMem := w.db.memtable
	// New writes don't have to wait and should go into the new memtable.
	// Meanwhile the old memtable is being saved on disk.
	w.db.memtable = &index.Memtable{}
	w.db.mu.Unlock()

	segName := "seg0"
	s, err := openWriteonlySegment(segName)
	if err != nil {
		return fmt.Errorf("failed to open %q segment: %w", segName, err)
	}
	if err = w.write(s.f, oldMem); err != nil {
		return fmt.Errorf("failed to write %q segment: %w", segName, err)
	}
	if err = s.Close(); err != nil {
		return fmt.Errorf("failed to close %q segment: %w", segName, err)
	}

	if err = w.db.wal.Truncate(); err != nil {
		return fmt.Errorf("failed to truncate WAL: %w", err)
	}

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
