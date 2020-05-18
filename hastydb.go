// Package hasty is a key-value LSM storage engine, see the presentation
// https://go-talks.appspot.com/github.com/marselester/storage-engines/log-structured-engine.slide.
package hasty

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"golang.org/x/sync/errgroup"

	"github.com/marselester/hastydb/internal/index"
)

// DB represents HastyDB database on disk.
type DB struct {
	// path is a dir where segment files are stored.
	path string
	cfg  Config

	memMu            sync.RWMutex
	memtable         *index.Memtable
	flushingMemtable *index.Memtable

	// wal is a write-ahead log file where records are appended to recover from a database crash.
	wal *wal

	segMu sync.Mutex
	// segments is a slice of segment files where records are stored.
	// Newest segments are in the beginning of the slice.
	segments atomic.Value

	sstWriter *sstableWriter
	segMerger *segmentMerger
}

// Open opens a database directory named path where it expects to find segment files.
// If a database doesn't exist, it will be created.
// Make sure to close database to save recent changes on disk.
func Open(path string, options ...ConfigOption) (db *DB, close func() error, err error) {
	db = &DB{
		path: path,
		cfg: Config{
			maxMemtableSize: DefaultMaxMemtableSize,
		},
		memtable: &index.Memtable{},
	}
	for _, opt := range options {
		opt(&db.cfg)
	}

	if err = os.MkdirAll(db.path, 0700); err != nil {
		return nil, nil, fmt.Errorf("failed to create database dir: %w", err)
	}

	// If WAL is not empty, then the memtable probably was not saved last time,
	// because the WAL file is truncated every time memtable is successfully written on disk.
	walPath := filepath.Join(db.path, "wal")
	if db.wal, err = openReadonlyWAL(walPath); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return nil, nil, fmt.Errorf("failed to open WAL file to recover database: %w", err)
		}
	} else {
		// Recover from WAL file and then truncate it...
		if err = db.wal.Close(); err != nil {
			return nil, nil, fmt.Errorf("failed to close WAL file after database recovery: %w", err)
		}
	}
	if db.wal, err = openAppendonlyWAL(walPath); err != nil {
		return nil, nil, fmt.Errorf("failed to open new WAL file: %w", err)
	}

	// Launch system workers that write memtable on disk, merge old segments.
	ctx, quit := context.WithCancel(context.Background())
	g, ctx := errgroup.WithContext(ctx)
	db.sstWriter = newSSTableWriter(db)
	db.segMerger = newSegmentMerger(db)
	g.Go(func() error {
		return db.sstWriter.Run(ctx)
	})
	g.Go(func() error {
		return db.segMerger.Run(ctx)
	})

	// Close database and releases associated resources.
	close = func() error {
		// Flush memtable on disk before exiting.
		db.sstWriter.Notify()
		quit()
		if err := g.Wait(); err != context.Canceled {
			return err
		}
		return nil
	}

	return db, close, nil
}

// Set puts a key in database. Note, operation is concurrency safe.
func (db *DB) Set(key string, value []byte) error {
	db.memMu.Lock()
	db.memtable.Set(key, value)
	db.memMu.Unlock()

	err := db.wal.WriteRecord(&record{
		key:   key,
		value: value,
	})
	if err != nil {
		return fmt.Errorf("failed to write record to WAL file: %w", err)
	}

	// Trigger memtable rotation (save the current one on disk, create new memtable).
	if db.memtable.Size() > db.cfg.maxMemtableSize {
		db.sstWriter.Notify()
	}

	return nil
}

// Get retrieves a key from database. Note, operation is concurrency safe.
func (db *DB) Get(key string) (value []byte, err error) {
	db.memMu.RLock()
	value = db.memtable.Get(key)
	if value == nil && db.flushingMemtable != nil {
		value = db.flushingMemtable.Get(key)
	}
	db.memMu.RUnlock()

	if value != nil {
		return value, nil
	}

	ss := db.segments.Load().([]*segment)
	var (
		found  bool
		offset int64
		rec    *record
	)
	for i := range ss {
		if offset, found = ss[i].index[key]; found {
			if rec, err = ss[i].ReadRecord(offset); err != nil {
				return nil, fmt.Errorf("failed to read record: %w", err)
			}
			return rec.value, nil
		}
	}

	return nil, ErrKeyNotFound
}
