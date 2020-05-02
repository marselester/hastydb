// Package hasty is a key-value LSM storage engine.
package hasty

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"golang.org/x/sync/errgroup"

	"github.com/marselester/hastydb/internal/index"
)

// DB represents HastyDB database on disk.
type DB struct {
	// path is a dir where segment files are stored.
	path string
	cfg  Config

	mu       sync.RWMutex
	memtable *index.Memtable
	wal      *wal

	sstWriter *sstableWriter
	segMerger *segmentMerger
	// quitc signals the database workers to stop.
	quitc    chan struct{}
	quitOnce sync.Once
	// g is a collection of workers that are stopped when quitc is closed.
	g *errgroup.Group
}

// Open opens a database directory named path where it expects to find segment files.
// If a database doesn't exist, it will be created.
func Open(path string, options ...ConfigOption) (*DB, error) {
	db := DB{
		path: path,
		cfg: Config{
			maxMemtableSize: DefaultMaxMemtableSize,
		},
		memtable: &index.Memtable{},
		quitc:    make(chan struct{}),
	}
	for _, opt := range options {
		opt(&db.cfg)
	}

	var err error
	if err = os.MkdirAll(db.path, 0700); err != nil {
		return nil, fmt.Errorf("failed to create database dir: %w", err)
	}

	// If WAL is not empty, then the memtable probably was not saved last time,
	// because the WAL file is truncated every time memtable is successfully written on disk.
	walPath := filepath.Join(db.path, "wal")
	if db.wal, err = openReadonlyWAL(walPath); err != nil {
		return nil, err
	}

	// Launch system workers that write memtable on disk, merge old segments.
	g, ctx := errgroup.WithContext(context.Background())
	db.sstWriter = newSSTableWriter(&db)
	db.segMerger = newSegmentMerger(&db)
	g.Go(func() error {
		<-db.quitc
		return fmt.Errorf("hastydb was signalled to quit")
	})
	g.Go(func() error {
		return db.sstWriter.Run(ctx)
	})
	g.Go(func() error {
		return db.segMerger.Run(ctx)
	})
	db.g = g

	return &db, nil
}

// Close closes database and releases associated resources.
func (db *DB) Close() error {
	// Flush memtable on disk before exiting.
	db.sstWriter.Notify()

	db.quitOnce.Do(func() {
		close(db.quitc)
	})
	return db.g.Wait()
}

// Set puts a key in database. Note, operation is concurrency safe.
func (db *DB) Set(key string, value []byte) error {
	db.mu.Lock()
	db.memtable.Set(key, value)
	db.mu.Unlock()

	db.wal.WriteRecord(&record{key: key, value: value})

	// Trigger memtable rotation (save the current one on disk, create new memtable).
	if db.memtable.Size() > db.cfg.maxMemtableSize {
		db.sstWriter.Notify()
	}

	return nil
}

// Get retrieves a key from database. Note, operation is concurrency safe.
func (db *DB) Get(key string) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.memtable.Get(key), nil
}
