package hasty

import (
	"fmt"
	"io"
	"os"
)

// wal represents a write-ahead log.
type wal struct {
	// path is a path to the WAL filename.
	path string
	f    *os.File

	encode func(out io.Writer, rec *record) error
}

// openReadonlyWAL opens a WAL file for reading.
func openReadonlyWAL(path string) (*wal, error) {
	w := wal{
		path:   path,
		encode: encode,
	}

	var err error
	if w.f, err = os.Open(path); err != nil {
		return nil, err
	}
	return &w, nil
}

// openWritableWAL opens a WAL file for appending records.
func openAppendonlyWAL(path string) (*wal, error) {
	w := wal{
		path:   path,
		encode: encode,
	}

	var err error
	if w.f, err = os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600); err != nil {
		return nil, err
	}
	return &w, nil
}

// Write appends a key-value pair to a log file.
// Note, it is not concurrency safe. By design there is only one writer.
func (w *wal) WriteRecord(rec *record) error {
	if err := w.encode(w.f, rec); err != nil {
		return fmt.Errorf("failed to encode record: %w", err)
	}
	if err := w.f.Sync(); err != nil {
		return fmt.Errorf("failed to sync file: %w", err)
	}
	return nil
}

// Truncate truncates the WAL file to discard WAL records after db recovery.
func (w *wal) Truncate() error {
	var err error
	if err = w.f.Truncate(0); err != nil {
		return err
	}
	_, err = w.f.Seek(0, 0)
	return err
}

// Close closes the WAL file.
func (w *wal) Close() error {
	return w.f.Close()
}
