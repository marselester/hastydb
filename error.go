package hasty

import "io"

// ErrKeyNotFound is returned when a requested key is not found in database.
const ErrKeyNotFound = Error("key not found")

// Error defines HastyDB errors.
type Error string

func (e Error) Error() string {
	return string(e)
}

// errWriter fulfils the io.Writer contract so it can be used to wrap an existing io.Writer.
// errWriter passes writes through to its underlying writer until an error is detected.
// From that point on, it discards any writes and returns the previous error.
// See https://dave.cheney.net/2019/01/27/eliminate-error-handling-by-eliminating-errors.
type errWriter struct {
	io.Writer
	err error
}

func (e *errWriter) Write(buf []byte) (int, error) {
	if e.err != nil {
		return 0, e.err
	}

	var n int
	n, e.err = e.Writer.Write(buf)
	return n, nil
}
