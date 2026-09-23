// Package logging provides the tiny logging adapters shared by the demo
// binary and the tests: a standard-library backed logger and a
// concurrency-safe in-memory logger whose output tests can assert on.
package logging

import (
	"bytes"
	"io"
	"log"
	"sync"
)

// NewStdLogger returns a logger writing to w (or stdout when nil) with
// timestamps and the component prefix.
func NewStdLogger(w io.Writer) *log.Logger {
	if w == nil {
		w = log.Writer()
	}
	return log.New(w, "fraud ", log.LstdFlags|log.Lmicroseconds)
}

// BufferLogger is a thread-safe logger that retains every formatted line.
type BufferLogger struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

// Printf implements the logger interfaces used across the component.
func (b *BufferLogger) Printf(format string, args ...any) {
	b.mu.Lock()
	defer b.mu.Unlock()
	log.New(&b.buf, "", 0).Printf(format, args...)
}

// String returns everything logged so far.
func (b *BufferLogger) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// Bytes returns everything logged so far as a byte slice.
func (b *BufferLogger) Bytes() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Bytes()
}

// Reset discards buffered output.
func (b *BufferLogger) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf.Reset()
}
