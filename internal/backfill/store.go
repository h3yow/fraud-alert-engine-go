// Package backfill implements resumable historical-transaction backfill:
// chunked external sorting (so batches far larger than memory can be
// processed in business-time ascending order) plus checkpointed progress,
// reusing the processor's EventID idempotence for exactly-once counts.
package backfill

import (
	"context"
	"time"

	"github.com/h3yow/fraud-alert-engine-go/internal/domain"
)

// Phase labels the two externally visible stages of a job.
const (
	// PhaseSort: reading the source and spilling sorted chunks.
	PhaseSort = "sort"
	// PhaseMerge: k-way merging chunks and ingesting into the processor.
	PhaseMerge = "merge"
	// PhaseDone: every historical record has been committed.
	PhaseDone = "done"
)

// Record is one raw historical transaction with an opaque source sequence
// number used as the final tiebreaker for fully deterministic ordering.
type Record struct {
	Seq         int64
	Transaction domain.Transaction
}

// Key returns the totally-ordered merge key (OccurredAt, EventID, Seq).
// Seq makes the key unique even when a batch repeats the same EventID at the
// same business time, so the merge cursor can address one exact record.
func (r Record) Key() Key {
	return Key{OccurredAt: r.Transaction.OccurredAt, EventID: r.Transaction.EventID, Seq: r.Seq}
}

// Key is a checkpointable cursor position in the merged stream.
type Key struct {
	OccurredAt time.Time
	EventID    string
	Seq        int64
}

// Before reports whether k strictly precedes o in merge order.
func (k Key) Before(o Key) bool {
	if !k.OccurredAt.Equal(o.OccurredAt) {
		return k.OccurredAt.Before(o.OccurredAt)
	}
	if k.EventID != o.EventID {
		return k.EventID < o.EventID
	}
	return k.Seq < o.Seq
}

// Equal reports whether two cursors address the same merged record.
func (k Key) Equal(o Key) bool {
	return k.Seq == o.Seq && k.EventID == o.EventID && k.OccurredAt.Equal(o.OccurredAt)
}

// IsZero reports whether k is the empty (start-of-stream) cursor.
func (k Key) IsZero() bool {
	return k.OccurredAt.IsZero() && k.EventID == "" && k.Seq == 0
}

// RecordIterator streams sorted records from one spilled chunk or from the
// merged stream.
type RecordIterator interface {
	// Next returns the next record. ok=false means the iterator is drained.
	Next() (rec Record, ok bool, err error)
	Close() error
}

// InputReader is the user-supplied, re-openable historical data source. It
// MUST return the same records in the same order on every Open (re-open is
// used when a crash happens before the sort phase finished).
type InputReader interface {
	Open(ctx context.Context) (RecordIterator, error)
}

// InputReaderFunc adapts a factory function to InputReader.
type InputReaderFunc func(ctx context.Context) (RecordIterator, error)

// Open implements InputReader.
func (f InputReaderFunc) Open(ctx context.Context) (RecordIterator, error) {
	return f(ctx)
}

// AlertSnapshot is the operational alert state persisted for resume so that
// operator Ack/Resolve performed before a crash is not lost.
type AlertSnapshot struct {
	Key       string
	State     domain.AlertState
	UpdatedAt time.Time
}

// Manifest is the checkpoint describing exactly where a job paused.
type Manifest struct {
	JobID string
	// Phase is sort/merge/done.
	Phase string
	// Chunks are the spill files already produced during the sort phase.
	Chunks []string
	// Cursor is the merge-order key of the last record committed to the
	// processor. Records with key <= Cursor are never ingested twice.
	Cursor Key
	// Applied counts unique historical EventIDs ingested for the first time.
	Applied int64
	// Duplicates counts records absorbed by EventID idempotence (in-batch
	// repeats or records already present in the live processor).
	Duplicates int64
	// HighWatermark is the maximum historical OccurredAt seen in the job.
	HighWatermark time.Time
	// Alerts snapshots operational alert states at the last commit point.
	Alerts []AlertSnapshot
}

// ProgressStore persists spill chunks and the checkpoint manifest; it is the
// extension point that keeps memory bounded and enables resume/restart.
//
// Implementations must be safe for concurrent use: the live processor may be
// serving real-time traffic in another goroutine while a checkpoint is
// written.
type ProgressStore interface {
	// WriteChunk persists recs (already in merge-key ascending order) under
	// an implementation-generated id and returns that id.
	WriteChunk(ctx context.Context, recs []Record) (id string, err error)
	// OpenChunk re-opens a previously written chunk for a forward read.
	OpenChunk(ctx context.Context, id string) (RecordIterator, error)
	// DeleteChunk removes a chunk (called once a job completes).
	DeleteChunk(ctx context.Context, id string) error
	// LoadManifest returns (nil, false, nil) when no checkpoint exists.
	LoadManifest(ctx context.Context, jobID string) (*Manifest, bool, error)
	// SaveManifest atomically persists the checkpoint.
	SaveManifest(ctx context.Context, m Manifest) error
	// DeleteManifest removes the checkpoint of a completed job.
	DeleteManifest(ctx context.Context, jobID string) error
	// PeakBuffered reports the largest number of source records held in
	// process memory at once; instrumentation for bounded-memory tests.
	PeakBuffered() int
}
