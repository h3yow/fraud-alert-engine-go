package backfill

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
)

// jsonlRecord mirrors wireRecord but accepts the friendlier standalone input
// schema (seq optional, assigned from line number when absent).
type jsonlRecord struct {
	Seq *int64          `json:"seq,omitempty"`
	Tx  wireTransaction `json:"tx"`
}

// NewJSONLFileReader returns an InputReader that re-opens and streams the
// JSON-Lines file at path on every Open. One line is one object:
//
//	{"seq": 1, "tx": {"event_id": "e1", "tx_id": "t1", ...}}
//
// When seq is omitted it is derived from the 1-based line number, which is
// exactly the stable tiebreaker needed for deterministic merge ordering.
// Re-opening MUST yield the same bytes (a plain file does).
func NewJSONLFileReader(path string) InputReader {
	return InputReaderFunc(func(ctx context.Context) (RecordIterator, error) {
		f, err := os.Open(path)
		if err != nil {
			return nil, fmt.Errorf("backfill: open jsonl input: %w", err)
		}
		return &jsonlIter{ctx: ctx, f: f, dec: json.NewDecoder(bufio.NewReader(f))}, nil
	})
}

// NewJSONLReader returns an InputReader over an arbitrary io.ReaderAt-like
// provider; openFn is invoked on every (re)open so callers can rebuild a
// fresh reader after a mid-sort crash.
func NewJSONLReader(openFn func() (io.ReadCloser, error)) InputReader {
	return InputReaderFunc(func(ctx context.Context) (RecordIterator, error) {
		rc, err := openFn()
		if err != nil {
			return nil, err
		}
		return &jsonlIter{ctx: ctx, f: rc, dec: json.NewDecoder(bufio.NewReader(rc))}, nil
	})
}

// SliceReader adapts an in-memory record slice to an InputReader. It is the
// natural choice for tests and for callers that page data out of their own
// source on demand.
func SliceReader(recs []Record) InputReader {
	return InputReaderFunc(func(ctx context.Context) (RecordIterator, error) {
		return &sliceIter{ctx: ctx, recs: recs}, nil
	})
}

type jsonlIter struct {
	ctx  context.Context
	f    io.Closer
	dec  *json.Decoder
	line int64
}

// Next implements RecordIterator.
func (it *jsonlIter) Next() (Record, bool, error) {
	if err := it.ctx.Err(); err != nil {
		return Record{}, false, err
	}
	var in jsonlRecord
	if err := it.dec.Decode(&in); err != nil {
		if err == io.EOF {
			return Record{}, false, nil
		}
		return Record{}, false, fmt.Errorf("backfill: decode jsonl line %d: %w", it.line+1, err)
	}
	it.line++
	rec := decodeRecord(wireRecord{Tx: in.Tx})
	if in.Seq != nil {
		rec.Seq = *in.Seq
	} else {
		rec.Seq = it.line
	}
	return rec, true, nil
}

// Close implements RecordIterator.
func (it *jsonlIter) Close() error {
	if it.f != nil {
		return it.f.Close()
	}
	return nil
}

type sliceIter struct {
	ctx   context.Context
	recs  []Record
	index int
}

// Next implements RecordIterator.
func (it *sliceIter) Next() (Record, bool, error) {
	if err := it.ctx.Err(); err != nil {
		return Record{}, false, err
	}
	if it.index >= len(it.recs) {
		return Record{}, false, nil
	}
	rec := it.recs[it.index]
	it.index++
	return rec, true, nil
}

// Close implements RecordIterator.
func (it *sliceIter) Close() error { return nil }
