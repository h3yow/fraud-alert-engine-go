package backfill

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
)

// MemStore is an in-memory ProgressStore. It preserves the exact chunk
// boundary semantics of DiskStore (records are only ever retained as
// separately spilled, sorted slices), which lets unit tests assert bounded
// memory via PeakBuffered without touching the filesystem.
type MemStore struct {
	mu       sync.Mutex
	chunks   map[string][]Record
	manifest map[string]Manifest
	counter  int64
	peakBuf  atomic.Int64
}

// NewMemStore creates an empty MemStore.
func NewMemStore() *MemStore {
	return &MemStore{chunks: map[string][]Record{}, manifest: map[string]Manifest{}}
}

// WriteChunk implements ProgressStore.
func (s *MemStore) WriteChunk(ctx context.Context, recs []Record) (string, error) {
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	default:
	}
	for {
		cur := s.peakBuf.Load()
		if int64(len(recs)) <= cur || s.peakBuf.CompareAndSwap(cur, int64(len(recs))) {
			break
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	n := s.counter
	s.counter++
	id := fmt.Sprintf("mem-chunk-%d", n)
	cp := make([]Record, len(recs))
	copy(cp, recs)
	s.chunks[id] = cp
	return id, nil
}

// OpenChunk implements ProgressStore.
func (s *MemStore) OpenChunk(ctx context.Context, id string) (RecordIterator, error) {
	s.mu.Lock()
	recs, ok := s.chunks[id]
	s.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("backfill: mem chunk not found: %s", id)
	}
	return &sliceIter{ctx: ctx, recs: recs}, nil
}

// DeleteChunk implements ProgressStore.
func (s *MemStore) DeleteChunk(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.chunks, id)
	return nil
}

// LoadManifest implements ProgressStore.
func (s *MemStore) LoadManifest(ctx context.Context, jobID string) (*Manifest, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.manifest[jobID]
	if !ok {
		return nil, false, nil
	}
	return &m, true, nil
}

// SaveManifest implements ProgressStore.
func (s *MemStore) SaveManifest(ctx context.Context, m Manifest) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := m
	cp.Chunks = append([]string(nil), m.Chunks...)
	cp.Alerts = append([]AlertSnapshot(nil), m.Alerts...)
	s.manifest[m.JobID] = cp
	return nil
}

// DeleteManifest implements ProgressStore.
func (s *MemStore) DeleteManifest(ctx context.Context, jobID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.manifest, jobID)
	return nil
}

// PeakBuffered implements ProgressStore.
func (s *MemStore) PeakBuffered() int { return int(s.peakBuf.Load()) }

// ChunkCount reports how many spill chunks currently exist (test helper).
func (s *MemStore) ChunkCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.chunks)
}
