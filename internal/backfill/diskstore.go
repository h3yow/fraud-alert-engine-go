package backfill

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/h3yow/fraud-alert-engine-go/internal/domain"
)

// DefaultDirStorePermissions are the file modes used by DiskStore.
const (
	dirPerm  = 0o755
	filePerm = 0o600
)

// DiskStore is a filesystem-backed ProgressStore: sorted chunks live as JSON
// Lines files, the manifest as one JSON file written atomically (tmp file +
// rename). It survives process restarts, which is what makes "stop, restart
// and continue" possible.
type DiskStore struct {
	root string

	mu      sync.Mutex
	peakBuf atomic.Int64
}

// NewDiskStore creates/opens a DiskStore rooted at dir.
func NewDiskStore(dir string) (*DiskStore, error) {
	if dir == "" {
		return nil, errors.New("backfill: empty DiskStore dir")
	}
	if err := os.MkdirAll(filepath.Join(dir, "chunks"), dirPerm); err != nil {
		return nil, fmt.Errorf("backfill: mkdir chunks: %w", err)
	}
	if err := os.MkdirAll(filepath.Join(dir, "manifests"), dirPerm); err != nil {
		return nil, fmt.Errorf("backfill: mkdir manifests: %w", err)
	}
	return &DiskStore{root: dir}, nil
}

// wireRecord is the stable JSON encoding of one spilled record.
type wireRecord struct {
	Seq int64           `json:"seq"`
	Tx  wireTransaction `json:"tx"`
}

type wireTransaction struct {
	EventID    string    `json:"event_id"`
	TxID       string    `json:"tx_id"`
	AccountID  string    `json:"account_id"`
	ClusterID  string    `json:"cluster_id"`
	Amount     float64   `json:"amount"`
	Currency   string    `json:"currency"`
	Country    string    `json:"country"`
	EventType  string    `json:"event_type"`
	OccurredAt time.Time `json:"occurred_at"`
	IngestedAt time.Time `json:"ingested_at"`
}

func encodeRecord(r Record) wireRecord {
	t := r.Transaction
	return wireRecord{
		Seq: r.Seq,
		Tx: wireTransaction{
			EventID: t.EventID, TxID: t.TxID, AccountID: t.AccountID,
			ClusterID: t.ClusterID, Amount: t.Amount, Currency: t.Currency,
			Country: t.Country, EventType: t.EventType,
			OccurredAt: t.OccurredAt, IngestedAt: t.IngestedAt,
		},
	}
}

func decodeRecord(w wireRecord) Record {
	t := w.Tx
	return Record{
		Seq: w.Seq,
		Transaction: domain.Transaction{
			EventID: t.EventID, TxID: t.TxID, AccountID: t.AccountID,
			ClusterID: t.ClusterID, Amount: t.Amount, Currency: t.Currency,
			Country: t.Country, EventType: t.EventType,
			OccurredAt: t.OccurredAt, IngestedAt: t.IngestedAt,
		},
	}
}

// WriteChunk implements ProgressStore.
func (s *DiskStore) WriteChunk(ctx context.Context, recs []Record) (string, error) {
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	default:
	}
	s.noteBuffered(len(recs))
	id := fmt.Sprintf("chunk-%d.jsonl", time.Now().UnixNano())
	path := s.chunkPath(id)
	tmp := path + ".tmp"

	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, filePerm)
	if err != nil {
		return "", fmt.Errorf("backfill: create chunk: %w", err)
	}
	bw := bufio.NewWriter(f)
	enc := json.NewEncoder(bw)
	for i := range recs {
		if err := enc.Encode(encodeRecord(recs[i])); err != nil {
			f.Close()
			os.Remove(tmp)
			return "", fmt.Errorf("backfill: encode chunk: %w", err)
		}
	}
	if err := bw.Flush(); err != nil {
		f.Close()
		os.Remove(tmp)
		return "", fmt.Errorf("backfill: flush chunk: %w", err)
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return "", fmt.Errorf("backfill: close chunk: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return "", fmt.Errorf("backfill: finalize chunk: %w", err)
	}
	return id, nil
}

// OpenChunk implements ProgressStore.
func (s *DiskStore) OpenChunk(ctx context.Context, id string) (RecordIterator, error) {
	f, err := os.Open(s.chunkPath(id))
	if err != nil {
		return nil, fmt.Errorf("backfill: open chunk %s: %w", id, err)
	}
	return &fileChunkIter{f: f, dec: json.NewDecoder(bufio.NewReader(f)), ctx: ctx}, nil
}

// DeleteChunk implements ProgressStore.
func (s *DiskStore) DeleteChunk(ctx context.Context, id string) error {
	if err := os.Remove(s.chunkPath(id)); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("backfill: delete chunk %s: %w", id, err)
	}
	return nil
}

// LoadManifest implements ProgressStore.
func (s *DiskStore) LoadManifest(ctx context.Context, jobID string) (*Manifest, bool, error) {
	data, err := os.ReadFile(s.manifestPath(jobID))
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("backfill: load manifest: %w", err)
	}
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, false, fmt.Errorf("backfill: decode manifest: %w", err)
	}
	return &m, true, nil
}

// SaveManifest implements ProgressStore (atomic tmp+rename).
func (s *DiskStore) SaveManifest(ctx context.Context, m Manifest) error {
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return fmt.Errorf("backfill: encode manifest: %w", err)
	}
	path := s.manifestPath(m.JobID)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, filePerm); err != nil {
		return fmt.Errorf("backfill: write manifest: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("backfill: finalize manifest: %w", err)
	}
	return nil
}

// DeleteManifest implements ProgressStore.
func (s *DiskStore) DeleteManifest(ctx context.Context, jobID string) error {
	if err := os.Remove(s.manifestPath(jobID)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("backfill: delete manifest: %w", err)
	}
	return nil
}

// PeakBuffered implements ProgressStore.
func (s *DiskStore) PeakBuffered() int { return int(s.peakBuf.Load()) }

func (s *DiskStore) noteBuffered(n int) {
	for {
		cur := s.peakBuf.Load()
		if int64(n) <= cur || s.peakBuf.CompareAndSwap(cur, int64(n)) {
			return
		}
	}
}

func (s *DiskStore) chunkPath(id string) string { return filepath.Join(s.root, "chunks", id) }
func (s *DiskStore) manifestPath(id string) string {
	return filepath.Join(s.root, "manifests", id+".json")
}

// fileChunkIter streams one JSONL chunk record by record.
type fileChunkIter struct {
	f   *os.File
	dec *json.Decoder
	ctx context.Context
}

// Next implements RecordIterator.
func (it *fileChunkIter) Next() (Record, bool, error) {
	if err := it.ctx.Err(); err != nil {
		return Record{}, false, err
	}
	var w wireRecord
	if err := it.dec.Decode(&w); err != nil {
		if errors.Is(err, io.EOF) {
			return Record{}, false, nil
		}
		return Record{}, false, fmt.Errorf("backfill: decode chunk record: %w", err)
	}
	return decodeRecord(w), true, nil
}

// Close implements RecordIterator.
func (it *fileChunkIter) Close() error { return it.f.Close() }
