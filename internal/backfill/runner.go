package backfill

import (
	"container/heap"
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/h3yow/fraud-alert-engine-go/internal/domain"
)

// Logger is the minimal logging surface used by the runner.
type Logger interface {
	Printf(format string, args ...any)
}

type nopLogger struct{}

func (nopLogger) Printf(string, ...any) {}

const (
	// DefaultChunkSize is the maximum number of source records buffered in
	// memory during the sort phase; spill chunks are written at this size.
	DefaultChunkSize = 10_000
	// DefaultMicroBatchSize is how many merged records are ingested between
	// two checkpoints during the merge phase.
	DefaultMicroBatchSize = 500
)

// Config controls a backfill run.
type Config struct {
	// JobID identifies the job; resuming uses the same JobID.
	JobID string
	// ChunkSize bounds sort-phase memory; <=0 means DefaultChunkSize.
	ChunkSize int
	// MicroBatchSize bounds merge-phase memory and sets checkpoint cadence;
	// <=0 means DefaultMicroBatchSize.
	MicroBatchSize int
	// Source is the re-openable historical input. It may be nil when
	// resuming a job whose sort phase already finished.
	Source InputReader
}

// Result summarizes one Run invocation.
type Result struct {
	JobID string
	// Applied is the number of unique historical EventIDs newly committed
	// during THIS invocation (excluding duplicates absorbed by idempotence).
	Applied int64
	// Duplicates is the number of records this invocation that were
	// replayed against an already-seen EventID (in-batch repeats, records
	// already present live, or records replayed after a restart).
	Duplicates int64
	// SkippedAfter is records skipped because they were <= the resume cursor.
	SkippedAfter int64
	// ChunksSpilled is the total number of spill chunks on disk after the run.
	ChunksSpilled int
	// PeakBuffered is the largest number of source records held in memory.
	PeakBuffered int
	// Completed is true when the whole job is done and cleaned up.
	Completed bool
	// Paused is true when the run returned because ctx was cancelled after a
	// checkpoint had been persisted (resumable).
	Paused bool
}

// Ingester is the processor-side contract used by the runner.
// *processor.Processor satisfies it.
type Ingester interface {
	IngestHistorical(tx domain.Transaction) domain.Verdict
	AdvanceWatermark(t time.Time)
	HistoricalHighWatermark() time.Time
	SnapshotAlertStates() []domain.AlertStateRecord
	RestoreAlertStates(recs []domain.AlertStateRecord)
}

// Runner executes/resumes chunked, checkpointed backfills.
type Runner struct {
	store  ProgressStore
	logger Logger

	mu sync.Mutex
}

// NewRunner builds a Runner backed by the given progress store.
func NewRunner(store ProgressStore, logger Logger) *Runner {
	if logger == nil {
		logger = nopLogger{}
	}
	return &Runner{store: store, logger: logger}
}

// Run starts a new job or resumes an existing one.
//
// ctx cancellation is cooperative: the current chunk/micro-batch finishes,
// the checkpoint is persisted, and the method returns with Paused=true and
// ctx.Err() (or nil when the job happened to complete). Calling Run again
// with the same JobID continues after the last committed micro-batch.
func (r *Runner) Run(ctx context.Context, ing Ingester, cfg Config) (Result, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	chunkSize := cfg.ChunkSize
	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize
	}
	microBatch := cfg.MicroBatchSize
	if microBatch <= 0 {
		microBatch = DefaultMicroBatchSize
	}
	if cfg.JobID == "" {
		return Result{}, errors.New("backfill: Config.JobID is required")
	}

	res := Result{JobID: cfg.JobID}

	m, exists, err := r.store.LoadManifest(ctx, cfg.JobID)
	if err != nil {
		return res, err
	}
	if !exists {
		m = &Manifest{JobID: cfg.JobID, Phase: PhaseSort}
	}

	switch m.Phase {
	case PhaseSort:
		if cfg.Source == nil {
			return res, errors.New("backfill: cannot (re)start sort phase without a Source")
		}
		paused, err := r.sortPhase(ctx, cfg.JobID, cfg.Source, chunkSize, m)
		res.ChunksSpilled = len(m.Chunks)
		res.PeakBuffered = r.store.PeakBuffered()
		if err != nil {
			return res, err
		}
		if paused {
			res.Paused = true
			r.logf("[backfill] job=%s sort phase paused: chunks=%d", cfg.JobID, len(m.Chunks))
			return res, ctx.Err()
		}
		m.Phase = PhaseMerge
		if err := r.store.SaveManifest(ctx, *m); err != nil {
			return res, err
		}
		res.ChunksSpilled = len(m.Chunks)
		// fall through to merge
	case PhaseMerge:
		res.ChunksSpilled = len(m.Chunks)
		// fall through to merge
	case PhaseDone:
		// Nothing to do; a finished job replayed after restart is rebuilt
		// explicitly through RestoreInto.
		res.Completed = true
		res.PeakBuffered = r.store.PeakBuffered()
		return res, nil
	default:
		return res, errors.New("backfill: manifest in unknown phase: " + m.Phase)
	}

	paused, applied, dups, skipped, err := r.mergePhase(ctx, ing, m, microBatch)
	res.Applied += applied
	res.Duplicates += dups
	res.SkippedAfter += skipped
	res.PeakBuffered = r.store.PeakBuffered()
	res.ChunksSpilled = len(m.Chunks)
	if err != nil {
		return res, err
	}
	if paused {
		res.Paused = true
		r.logf("[backfill] job=%s merge paused at cursor={%s,%s,%d} applied=%d dup=%d",
			cfg.JobID, m.Cursor.OccurredAt.Format("2006-01-02T15:04:05Z07:00"),
			m.Cursor.EventID, m.Cursor.Seq, m.Applied, m.Duplicates)
		return res, ctx.Err()
	}

	// Completed: connect the live watermark to historical business time
	// monotonically, snapshot is irrelevant anymore, clean spill files.
	m.Phase = PhaseDone
	hw := m.HighWatermark
	if ing != nil {
		if h := ing.HistoricalHighWatermark(); h.After(hw) {
			hw = h
		}
		ing.AdvanceWatermark(hw)
	}
	m.HighWatermark = hw
	if err := r.store.SaveManifest(ctx, *m); err != nil {
		return res, err
	}
	for _, id := range m.Chunks {
		if err := r.store.DeleteChunk(ctx, id); err != nil {
			return res, err
		}
	}
	m.Chunks = nil
	if err := r.store.SaveManifest(ctx, *m); err != nil {
		return res, err
	}
	if err := r.store.DeleteManifest(ctx, cfg.JobID); err != nil {
		return res, err
	}
	res.Completed = true
	res.ChunksSpilled = 0
	r.logf("[backfill] job=%s complete applied_total=%d dup_total=%d high_watermark=%s",
		cfg.JobID, m.Applied, m.Duplicates, hw.Format("2006-01-02T15:04:05Z07:00"))
	return res, nil
}

// sortPhase reads the source once, buffers up to chunkSize records, sorts the
// buffer in memory and spills it. Already-spilled chunk ids in m survive a
// mid-sort crash: the re-opened source is read from the start and the
// already-spilled prefix is skipped by record count.
func (r *Runner) sortPhase(ctx context.Context, jobID string, src InputReader, chunkSize int, m *Manifest) (paused bool, err error) {
	iter, err := src.Open(ctx)
	if err != nil {
		return false, err
	}
	defer iter.Close()

	// Skip records already persisted in earlier chunks after a mid-sort
	// crash. Every chunk flushed by the read loop below is exactly
	// chunkSize records long (the only partial chunk is the tail flush,
	// which only happens when the source is already drained), so the
	// already-spilled prefix length is derivable without rescanning files.
	spilledRecords := int64(len(m.Chunks)) * int64(chunkSize)
	buf := make([]Record, 0, chunkSize)
	seq := int64(0)

	flush := func() error {
		sort.Slice(buf, func(i, j int) bool { return buf[i].Key().Before(buf[j].Key()) })
		id, err := r.store.WriteChunk(ctx, buf)
		if err != nil {
			return err
		}
		m.Chunks = append(m.Chunks, id)
		if err := r.store.SaveManifest(ctx, *m); err != nil {
			return err
		}
		spilledRecords += int64(len(buf))
		r.logf("[backfill] job=%s spilled chunk=%s records=%d total_chunks=%d",
			jobID, id, len(buf), len(m.Chunks))
		buf = make([]Record, 0, chunkSize)
		return nil
	}

	for {
		rec, ok, err := iter.Next()
		if err != nil {
			return false, err
		}
		if !ok {
			break
		}
		seq++
		if seq <= spilledRecords {
			continue
		}
		buf = append(buf, rec)
		if len(buf) >= chunkSize {
			if err := flush(); err != nil {
				return false, err
			}
			// Cooperative pause: the spilled prefix is durable and recorded,
			// so a later resume re-opens the source, skips this prefix by
			// record count and continues sorting the remainder.
			if ctx.Err() != nil {
				return true, nil
			}
		}
	}
	if len(buf) > 0 {
		if err := flush(); err != nil {
			return false, err
		}
	}
	if len(m.Chunks) == 0 {
		// Empty input: still a well-formed (trivially complete) job.
		m.Chunks = nil
	}
	return false, nil
}

// mergePhase opens every spilled chunk, k-way merges them in key order, skips
// records at or before the resume cursor and commits micro-batches, each
// followed by a durable checkpoint.
func (r *Runner) mergePhase(ctx context.Context, ing Ingester, m *Manifest, microBatch int) (
	paused bool, applied, dups, skipped int64, err error) {

	iters := make([]*chunkCursor, 0, len(m.Chunks))
	defer func() {
		for _, it := range iters {
			it.Close()
		}
	}()
	for _, id := range m.Chunks {
		it, err := r.store.OpenChunk(ctx, id)
		if err != nil {
			return false, applied, dups, skipped, err
		}
		iters = append(iters, &chunkCursor{iter: it})
	}

	mh := &mergeHeap{}
	for i, it := range iters {
		rec, ok, err := it.Next()
		if err != nil {
			return false, applied, dups, skipped, err
		}
		if ok {
			*mh = append(*mh, mergeItem{rec: rec, chunk: i})
		}
	}
	heap.Init(mh)

	// applied/dups/skipped are named returns accumulating THIS invocation's
	// totals (never reset); pending* are the un-checkpointed micro-batch.
	var pendingApplied, pendingDups int64
	commitCount := 0
	for mh.Len() > 0 {
		item := heap.Pop(mh).(mergeItem)
		rec := item.rec

		// Pull the next record from the same chunk.
		if next, ok, err := iters[item.chunk].Next(); err != nil {
			return false, applied, dups, skipped, err
		} else if ok {
			heap.Push(mh, mergeItem{rec: next, chunk: item.chunk})
		}

		// Resume: anything at or before the committed cursor is history.
		if !m.Cursor.IsZero() && !m.Cursor.Before(rec.Key()) {
			skipped++
			continue
		}

		var v domain.Verdict
		if ing != nil {
			v = ing.IngestHistorical(rec.Transaction)
		}
		if v.IsDuplicate {
			dups++
			pendingDups++
		} else {
			applied++
			pendingApplied++
		}
		if rec.Transaction.OccurredAt.After(m.HighWatermark) {
			m.HighWatermark = rec.Transaction.OccurredAt
		}
		m.Cursor = rec.Key()
		commitCount++

		if commitCount >= microBatch {
			m.Applied += pendingApplied
			m.Duplicates += pendingDups
			pendingApplied, pendingDups = 0, 0
			m.Alerts = alertSnapshots(ing)
			if err := r.store.SaveManifest(ctx, *m); err != nil {
				return false, applied, dups, skipped, err
			}
			r.logf("[backfill] job=%s checkpoint applied+=%d dup+=%d cursor={%s,%s,%d}",
				m.JobID, applied, dups,
				m.Cursor.OccurredAt.Format("2006-01-02T15:04:05Z07:00"),
				m.Cursor.EventID, m.Cursor.Seq)
			commitCount = 0
			if ctx.Err() != nil {
				return true, applied, dups, skipped, nil
			}
		}
	}

	// Final partial micro-batch.
	if commitCount > 0 {
		m.Applied += pendingApplied
		m.Duplicates += pendingDups
		m.Alerts = alertSnapshots(ing)
		if err := r.store.SaveManifest(ctx, *m); err != nil {
			return false, applied, dups, skipped, err
		}
	}
	return false, applied, dups, skipped, nil
}

// RestoreInto rebuilds a freshly created processor's state after a process
// restart by replaying a job's chunks in merge order. It must be called on
// an empty processor: the chunks contain the full historical stream, the
// cursor is ignored, and the processor's own EventID idempotence keeps the
// replay exactly-once. After replay, alert operational states from the
// checkpoint are restored (monotone) and the watermark is advanced to the
// job's high watermark.
func (r *Runner) RestoreInto(ctx context.Context, ing Ingester, jobID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	m, exists, err := r.store.LoadManifest(ctx, jobID)
	if err != nil {
		return err
	}
	if !exists {
		return errors.New("backfill: no checkpoint found for job " + jobID)
	}
	if len(m.Chunks) == 0 {
		// Completed-job artifacts were removed; nothing more to rebuild.
		ing.RestoreAlertStates(toStateRecords(m.Alerts))
		ing.AdvanceWatermark(m.HighWatermark)
		return nil
	}

	iters := make([]*chunkCursor, 0, len(m.Chunks))
	defer func() {
		for _, it := range iters {
			it.Close()
		}
	}()
	mh := &mergeHeap{}
	for _, id := range m.Chunks {
		it, err := r.store.OpenChunk(ctx, id)
		if err != nil {
			return err
		}
		iters = append(iters, &chunkCursor{iter: it})
	}
	for i, it := range iters {
		rec, ok, err := it.Next()
		if err != nil {
			return err
		}
		if ok {
			*mh = append(*mh, mergeItem{rec: rec, chunk: i})
		}
	}
	heap.Init(mh)

	var n int64
	for mh.Len() > 0 {
		item := heap.Pop(mh).(mergeItem)
		if next, ok, err := iters[item.chunk].Next(); err != nil {
			return err
		} else if ok {
			heap.Push(mh, mergeItem{rec: next, chunk: item.chunk})
		}
		ing.IngestHistorical(item.rec.Transaction)
		n++
		if n%DefaultMicroBatchSize == 0 && ctx.Err() != nil {
			return ctx.Err()
		}
	}
	ing.RestoreAlertStates(toStateRecords(m.Alerts))
	ing.AdvanceWatermark(m.HighWatermark)
	r.logf("[backfill] job=%s state rebuilt into processor records=%d high_watermark=%s",
		jobID, n, m.HighWatermark.Format("2006-01-02T15:04:05Z07:00"))
	return nil
}

// Progress returns the last saved checkpoint for a job.
func (r *Runner) Progress(ctx context.Context, jobID string) (*Manifest, bool, error) {
	return r.store.LoadManifest(ctx, jobID)
}

func (r *Runner) logf(format string, args ...any) { r.logger.Printf(format, args...) }

// alertSnapshots reads operational alert states from the ingester for
// persistence; nil ingester (unit harness) yields nil.
func alertSnapshots(ing Ingester) []AlertSnapshot {
	if ing == nil {
		return nil
	}
	recs := ing.SnapshotAlertStates()
	out := make([]AlertSnapshot, 0, len(recs))
	for _, r := range recs {
		out = append(out, AlertSnapshot{Key: r.Key, State: r.State, UpdatedAt: r.UpdatedAt})
	}
	return out
}

// toStateRecords converts persisted checkpoint states back to the domain type.
func toStateRecords(in []AlertSnapshot) []domain.AlertStateRecord {
	out := make([]domain.AlertStateRecord, 0, len(in))
	for _, a := range in {
		out = append(out, domain.AlertStateRecord{Key: a.Key, State: a.State, UpdatedAt: a.UpdatedAt})
	}
	return out
}

// --- k-way merge plumbing ------------------------------------------------

type chunkCursor struct {
	iter RecordIterator
}

func (c *chunkCursor) Next() (Record, bool, error) { return c.iter.Next() }
func (c *chunkCursor) Close() error                { return c.iter.Close() }

type mergeItem struct {
	rec   Record
	chunk int
}

// mergeHeap orders items by global merge key, chunk index as final
// tiebreaker (Seq already makes keys unique; chunk only guards stability).
type mergeHeap []mergeItem

func (h mergeHeap) Len() int { return len(h) }
func (h mergeHeap) Less(i, j int) bool {
	ki, kj := h[i].rec.Key(), h[j].rec.Key()
	if !ki.Equal(kj) {
		return ki.Before(kj)
	}
	return h[i].chunk < h[j].chunk
}
func (h mergeHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *mergeHeap) Push(x any)   { *h = append(*h, x.(mergeItem)) }
func (h *mergeHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}
