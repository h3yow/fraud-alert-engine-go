// Package backfill adds bounded-memory, resumable historical batch backfill
// on top of the real-time processor.
//
// A backfill session accepts historical transactions in chunks (each chunk
// may be internally out of order, may contain duplicates, and may span a
// wide business-time range). Every chunk is sorted and spilled to disk as a
// "run", so the whole batch never has to fit in memory. Run then replays
// the merged runs through processor.Processor.ProcessHistorical in global
// (OccurredAt, EventID) order, atomically persisting the cursor together
// with the complete processor state after every applied event, so the
// session can be stopped and resumed — including across a process restart
// where the component comes back EMPTY — without re-counting, skipping or
// re-concluding anything.
//
// Restart semantics (Begin):
//   - Applied progress + empty processor: the full checkpointed state
//     (verdict cache, watermark, stateful-rule state, alerts including
//     OPEN/ACKED/RESOLVED lifecycle) is restored first, then the cursor
//     prefix is skipped and replay continues after it.
//   - Applied progress + the same live processor: the prefix effects are
//     already in memory; the cursor event must be present there or Begin
//     refuses (a foreign non-empty processor cannot fork history).
//
// Consistency contract:
//   - The final per-event verdict, hit rules, alert attribution and counts
//     equal those of feeding the unique events (one record per EventID;
//     collisions keep the earliest-OccurredAt copy), sorted by business
//     time, through the processor one by one.
//   - The same EventID submitted any number of times is applied once.
//   - Real-time traffic may keep flowing through Processor.Process while a
//     backfill runs; the processor lock guarantees one stable verdict per
//     EventID and the alert store's EventID dedup keeps counts conserved.
//   - Backfilled events are never flagged late and never move alert state
//     backwards; the watermark only advances, so late/out-of-order
//     judgement for new real-time data connects smoothly afterwards.
package backfill

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/h3yow/fraud-alert-engine-go/internal/domain"
	"github.com/h3yow/fraud-alert-engine-go/internal/processor"
)

// Logger is the minimal logging surface used by the backfiller.
type Logger interface {
	Printf(format string, args ...any)
}

type nopLogger struct{}

func (nopLogger) Printf(string, ...any) {}

// defaultMaxOpenRuns bounds simultaneously open run files (memory + fds).
const defaultMaxOpenRuns = 32

// Options tunes a Backfiller.
type Options struct {
	// Dir is the persistent root directory for session state (sorted run
	// files and checkpoints). Required.
	Dir string
	// Logger receives progress logs; nil disables logging.
	Logger Logger
	// MaxOpenRuns bounds how many sorted run files may exist before they
	// are compacted into one. Default 32.
	MaxOpenRuns int
}

// snapshotVersion is the on-disk format version of a backfill snapshot.
const snapshotVersion = 2

// Stats reports session progress.
type Stats struct {
	Submitted int64 // transactions accepted via Submit (including duplicates)
	Applied   int64 // unique transactions applied to the processor
	Skipped   int64 // merged records skipped (already applied per checkpoint, or in-batch duplicates)
	Done      bool  // true once every submitted record has been applied
	LastEvent string
	LastOccur time.Time
}

// checkpoint is the persisted session state. The cursor marks every record
// with sort key <= (LastOccur, LastEvent) as applied, and Processor carries
// the complete component state (verdict cache, watermark, stateful-rule
// state, alerts with their OPEN/ACKED/RESOLVED lifecycle) sampled at that
// exact cursor. The two are persisted in one atomic file, so a process
// restart into an empty component can restore the state and then continue
// from the cursor with identical results to an uninterrupted run.
type checkpoint struct {
	Version   int             `json:"version"`
	LastEvent string          `json:"last_event"`
	LastOccur time.Time       `json:"last_occur"`
	Applied   int64           `json:"applied"`
	Submitted int64           `json:"submitted"`
	Done      bool            `json:"done"`
	Processor processor.State `json:"processor"`
}

// Backfiller runs resumable backfill sessions against a live processor.
type Backfiller struct {
	proc   *processor.Processor
	opts   Options
	logger Logger
}

// New builds a Backfiller over proc.
func New(proc *processor.Processor, opts Options) *Backfiller {
	if opts.MaxOpenRuns <= 0 {
		opts.MaxOpenRuns = defaultMaxOpenRuns
	}
	logger := opts.Logger
	if logger == nil {
		logger = nopLogger{}
	}
	return &Backfiller{proc: proc, opts: opts, logger: logger}
}

// Session is one resumable backfill job persisted under Dir/<id>.
type Session struct {
	b   *Backfiller
	id  string
	dir string

	mu sync.Mutex // guards cp
	cp checkpoint

	// stopHook, when set, is consulted after every applied event; returning
	// true stops the run exactly at that event (treated like a cancelled
	// context). Test-only deterministic stop point.
	stopHook func(applied int64) bool
}

// SetStopHook installs a deterministic stop point consulted after every
// applied event: when stop returns true the run ends cleanly at that event
// (exactly as a cancelled context would). It is intended for tests and
// local demos that need to reproduce an interruption at an exact event;
// production callers cancel the run context instead.
func (s *Session) SetStopHook(stop func(applied int64) bool) {
	s.stopHook = stop
}

// Begin creates a new session or resumes the persisted session with the
// same id. Resume behaviour depends on the attached processor:
//
//   - A session with applied progress attached to an EMPTY processor (the
//     normal "process restarted" case) has its complete state restored
//     (verdicts, watermark, stateful-rule state and alerts, including
//     lifecycle) before Begin returns, so the checkpointed prefix is
//     skipped on the next Run without losing any of its effects.
//   - Attached to a non-empty processor that already contains the cursor
//     event (same-process continuation), nothing is restored; the live
//     state is used. A non-empty processor that does NOT contain the
//     cursor state makes Begin return an error rather than silently fork.
//
// Unsubmitted work is never lost because it lives in the on-disk run files.
func (b *Backfiller) Begin(id string) (*Session, error) {
	if id == "" {
		return nil, fmt.Errorf("backfill: empty session id")
	}
	if b.opts.Dir == "" {
		return nil, fmt.Errorf("backfill: Options.Dir is required")
	}
	dir := filepath.Join(b.opts.Dir, id)
	if err := os.MkdirAll(filepath.Join(dir, "runs"), 0o755); err != nil {
		return nil, err
	}
	s := &Session{b: b, id: id, dir: dir}
	raw, err := os.ReadFile(s.checkpointPath())
	if err == nil {
		if err := json.Unmarshal(raw, &s.cp); err != nil {
			return nil, fmt.Errorf("backfill: corrupt checkpoint: %w", err)
		}
		if s.cp.Version != snapshotVersion {
			return nil, fmt.Errorf("backfill: session %s checkpoint version %d unsupported (want %d); "+
				"cannot guarantee state restoration", id, s.cp.Version, snapshotVersion)
		}
		switch {
		case s.cp.Applied > 0 && b.proc.IsEmpty():
			// Real restart: the process came back with an empty component.
			// Restore verdict cache, watermark, stateful-rule state and
			// alerts to the exact checkpoint cursor before continuing, so
			// the skipped prefix is not lost.
			if err := b.proc.RestoreState(s.cp.Processor); err != nil {
				return nil, fmt.Errorf("backfill: restore session %s: %w", id, err)
			}
			b.logger.Printf("[backfill] restart session=%s restored_state=true verdicts=%d alerts=%d applied=%d done=%v cursor=(%s,%s)",
				id, len(s.cp.Processor.Verdicts), len(s.cp.Processor.Alerts.Alerts),
				s.cp.Applied, s.cp.Done, s.cp.LastOccur.Format(time.RFC3339Nano), s.cp.LastEvent)
		case s.cp.Applied > 0:
			// Same-process continuation (e.g. ctx cancellation without a
			// process restart): the live component already holds every
			// effect of the checkpointed prefix. Verify it really does
			// before trusting the cursor, so a foreign/non-empty processor
			// can never silently fork history.
			if _, ok := b.proc.Verdict(s.cp.LastEvent); !ok {
				return nil, fmt.Errorf("backfill: session %s has applied progress to (%s,%s) but the "+
					"attached processor does not contain that state; attach the matching processor or start empty",
					id, s.cp.LastOccur.Format(time.RFC3339Nano), s.cp.LastEvent)
			}
			b.logger.Printf("[backfill] resume session=%s restored_state=false live_state_matches=true applied=%d done=%v cursor=(%s,%s)",
				id, s.cp.Applied, s.cp.Done, s.cp.LastOccur.Format(time.RFC3339Nano), s.cp.LastEvent)
		default:
			b.logger.Printf("[backfill] resume session=%s no_applied_progress submitted=%d", id, s.cp.Submitted)
		}
	} else if !os.IsNotExist(err) {
		return nil, err
	} else {
		s.cp.Version = snapshotVersion
		b.logger.Printf("[backfill] begin session=%s dir=%s", id, dir)
	}
	return s, nil
}

// Submit buffers one chunk of historical transactions (any order, duplicates
// allowed) as a sorted run on disk. Memory usage is bounded by the chunk
// size, so callers can stream arbitrarily large batches chunk by chunk.
//
// Once the session has applied progress (a previous Run), newly submitted
// records must sort strictly after the applied cursor; submitting older
// records at that point is rejected loudly instead of silently dropping
// them.
func (s *Session) Submit(chunk []domain.Transaction) error {
	if len(chunk) == 0 {
		return nil
	}
	sorted, dups := sortAndDedupe(chunk)
	s.mu.Lock()
	if s.cp.Applied > 0 {
		first := keyOf(sorted[0])
		cur := sortKey{occurred: s.cp.LastOccur, eventID: s.cp.LastEvent}
		if !lessKey(cur, first) {
			s.mu.Unlock()
			return fmt.Errorf("backfill: session %s already applied up to (%s,%s); "+
				"chunk starts at (%s,%s): refusing to silently drop older records",
				s.id, s.cp.LastOccur.Format(time.RFC3339Nano), s.cp.LastEvent,
				first.occurred.Format(time.RFC3339Nano), first.eventID)
		}
	}
	path, err := s.nextRunPath()
	if err != nil {
		s.mu.Unlock()
		return err
	}
	w, err := newRunWriter(path)
	if err != nil {
		s.mu.Unlock()
		return err
	}
	for _, tx := range sorted {
		if err := w.write(tx); err != nil {
			w.close()
			s.mu.Unlock()
			return err
		}
	}
	if err := w.close(); err != nil {
		s.mu.Unlock()
		return err
	}
	s.cp.Submitted += int64(len(chunk))
	s.cp.Done = false
	submitted := s.cp.Submitted
	if err := s.saveCheckpointLocked(); err != nil {
		s.mu.Unlock()
		return err
	}
	s.mu.Unlock()
	s.b.logger.Printf("[backfill] submit session=%s chunk=%d in_chunk_dups=%d run=%s submitted_total=%d",
		s.id, len(chunk), dups, filepath.Base(path), submitted)
	return s.compactIfNeeded()
}

// Run applies all submitted-but-unapplied transactions to the processor in
// global (OccurredAt, EventID) order. After every applied event it
// atomically persists the cursor PLUS the complete processor state;
// cancelling ctx (or SetStopHook, or a process hard-stop) simply ends the
// run. A later Run with the same id resumes exactly after the last
// checkpointed record — on an empty component Begin first restores the
// checkpointed state — so nothing is re-counted and nothing is missed.
func (s *Session) Run(ctx context.Context) (Stats, error) {
	runs, err := s.runFiles()
	if err != nil {
		return Stats{}, err
	}
	if len(runs) == 0 {
		s.mu.Lock()
		s.cp.Done = true
		err := s.saveCheckpointLocked()
		s.mu.Unlock()
		if err != nil {
			return Stats{}, err
		}
		return s.stats(), nil
	}
	s.mu.Lock()
	cursor := sortKey{occurred: s.cp.LastOccur, eventID: s.cp.LastEvent}
	hasCursor := s.cp.Applied > 0
	s.mu.Unlock()
	var skipped int64
	emit := func(tx domain.Transaction) error {
		if hasCursor && !lessKey(cursor, keyOf(tx)) {
			// At or before the checkpoint cursor. On a restarted-empty
			// component these effects were already restored as state; on an
			// in-process resume they are still live. Either way skip
			// without re-evaluating or re-counting.
			skipped++
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		v := s.b.proc.ProcessHistorical(tx)
		s.mu.Lock()
		s.cp.LastEvent = tx.EventID
		s.cp.LastOccur = tx.OccurredAt
		s.cp.Applied++
		applied := s.cp.Applied
		err := s.saveCheckpointLocked()
		s.mu.Unlock()
		if err != nil {
			return err
		}
		s.b.logger.Printf("[backfill] apply session=%s event=%s tx=%s occurred=%s verdict=%s alert=%s applied=%d",
			s.id, tx.EventID, tx.TxID, tx.OccurredAt.Format(time.RFC3339Nano), v.Action, v.AlertKey, applied)
		if s.stopHook != nil && s.stopHook(applied) {
			return context.Canceled
		}
		return nil
	}
	dups, err := mergeRuns(runs, emit)
	skipped += dups
	if err != nil {
		if ctx.Err() != nil || errors.Is(err, context.Canceled) {
			cur := s.statsWithSkipped(skipped)
			s.b.logger.Printf("[backfill] stopped session=%s applied=%d skipped=%d resume_cursor=(%s,%s)",
				s.id, cur.Applied, skipped, cur.LastOccur.Format(time.RFC3339Nano), cur.LastEvent)
			return cur, nil
		}
		return s.statsWithSkipped(skipped), err
	}
	s.mu.Lock()
	s.cp.Done = true
	err = s.saveCheckpointLocked()
	s.mu.Unlock()
	if err != nil {
		return s.statsWithSkipped(skipped), err
	}
	final := s.statsWithSkipped(skipped)
	s.b.logger.Printf("[backfill] done session=%s applied=%d skipped=%d submitted=%d",
		s.id, final.Applied, skipped, final.Submitted)
	return final, nil
}

// Progress reports the persisted progress of the session.
func (s *Session) Progress() Stats { return s.stats() }

func (s *Session) stats() Stats { return s.statsWithSkipped(0) }

func (s *Session) statsWithSkipped(skipped int64) Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return Stats{
		Submitted: s.cp.Submitted,
		Applied:   s.cp.Applied,
		Skipped:   skipped,
		Done:      s.cp.Done,
		LastEvent: s.cp.LastEvent,
		LastOccur: s.cp.LastOccur,
	}
}

// --- persistence helpers ---------------------------------------------------

func (s *Session) checkpointPath() string {
	return filepath.Join(s.dir, "checkpoint.json")
}

// saveCheckpointLocked persists the cursor together with the complete
// processor state, atomically (tmp file + rename), so a crash can never
// leave a torn checkpoint or a cursor without the state it refers to. The
// caller must hold s.mu. SnapshotState takes the processor lock (which may
// take rule/alert locks) while s.mu is held; no code path ever takes s.mu
// while holding the processor lock, so the ordering cannot deadlock.
func (s *Session) saveCheckpointLocked() error {
	st, err := s.b.proc.SnapshotState()
	if err != nil {
		return err
	}
	s.cp.Version = snapshotVersion
	s.cp.Processor = st
	raw, err := json.Marshal(s.cp)
	if err != nil {
		return err
	}
	tmp := s.checkpointPath() + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, s.checkpointPath())
}

func (s *Session) runFiles() ([]string, error) {
	entries, err := os.ReadDir(filepath.Join(s.dir, "runs"))
	if err != nil {
		return nil, err
	}
	var runs []string
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".jsonl" {
			runs = append(runs, filepath.Join(s.dir, "runs", e.Name()))
		}
	}
	sort.Strings(runs)
	return runs, nil
}

func (s *Session) nextRunPath() (string, error) {
	runs, err := s.runFiles()
	if err != nil {
		return "", err
	}
	next := 0
	if len(runs) > 0 {
		base := filepath.Base(runs[len(runs)-1])
		n, err := strconv.Atoi(base[:len(base)-len(".jsonl")])
		if err != nil {
			return "", fmt.Errorf("backfill: unparsable run file %s", base)
		}
		next = n + 1
	}
	return filepath.Join(s.dir, "runs", fmt.Sprintf("%06d.jsonl", next)), nil
}

// compactIfNeeded merges all runs into one when too many have accumulated,
// bounding open-file handles and merge memory. Compaction is itself a
// streaming merge, so memory stays bounded by one record per run.
func (s *Session) compactIfNeeded() error {
	runs, err := s.runFiles()
	if err != nil {
		return err
	}
	if len(runs) < s.b.opts.MaxOpenRuns {
		return nil
	}
	// Compact into a temporary run outside the runs dir numbering, then
	// atomically replace the run set.
	tmp := filepath.Join(s.dir, "compact.tmp")
	w, err := newRunWriter(tmp)
	if err != nil {
		return err
	}
	if _, err := mergeRuns(runs, func(tx domain.Transaction) error { return w.write(tx) }); err != nil {
		w.close()
		os.Remove(tmp)
		return err
	}
	if err := w.close(); err != nil {
		os.Remove(tmp)
		return err
	}
	for _, r := range runs {
		if err := os.Remove(r); err != nil {
			return err
		}
	}
	path, err := s.nextRunPath()
	if err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	s.b.logger.Printf("[backfill] compact session=%s merged_runs=%d into=%s", s.id, len(runs), filepath.Base(path))
	return nil
}
