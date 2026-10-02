// Package processor assembles the full local pipeline: input validation,
// duplicate suppression, late/out-of-order handling, rule evaluation,
// alert dedup/aggregation and manual-review hand-off.
//
// On top of the real-time Process entry point it exposes a historical
// ingestion path (IngestHistorical + AdvanceWatermark) used by the backfill
// package: historical events are evaluated in business-time ascending order
// without ever moving the live watermark backwards, and the watermark is
// raised monotonically only after a backfill completes.
package processor

import (
	"sync"
	"time"

	"github.com/h3yow/fraud-alert-engine-go/internal/alert"
	"github.com/h3yow/fraud-alert-engine-go/internal/domain"
	"github.com/h3yow/fraud-alert-engine-go/internal/engine"
)

// Logger is the minimal logging surface used by the processor.
type Logger interface {
	Printf(format string, args ...any)
}

// Processor is the stateful entry point of the component.
type Processor struct {
	eng       *engine.Engine
	store     *alert.Store
	logger    Logger
	lateAfter time.Duration

	mu            sync.Mutex
	seen          map[string]domain.Verdict
	watermark     time.Time
	histWatermark time.Time
	inflight      map[string]*inflightVerdict
}

// inflightVerdict lets concurrent deliveries of a not-yet-finished EventID
// wait for the single authoritative first evaluation instead of racing into
// a second evaluation (which could produce a different raw conclusion for a
// stateful rule).
type inflightVerdict struct {
	done chan struct{}
	v    domain.Verdict
}

// New builds a Processor. lateAfter defines how far behind the highest
// observed OccurredAt an event may arrive before it is flagged as late.
func New(eng *engine.Engine, store *alert.Store, lateAfter time.Duration, logger Logger) *Processor {
	if lateAfter <= 0 {
		lateAfter = time.Minute
	}
	return &Processor{
		eng:       eng,
		store:     store,
		logger:    logger,
		lateAfter: lateAfter,
		seen:      map[string]domain.Verdict{},
		inflight:  map[string]*inflightVerdict{},
	}
}

// Process feeds one possibly duplicated, late or out-of-order transaction
// into the real-time pipeline and returns its stable verdict.
//
// Guarantees:
//   - The same EventID always yields the same verdict (idempotent); the
//     second delivery is marked IsDuplicate and never touches alert counts.
//   - Late/out-of-order events are still evaluated (flagged IsLate) but
//     cannot roll back any alert state or inflate counts.
//   - Every non-ALLOW verdict (including degraded ones) produces or folds
//     into exactly one alert, so suspicious events are never silent.
func (p *Processor) Process(tx domain.Transaction) domain.Verdict {
	return p.ingest(tx, true)
}

// IngestHistorical feeds one historical transaction that the backfill layer
// has already placed in business-time ascending order. Differences from the
// real-time path:
//
//   - The event is never flagged IsLate merely because live traffic has
//     advanced the watermark far past it; otherwise backfilling old data
//     would spuriously mark every normal event as late.
//   - The live watermark is untouched. Historical progress is tracked
//     separately; the runner raises the real watermark once via
//     AdvanceWatermark when the job completes, so post-backfill late
//     detection connects smoothly to the historical business time without
//     any rollback.
//
// EventID idempotence is shared with the live path: an event already seen
// live (or in an earlier backfill) is replayed as a duplicate and never
// double-counted, which is exactly what makes resume/restart safe.
func (p *Processor) IngestHistorical(tx domain.Transaction) domain.Verdict {
	return p.ingest(tx, false)
}

// ingest is the single evaluation path. live=true uses the real-time
// watermark for late detection; live=false tracks the historical
// watermark only and never flags the event late.
func (p *Processor) ingest(tx domain.Transaction, live bool) domain.Verdict {
	p.mu.Lock()
	if cached, ok := p.seen[tx.EventID]; ok {
		p.mu.Unlock()
		cached.IsDuplicate = true
		p.logger.Printf("[processor] duplicate event=%s tx=%s replay verdict=%s alert=%s",
			tx.EventID, tx.TxID, cached.Action, cached.AlertKey)
		return cached
	}
	if infl, ok := p.inflight[tx.EventID]; ok {
		// Another goroutine is performing the authoritative first
		// evaluation of this exact EventID: wait for it and replay.
		p.mu.Unlock()
		<-infl.done
		v := infl.v
		v.IsDuplicate = true
		p.logger.Printf("[processor] duplicate event=%s tx=%s concurrent replay verdict=%s alert=%s",
			tx.EventID, tx.TxID, v.Action, v.AlertKey)
		return v
	}

	late := false
	if live {
		late = !p.watermark.IsZero() && tx.OccurredAt.Add(p.lateAfter).Before(p.watermark)
		if tx.OccurredAt.After(p.watermark) {
			p.watermark = tx.OccurredAt
		}
	} else if tx.OccurredAt.After(p.histWatermark) {
		p.histWatermark = tx.OccurredAt
	}

	infl := &inflightVerdict{done: make(chan struct{})}
	p.inflight[tx.EventID] = infl
	p.mu.Unlock()

	v := p.evaluateAndRecord(tx, late, !live)

	p.mu.Lock()
	p.seen[tx.EventID] = v
	infl.v = v
	delete(p.inflight, tx.EventID)
	p.mu.Unlock()
	close(infl.done)
	return v
}

// evaluateAndRecord runs the engine and folds a non-ALLOW verdict into the
// alert store. It is performed without holding p.mu (the engine and the
// alert store each serialize their own state), allowing different EventIDs
// to be evaluated concurrently while one EventID is evaluated exactly once.
func (p *Processor) evaluateAndRecord(tx domain.Transaction, late, historical bool) domain.Verdict {
	v := p.eng.Evaluate(tx, late)

	// Non-ALLOW conclusions (BLOCK / REVIEW, degraded or not) must surface
	// as alerts for the human review workflow.
	if v.Action != domain.ActionAllow && v.WinningHit != nil {
		a, dup := p.store.Record(v.WinningHit, tx, late)
		if a != nil {
			v.AlertKey = a.Key
		}
		p.logger.Printf("[processor] event=%s verdict=%s alert=%s alert_dup=%v degraded=%v failure=%s late=%v historical=%v",
			tx.EventID, v.Action, v.AlertKey, dup, v.Degraded, v.Failure, late, historical)
	} else {
		p.logger.Printf("[processor] event=%s verdict=%s no-alert late=%v", tx.EventID, v.Action, late)
	}
	return v
}

// HistoricalHighWatermark returns the maximum business time ingested via
// IngestHistorical so far.
func (p *Processor) HistoricalHighWatermark() time.Time {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.histWatermark
}

// AdvanceWatermark raises the real-time watermark to at least t. It is
// monotone by construction: a finished backfill connects the live pipeline
// to the historical business time, and a backfill containing only old data
// never rolls the watermark (or any late/out-of-order decision) backwards.
func (p *Processor) AdvanceWatermark(t time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if t.After(p.watermark) {
		p.logger.Printf("[processor] watermark advanced %s -> %s (historical backfill complete)",
			p.watermark.Format(time.RFC3339), t.Format(time.RFC3339))
		p.watermark = t
	}
}

// SnapshotAlertStates exports operational alert states for a checkpoint.
func (p *Processor) SnapshotAlertStates() []domain.AlertStateRecord {
	return p.store.SnapshotStates()
}

// RestoreAlertStates re-applies operational alert states from a checkpoint
// (e.g. after process restart followed by historical replay).
func (p *Processor) RestoreAlertStates(recs []domain.AlertStateRecord) {
	p.store.RestoreStates(recs)
}

// Alerts returns a stable-order snapshot of all alerts.
func (p *Processor) Alerts() []domain.Alert {
	return p.store.Snapshot()
}

// AlertStore exposes the underlying store for workflow operations such as
// Ack/Resolve.
func (p *Processor) AlertStore() *alert.Store { return p.store }
