// Package processor assembles the full local pipeline: input validation,
// duplicate suppression, late/out-of-order handling, rule evaluation,
// alert dedup/aggregation and manual-review hand-off.
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

	mu        sync.Mutex
	seen      map[string]domain.Verdict
	watermark time.Time
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
	}
}

// Process feeds one possibly duplicated, late or out-of-order transaction
// into the pipeline and returns its stable verdict.
//
// Guarantees:
//   - The same EventID always yields the same verdict (idempotent); the
//     second delivery is marked IsDuplicate and never touches alert counts.
//   - Late/out-of-order events are still evaluated (flagged IsLate) but
//     cannot roll back any alert state or inflate counts.
//   - Every non-ALLOW verdict (including degraded ones) produces or folds
//     into exactly one alert, so suspicious events are never silent.
//
// The whole pipeline for one event runs under the processor lock, so a
// concurrent real-time delivery and a historical backfill of the same
// EventID can never produce two different verdicts: the first evaluation
// wins and every later delivery replays the cached verdict.
func (p *Processor) Process(tx domain.Transaction) domain.Verdict {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.processLocked(tx, true)
}

// ProcessHistorical feeds one historical (backfilled) transaction into the
// pipeline. It differs from Process only in the late flag: a backfilled
// stream is replayed in business-time order, so its events are never
// "late" and must not be flagged as anomalies just because the real-time
// watermark has already moved past them. The watermark itself still
// advances monotonically, so late/out-of-order judgement for subsequent
// real-time traffic connects smoothly to the backfilled business time.
func (p *Processor) ProcessHistorical(tx domain.Transaction) domain.Verdict {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.processLocked(tx, false)
}

// processLocked is the shared pipeline; the caller must hold p.mu.
// markLate selects real-time (watermark-based late flag) vs historical
// (never late) semantics.
func (p *Processor) processLocked(tx domain.Transaction, markLate bool) domain.Verdict {
	if cached, ok := p.seen[tx.EventID]; ok {
		cached.IsDuplicate = true
		p.logger.Printf("[processor] duplicate event=%s tx=%s replay verdict=%s alert=%s",
			tx.EventID, tx.TxID, cached.Action, cached.AlertKey)
		return cached
	}
	late := markLate && !p.watermark.IsZero() && tx.OccurredAt.Add(p.lateAfter).Before(p.watermark)
	if tx.OccurredAt.After(p.watermark) {
		p.watermark = tx.OccurredAt
	}

	v := p.eng.Evaluate(tx, late)

	// Non-ALLOW conclusions (BLOCK / REVIEW, degraded or not) must surface
	// as alerts for the human review workflow.
	if v.Action != domain.ActionAllow && v.WinningHit != nil {
		a, dup := p.store.Record(v.WinningHit, tx, late)
		if a != nil {
			v.AlertKey = a.Key
		}
		p.logger.Printf("[processor] event=%s verdict=%s alert=%s alert_dup=%v degraded=%v failure=%s",
			tx.EventID, v.Action, v.AlertKey, dup, v.Degraded, v.Failure)
	} else {
		p.logger.Printf("[processor] event=%s verdict=%s no-alert late=%v", tx.EventID, v.Action, late)
	}

	p.seen[tx.EventID] = v
	return v
}

// Verdict returns the cached verdict for an already-processed EventID.
func (p *Processor) Verdict(eventID string) (domain.Verdict, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	v, ok := p.seen[eventID]
	return v, ok
}

// Alerts returns a stable-order snapshot of all alerts.
func (p *Processor) Alerts() []domain.Alert {
	return p.store.Snapshot()
}

// AlertStore exposes the underlying store for workflow operations such as
// Ack/Resolve.
func (p *Processor) AlertStore() *alert.Store { return p.store }
