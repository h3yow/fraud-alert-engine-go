package processor

import (
	"fmt"
	"sort"
	"time"

	"github.com/h3yow/fraud-alert-engine-go/internal/alert"
	"github.com/h3yow/fraud-alert-engine-go/internal/domain"
	"github.com/h3yow/fraud-alert-engine-go/internal/rule"
)

// processorStateVersion is the on-disk version of a processor snapshot.
const processorStateVersion = 1

// hitMemento is the JSON shape of one rule hit.
type hitMemento struct {
	RuleID     string               `json:"rule_id"`
	Priority   int                  `json:"priority"`
	Action     domain.Action        `json:"action"`
	Reason     string               `json:"reason"`
	Detail     string               `json:"detail"`
	IsDegraded bool                 `json:"is_degraded"`
	Failure    domain.FailureReason `json:"failure"`
}

// verdictMemento is the JSON shape of a cached first verdict. IsDuplicate is
// always false in the snapshot: it is a property of a re-delivery, never of
// the cached conclusion itself.
type verdictMemento struct {
	EventID     string               `json:"event_id"`
	TxID        string               `json:"tx_id"`
	Action      domain.Action        `json:"action"`
	Hits        []hitMemento         `json:"hits"`
	AlertKey    string               `json:"alert_key"`
	Degraded    bool                 `json:"degraded"`
	Failure     domain.FailureReason `json:"failure"`
	IsLate      bool                 `json:"is_late"`
	EvaluatedAt time.Time            `json:"evaluated_at"`
}

// State is the full, self-contained snapshot of a processor: seen verdicts,
// the business-time watermark, every stateful rule's state and all alerts.
// All pieces are sampled under one processor lock, so the snapshot is always
// internally consistent (the cursor-relative rule state, verdict cache and
// alert counts can never disagree by construction).
type State struct {
	Version   int                 `json:"version"`
	Watermark time.Time           `json:"watermark"`
	Verdicts  []verdictMemento    `json:"verdicts"`
	Rules     []rule.StateMemento `json:"rules"`
	Alerts    alert.State         `json:"alerts"`
}

// SnapshotState atomically exports the complete processor state: verdict
// cache, watermark, stateful-rule state and alert store.
func (p *Processor) SnapshotState() (State, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	rules, err := p.eng.SnapshotState()
	if err != nil {
		return State{}, err
	}
	alerts := p.store.SnapshotState()

	ids := make([]string, 0, len(p.seen))
	for id := range p.seen {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := State{
		Version:   processorStateVersion,
		Watermark: p.watermark,
		Verdicts:  make([]verdictMemento, 0, len(ids)),
		Rules:     rules,
		Alerts:    alerts,
	}
	for _, id := range ids {
		v := p.seen[id]
		m := verdictMemento{
			EventID:     v.EventID,
			TxID:        v.TxID,
			Action:      v.Action,
			Hits:        make([]hitMemento, 0, len(v.Hits)),
			AlertKey:    v.AlertKey,
			Degraded:    v.Degraded,
			Failure:     v.Failure,
			IsLate:      v.IsLate,
			EvaluatedAt: v.EvaluatedAt,
		}
		for _, h := range v.Hits {
			m.Hits = append(m.Hits, hitMemento{
				RuleID: h.RuleID, Priority: h.Priority, Action: h.Action,
				Reason: h.Reason, Detail: h.Detail, IsDegraded: h.IsDegraded, Failure: h.Failure,
			})
		}
		out.Verdicts = append(out.Verdicts, m)
	}
	return out, nil
}

// RestoreState atomically imports a snapshot into a fresh (empty) processor.
// Restoring into a processor that already holds state is rejected, so a
// restarted component never silently merges two divergent histories. Rule
// state, alerts, verdict cache and watermark are restored together; on
// failure the processor must be considered unusable (defensive: this only
// runs against a freshly started, empty component).
func (p *Processor) RestoreState(st State) error {
	if st.Version != processorStateVersion {
		return fmt.Errorf("processor: unsupported snapshot version %d (want %d)", st.Version, processorStateVersion)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.seen) != 0 || !p.watermark.IsZero() {
		return fmt.Errorf("processor: cannot restore state into a non-empty processor (%d cached verdicts)", len(p.seen))
	}
	if !p.store.IsEmpty() {
		return fmt.Errorf("processor: cannot restore state: alert store is not empty")
	}
	// Rule state first (registry locks are always taken after p.mu, matching
	// the processing path), then alerts, then the verdict cache + watermark.
	if err := p.eng.RestoreState(st.Rules); err != nil {
		return err
	}
	if err := p.store.RestoreState(st.Alerts); err != nil {
		return err
	}
	for _, m := range st.Verdicts {
		hits := make([]domain.Hit, 0, len(m.Hits))
		for _, h := range m.Hits {
			hits = append(hits, domain.Hit{
				RuleID: h.RuleID, Priority: h.Priority, Action: h.Action,
				Reason: h.Reason, Detail: h.Detail, IsDegraded: h.IsDegraded, Failure: h.Failure,
			})
		}
		v := domain.Verdict{
			EventID:     m.EventID,
			TxID:        m.TxID,
			Action:      m.Action,
			Hits:        hits,
			AlertKey:    m.AlertKey,
			Degraded:    m.Degraded,
			Failure:     m.Failure,
			IsLate:      m.IsLate,
			EvaluatedAt: m.EvaluatedAt,
		}
		// Reconstruct the winning-hit pointer from the ordered Hits slice so
		// later re-deliveries replay exactly the same conclusion.
		if len(hits) > 0 {
			v.WinningHit = &v.Hits[0]
		}
		p.seen[m.EventID] = v
	}
	p.watermark = st.Watermark
	return nil
}

// IsEmpty reports whether the processor holds no cached verdict or watermark.
func (p *Processor) IsEmpty() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.seen) == 0 && p.watermark.IsZero()
}
