// Package rule defines the rule abstraction, a registry and the concrete
// built-in risk rules.
package rule

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/h3yow/fraud-alert-engine-go/internal/domain"
)

// Fixed rule ids and priorities. A smaller Priority value wins when more
// than one rule fires. Priorities are part of the public contract so that
// conflict resolution is explainable and stable.
const (
	IDLargeAmount     = "R100_LARGE_AMOUNT"
	IDHighRiskCountry = "R200_HIGH_RISK_COUNTRY"
	IDVelocity        = "R300_VELOCITY"

	PrioLargeAmount     = 200
	PrioHighRiskCountry = 100
	PrioVelocity        = 300
)

// Rule evaluates one transaction against a single risk policy.
type Rule interface {
	ID() string
	// Priority: smaller value wins when multiple rules fire.
	Priority() int
	// OnError is the degradation action used when evaluation of this rule
	// itself fails internally.
	OnError() domain.Action
	// Enabled reports whether the rule participates in evaluation.
	Enabled() bool
	// Evaluate returns a non-nil hit when the rule fires, nil otherwise.
	Evaluate(tx domain.Transaction) (*domain.Hit, error)
}

// Registry looks rules up by id and enumerates them in stable order.
type Registry struct {
	mu    sync.RWMutex
	rules map[string]Rule
}

// NewRegistry builds an empty registry.
func NewRegistry() *Registry {
	return &Registry{rules: map[string]Rule{}}
}

// Register adds or replaces a rule.
func (r *Registry) Register(rl Rule) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.rules[rl.ID()] = rl
}

// SetEnabled replaces the registration state of a rule id by wrapping the
// rule in an enable toggle. It is primarily used to simulate a missing or
// disabled rule in degradation tests.
func (r *Registry) SetEnabled(id string, enabled bool) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	rl, ok := r.rules[id]
	if !ok {
		return false
	}
	r.rules[id] = &toggledRule{Rule: rl, enabled: enabled}
	return true
}

// Get returns the rule with the given id, regardless of its enabled state.
func (r *Registry) Get(id string) (Rule, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	rl, ok := r.rules[id]
	return rl, ok
}

// All returns every registered, enabled rule in ascending priority order,
// ties broken by rule id, so evaluation and conflict resolution are
// deterministic.
func (r *Registry) All() []Rule {
	r.mu.RLock()
	all := make([]Rule, 0, len(r.rules))
	for _, rl := range r.rules {
		if rl.Enabled() {
			all = append(all, rl)
		}
	}
	r.mu.RUnlock()
	sort.Slice(all, func(i, j int) bool {
		if all[i].Priority() != all[j].Priority() {
			return all[i].Priority() < all[j].Priority()
		}
		return all[i].ID() < all[j].ID()
	})
	return all
}

// toggledRule overrides the Enabled() result of an existing rule.
type toggledRule struct {
	Rule
	enabled bool
}

func (t *toggledRule) Enabled() bool { return t.enabled }

// --- R100: large amount -------------------------------------------------

// LargeAmountRule flags transactions whose amount exceeds a fixed threshold.
type LargeAmountRule struct {
	Threshold float64
}

func (r *LargeAmountRule) ID() string             { return IDLargeAmount }
func (r *LargeAmountRule) Priority() int          { return PrioLargeAmount }
func (r *LargeAmountRule) OnError() domain.Action { return domain.ActionReview }
func (r *LargeAmountRule) Enabled() bool          { return true }

func (r *LargeAmountRule) Evaluate(tx domain.Transaction) (*domain.Hit, error) {
	if r.Threshold <= 0 {
		return nil, fmt.Errorf("large-amount rule misconfigured: threshold=%v", r.Threshold)
	}
	if tx.Amount > r.Threshold {
		return &domain.Hit{
			RuleID:   r.ID(),
			Priority: r.Priority(),
			Action:   domain.ActionBlock,
			Reason:   "amount exceeds threshold",
			Detail:   fmt.Sprintf("amount=%.2f currency=%s threshold=%.2f", tx.Amount, tx.Currency, r.Threshold),
		}, nil
	}
	return nil, nil
}

// --- R200: high risk country --------------------------------------------

// HighRiskCountryRule flags transactions originating from configured
// countries.
type HighRiskCountryRule struct {
	Countries map[string]struct{}
}

func (r *HighRiskCountryRule) ID() string             { return IDHighRiskCountry }
func (r *HighRiskCountryRule) Priority() int          { return PrioHighRiskCountry }
func (r *HighRiskCountryRule) OnError() domain.Action { return domain.ActionReview }
func (r *HighRiskCountryRule) Enabled() bool          { return true }

func (r *HighRiskCountryRule) Evaluate(tx domain.Transaction) (*domain.Hit, error) {
	if r.Countries == nil {
		return nil, fmt.Errorf("high-risk-country rule has no country set configured")
	}
	if _, bad := r.Countries[tx.Country]; bad {
		return &domain.Hit{
			RuleID:   r.ID(),
			Priority: r.Priority(),
			Action:   domain.ActionBlock,
			Reason:   "origin country is on the high-risk list",
			Detail:   fmt.Sprintf("country=%s account=%s", tx.Country, tx.AccountID),
		}, nil
	}
	return nil, nil
}

// --- R300: velocity ------------------------------------------------------

// VelocityRule flags more than MaxEvents unique transactions of one account
// inside Window, measured up to the evaluated event's own OccurredAt.
//
// State is keyed by account and EventID: duplicate deliveries are absorbed
// and only events with OccurredAt <= the evaluated event are counted, so
// the verdict for any fixed stream prefix is independent of arrival
// ordering.
type VelocityRule struct {
	Window    time.Duration
	MaxEvents int

	mu      sync.Mutex
	account map[string]map[string]time.Time
}

func (r *VelocityRule) ID() string             { return IDVelocity }
func (r *VelocityRule) Priority() int          { return PrioVelocity }
func (r *VelocityRule) OnError() domain.Action { return domain.ActionReview }
func (r *VelocityRule) Enabled() bool          { return true }

// Evaluate records the event, then counts unique events of the same account
// whose OccurredAt lies within (tx.OccurredAt-Window, tx.OccurredAt]. The
// window is defined purely on business timestamps, not arrival order, so
// whether one event "caused" the breach depends only on the stream prefix it
// belongs to. Duplicate deliveries of one EventID are absorbed by the id set.
//
// Note: a stateful rule observed mid-stream can legitimately draw different
// raw conclusions depending on which events have already been ingested. The
// pipeline (package processor) is responsible for the external consistency
// guarantee: it caches the first verdict per EventID and replays it for
// duplicate or re-delivered copies.
func (r *VelocityRule) Evaluate(tx domain.Transaction) (*domain.Hit, error) {
	if r.Window <= 0 || r.MaxEvents <= 0 {
		return nil, fmt.Errorf("velocity rule misconfigured: window=%s maxEvents=%d", r.Window, r.MaxEvents)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.account == nil {
		r.account = map[string]map[string]time.Time{}
	}
	events := r.account[tx.AccountID]
	if events == nil {
		events = map[string]time.Time{}
		r.account[tx.AccountID] = events
	}
	// Earliest-occurrence-wins for a known EventID: the merge pipeline may
	// encounter an in-batch repeat carrying a *later* OccurredAt, but the
	// reference semantics ("one EventID, the earliest business time") make
	// that repeat a no-op everywhere — including this window state. This also
	// keeps RestoreInto replays (which always arrive ascending) stable.
	if _, seen := events[tx.EventID]; !seen {
		events[tx.EventID] = tx.OccurredAt
	}
	cutoff := tx.OccurredAt.Add(-r.Window)
	count := 0
	for _, at := range events {
		if !at.Before(cutoff) && !at.After(tx.OccurredAt) {
			count++
		}
	}
	if count > r.MaxEvents {
		return &domain.Hit{
			RuleID:   r.ID(),
			Priority: r.Priority(),
			Action:   domain.ActionReview,
			Reason:   "transaction frequency exceeds velocity limit",
			Detail:   fmt.Sprintf("account=%s events_in_window=%d limit=%d window=%s", tx.AccountID, count, r.MaxEvents, r.Window),
		}, nil
	}
	return nil, nil
}

// --- default assembly ----------------------------------------------------

// NewDefaultRegistry assembles the built-in rule set with fixed priorities.
func NewDefaultRegistry() *Registry {
	reg := NewRegistry()
	reg.Register(&HighRiskCountryRule{Countries: map[string]struct{}{
		"ZZ": {}, "XX": {},
	}})
	reg.Register(&LargeAmountRule{Threshold: 10000})
	reg.Register(&VelocityRule{Window: 5 * time.Minute, MaxEvents: 3})
	return reg
}
