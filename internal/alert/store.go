// Package alert implements alert deduplication, aggregation and the one-way
// alert state machine.
package alert

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/h3yow/fraud-alert-engine-go/internal/domain"
)

// Logger is the minimal logging surface used by the store.
type Logger interface {
	Printf(format string, args ...any)
}

type nopLogger struct{}

func (nopLogger) Printf(string, ...any) {}

// Cooldown is the default aggregation window: repeated hits of the same
// (rule, account) inside one window fold into a single alert.
const Cooldown = 10 * time.Minute

// Store holds the set of live alerts. All mutations are serialized.
type Store struct {
	mu       sync.Mutex
	alerts   map[string]*domain.Alert
	logger   Logger
	cooldown time.Duration
}

// New creates an empty Store with the default cooldown window.
func New(logger Logger) *Store {
	if logger == nil {
		logger = nopLogger{}
	}
	return &Store{alerts: map[string]*domain.Alert{}, logger: logger, cooldown: Cooldown}
}

// NewWithCooldown creates a Store with a custom aggregation window.
func NewWithCooldown(logger Logger, cooldown time.Duration) *Store {
	s := New(logger)
	s.cooldown = cooldown
	return s
}

// DedupKey computes the aggregation key for a hit.
//
//	Events carrying the same non-empty ClusterID share one cluster alert
//	(rule-independent); otherwise hits aggregate per (rule, account, bucket),
//	where bucket is the OccurredAt time truncated to the cooldown window.
func DedupKey(hit *domain.Hit, tx domain.Transaction, cooldown time.Duration) string {
	if tx.ClusterID != "" {
		return fmt.Sprintf("cluster:%s", tx.ClusterID)
	}
	bucket := tx.OccurredAt.Unix()
	if cooldown > 0 {
		bucket = tx.OccurredAt.Truncate(cooldown).Unix()
	}
	return fmt.Sprintf("rule:%s|acct:%s|bucket:%d", hit.RuleID, tx.AccountID, bucket)
}

// Record folds one firing event into the store. Repeated delivery of the same
// EventID for the same key is idempotent: it returns duplicate=true and does
// not inflate HitCount.
func (s *Store) Record(hit *domain.Hit, tx domain.Transaction, late bool) (*domain.Alert, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := DedupKey(hit, tx, s.cooldown)
	a := s.alerts[key]
	if a == nil {
		a = &domain.Alert{
			Key:       key,
			RuleID:    hit.RuleID,
			AccountID: tx.AccountID,
			ClusterID: tx.ClusterID,
			State:     domain.StateOpen,
			EventIDs:  map[string]struct{}{},
			FirstSeen: tx.OccurredAt,
			LastSeen:  tx.OccurredAt,
		}
		s.alerts[key] = a
	}

	_, dup := a.EventIDs[tx.EventID]
	if !dup {
		a.EventIDs[tx.EventID] = struct{}{}
		a.HitCount = len(a.EventIDs)
		if tx.OccurredAt.Before(a.FirstSeen) {
			a.FirstSeen = tx.OccurredAt
		}
		if tx.OccurredAt.After(a.LastSeen) {
			a.LastSeen = tx.OccurredAt
		}
		reason := hit.Reason
		if !containsReason(a.Reasons, reason) {
			a.Reasons = append(a.Reasons, reason)
		}
	}
	// UpdatedAt always advances with the latest ingestion-side clock; it is
	// bookkeeping, never a state transition, so it cannot "roll back" state.
	a.UpdatedAt = time.Now()

	s.logger.Printf("[alert] record key=%s event=%s duplicate=%v late=%v state=%s unique_hits=%d",
		key, tx.EventID, dup, late, a.State, a.HitCount)
	return cloneAlert(a), dup
}

// Ack implements OPEN -> ACKED. Any other transition (already acked,
// resolved, or out-of-order duplicate ack) is rejected with ok=false; the
// state never moves backwards.
func (s *Store) Ack(key string, at time.Time) (*domain.Alert, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.alerts[key]
	if a == nil {
		s.logger.Printf("[alert] ack missing key=%s rejected=true", key)
		return nil, false
	}
	if a.State != domain.StateOpen {
		s.logger.Printf("[alert] ack key=%s rejected=true current=%s", key, a.State)
		return cloneAlert(a), false
	}
	a.State = domain.StateAcked
	a.UpdatedAt = at
	s.logger.Printf("[alert] ack key=%s state=%s", key, a.State)
	return cloneAlert(a), true
}

// Resolve implements OPEN/ACKED -> RESOLVED. RESOLVED is terminal: repeated
// or out-of-order resolve calls are rejected with ok=false.
func (s *Store) Resolve(key string, at time.Time) (*domain.Alert, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.alerts[key]
	if a == nil {
		s.logger.Printf("[alert] resolve missing key=%s rejected=true", key)
		return nil, false
	}
	if a.State == domain.StateResolved {
		s.logger.Printf("[alert] resolve key=%s rejected=true current=%s", key, a.State)
		return cloneAlert(a), false
	}
	a.State = domain.StateResolved
	a.UpdatedAt = at
	s.logger.Printf("[alert] resolve key=%s state=%s", key, a.State)
	return cloneAlert(a), true
}

// Get returns a snapshot copy of the alert for a key.
func (s *Store) Get(key string) (*domain.Alert, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.alerts[key]
	if a == nil {
		return nil, false
	}
	return cloneAlert(a), true
}

// SnapshotStates exports only the operational lifecycle state (OPEN/ACKED/
// RESOLVED) of every alert. It is deliberately much smaller than Snapshot:
// backfill checkpoints must persist operator state cheaply without caching
// the (derivable, EventID-deduplicated) hit contents.
func (s *Store) SnapshotStates() []domain.AlertStateRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	keys := make([]string, 0, len(s.alerts))
	for k := range s.alerts {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]domain.AlertStateRecord, 0, len(keys))
	for _, k := range keys {
		out = append(out, domain.AlertStateRecord{
			Key: k, State: s.alerts[k].State, UpdatedAt: s.alerts[k].UpdatedAt,
		})
	}
	return out
}

// RestoreStates replays operational states from a checkpoint onto a freshly
// rebuilt store. It is monotone: a stored state never moves an alert
// backwards (OPEN->ACKED->RESOLVED ordering is respected), and alerts that
// do not exist yet are created as minimal placeholders that subsequent
// historical replay fills with hits, EventIDs and first/last timestamps.
func (s *Store) RestoreStates(recs []domain.AlertStateRecord) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rank := map[domain.AlertState]int{
		domain.StateOpen: 0, domain.StateAcked: 1, domain.StateResolved: 2,
	}
	for _, rec := range recs {
		a := s.alerts[rec.Key]
		if a == nil {
			a = &domain.Alert{
				Key: rec.Key, State: domain.StateOpen,
				EventIDs: map[string]struct{}{},
			}
			s.alerts[rec.Key] = a
		}
		if rank[rec.State] > rank[a.State] {
			a.State = rec.State
		}
		if rec.UpdatedAt.After(a.UpdatedAt) {
			a.UpdatedAt = rec.UpdatedAt
		}
	}
}

// Snapshot returns snapshot copies of all alerts in stable key order.
func (s *Store) Snapshot() []domain.Alert {
	s.mu.Lock()
	defer s.mu.Unlock()
	keys := make([]string, 0, len(s.alerts))
	for k := range s.alerts {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]domain.Alert, 0, len(keys))
	for _, k := range keys {
		out = append(out, *cloneAlert(s.alerts[k]))
	}
	return out
}

func containsReason(rs []string, r string) bool {
	for _, x := range rs {
		if x == r {
			return true
		}
	}
	return false
}

func cloneAlert(a *domain.Alert) *domain.Alert {
	c := *a
	c.EventIDs = make(map[string]struct{}, len(a.EventIDs))
	for k := range a.EventIDs {
		c.EventIDs[k] = struct{}{}
	}
	c.Reasons = append([]string(nil), a.Reasons...)
	return &c
}
