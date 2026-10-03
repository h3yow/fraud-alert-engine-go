package alert

import (
	"fmt"
	"sort"
	"time"

	"github.com/h3yow/fraud-alert-engine-go/internal/domain"
)

// stateVersion is the current on-disk format version of an alert Store state
// snapshot.
const stateVersion = 1

// AlertState is the serializable state of one aggregated alert. EventIDs is
// sorted so snapshots are byte-stable; FirstSeen/LastSeen are business-time
// extrema and UpdatedAt is workflow bookkeeping.
type AlertState struct {
	Key       string            `json:"key"`
	RuleID    string            `json:"rule_id"`
	AccountID string            `json:"account_id"`
	ClusterID string            `json:"cluster_id"`
	State     domain.AlertState `json:"state"`
	EventIDs  []string          `json:"event_ids"`
	FirstSeen time.Time         `json:"first_seen"`
	LastSeen  time.Time         `json:"last_seen"`
	UpdatedAt time.Time         `json:"updated_at"`
	Reasons   []string          `json:"reasons"`
}

// State is the serializable full state of a Store.
type State struct {
	Version  int          `json:"version"`
	Cooldown string       `json:"cooldown"`
	Alerts   []AlertState `json:"alerts"`
}

// SnapshotState exports a JSON-friendly deep copy of the store state, with
// alerts ordered by key and EventID sets ordered lexicographically.
func (s *Store) SnapshotState() State {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := State{
		Version:  stateVersion,
		Cooldown: s.cooldown.String(),
		Alerts:   make([]AlertState, 0, len(s.alerts)),
	}
	keys := make([]string, 0, len(s.alerts))
	for k := range s.alerts {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		a := s.alerts[k]
		ids := make([]string, 0, len(a.EventIDs))
		for id := range a.EventIDs {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		out.Alerts = append(out.Alerts, AlertState{
			Key:       a.Key,
			RuleID:    a.RuleID,
			AccountID: a.AccountID,
			ClusterID: a.ClusterID,
			State:     a.State,
			EventIDs:  ids,
			FirstSeen: a.FirstSeen,
			LastSeen:  a.LastSeen,
			UpdatedAt: a.UpdatedAt,
			Reasons:   append([]string(nil), a.Reasons...),
		})
	}
	return out
}

// RestoreState replaces the store content with a previously snapshotted
// state. The target store must be empty (a fresh component after restart):
// restoring over live state is rejected loudly rather than silently merging
// or overwriting counts. The snapshot cooldown must match the live
// configuration, since bucket keys are computed against it.
func (s *Store) RestoreState(st State) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.alerts) != 0 {
		return fmt.Errorf("alert: cannot restore state into a non-empty store (%d alerts)", len(s.alerts))
	}
	if st.Version != stateVersion {
		return fmt.Errorf("alert: unsupported snapshot version %d (want %d)", st.Version, stateVersion)
	}
	if cd := s.cooldown.String(); st.Cooldown != "" && st.Cooldown != cd {
		return fmt.Errorf("alert: cooldown drift: snapshot=%s live=%s", st.Cooldown, cd)
	}
	for _, as := range st.Alerts {
		switch as.State {
		case domain.StateOpen, domain.StateAcked, domain.StateResolved:
		default:
			return fmt.Errorf("alert: alert %s has unknown state %q", as.Key, as.State)
		}
		ids := make(map[string]struct{}, len(as.EventIDs))
		for _, id := range as.EventIDs {
			ids[id] = struct{}{}
		}
		s.alerts[as.Key] = &domain.Alert{
			Key:       as.Key,
			RuleID:    as.RuleID,
			AccountID: as.AccountID,
			ClusterID: as.ClusterID,
			State:     as.State,
			HitCount:  len(ids),
			EventIDs:  ids,
			FirstSeen: as.FirstSeen,
			LastSeen:  as.LastSeen,
			UpdatedAt: as.UpdatedAt,
			Reasons:   append([]string(nil), as.Reasons...),
		}
	}
	return nil
}

// IsEmpty reports whether the store currently holds no alerts.
func (s *Store) IsEmpty() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.alerts) == 0
}
