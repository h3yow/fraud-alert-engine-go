package rule

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"
)

// StateMemento is one rule's serializable state.
type StateMemento struct {
	RuleID  string
	Version int
	Data    []byte
}

// StatefulRule is implemented by rules whose evaluation depends on the set of
// previously observed events (currently the velocity rule). It lets the
// pipeline export and restore rule state as part of a checkpoint, so a
// backfill resumed into a freshly started process yields the same verdicts
// as an uninterrupted run.
type StatefulRule interface {
	Rule
	// SnapshotState returns an opaque, JSON-friendly blob of the rule state.
	SnapshotState() (StateMemento, error)
	// RestoreState replaces the rule state with a previously snapshotted
	// blob. The rule must currently hold no state (fresh process).
	RestoreState(StateMemento) error
}

// RuleState is the JSON shape of one rule's snapshot payload.
type RuleState struct {
	Version int `json:"version"`
	// entries: account -> eventID -> OccurredAt
	Entries map[string]map[string]time.Time `json:"entries"`
}

// velocityStateVersion is the on-disk version of the velocity rule payload.
const velocityStateVersion = 1

// SnapshotState implements StatefulRule for VelocityRule: it deep-copies the
// account -> EventID -> OccurredAt tables under the rule lock.
func (r *VelocityRule) SnapshotState() (StateMemento, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	rs := RuleState{Version: velocityStateVersion, Entries: map[string]map[string]time.Time{}}
	for acct, events := range r.account {
		cp := make(map[string]time.Time, len(events))
		for id, at := range events {
			cp[id] = at
		}
		rs.Entries[acct] = cp
	}
	data, err := json.Marshal(rs)
	if err != nil {
		return StateMemento{}, err
	}
	return StateMemento{RuleID: r.ID(), Version: velocityStateVersion, Data: data}, nil
}

// RestoreState implements StatefulRule for VelocityRule: it replaces the
// tables wholesale (fresh-process resume).
func (r *VelocityRule) RestoreState(m StateMemento) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if m.RuleID != r.ID() {
		return fmt.Errorf("rule: memento rule id %q does not match %q", m.RuleID, r.ID())
	}
	if m.Version != velocityStateVersion {
		return fmt.Errorf("rule: unsupported velocity snapshot version %d", m.Version)
	}
	var rs RuleState
	if err := json.Unmarshal(m.Data, &rs); err != nil {
		return fmt.Errorf("rule: corrupt velocity snapshot: %w", err)
	}
	if rs.Version != velocityStateVersion {
		return fmt.Errorf("rule: unsupported velocity payload version %d", rs.Version)
	}
	accounts := make(map[string]map[string]time.Time, len(rs.Entries))
	for acct, events := range rs.Entries {
		cp := make(map[string]time.Time, len(events))
		for id, at := range events {
			cp[id] = at
		}
		accounts[acct] = cp
	}
	r.account = accounts
	return nil
}

// StatefulRules enumerates every registered rule implementing StatefulRule
// (regardless of enabled state), in ascending rule id order, so snapshot
// export and restore iterate rules deterministically. A disabled toggle
// still exposes the underlying rule's state.
func (reg *Registry) StatefulRules() []StatefulRule {
	reg.mu.RLock()
	defer reg.mu.RUnlock()
	var out []StatefulRule
	for _, rl := range reg.rules {
		if t, ok := rl.(*toggledRule); ok {
			rl = t.Rule
		}
		if sr, ok := rl.(StatefulRule); ok {
			out = append(out, sr)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID() < out[j].ID() })
	return out
}
