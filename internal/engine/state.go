package engine

import (
	"fmt"

	"github.com/h3yow/fraud-alert-engine-go/internal/rule"
)

// SnapshotState exports the state of every stateful rule (velocity, ...) in
// stable rule-id order.
func (e *Engine) SnapshotState() ([]rule.StateMemento, error) {
	out := make([]rule.StateMemento, 0)
	for _, sr := range e.reg.StatefulRules() {
		m, err := sr.SnapshotState()
		if err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, nil
}

// RestoreState restores stateful-rule state into the engine. Every memento
// must match a registered rule; rules may only be restored while they still
// hold no state (fresh process).
func (e *Engine) RestoreState(mementos []rule.StateMemento) error {
	have := map[string]rule.StatefulRule{}
	for _, sr := range e.reg.StatefulRules() {
		have[sr.ID()] = sr
	}
	for _, m := range mementos {
		sr, ok := have[m.RuleID]
		if !ok {
			return fmt.Errorf("engine: cannot restore state: rule %q is not registered", m.RuleID)
		}
		if err := sr.RestoreState(m); err != nil {
			return err
		}
	}
	return nil
}
