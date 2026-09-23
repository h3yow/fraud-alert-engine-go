// Package engine combines the verdicts of every applicable rule into one
// deterministic final conclusion, applying stable priority ordering and
// degradation policies for invalid input, missing rules and internal errors.
package engine

import (
	"fmt"
	"sort"
	"time"

	"github.com/h3yow/fraud-alert-engine-go/internal/domain"
	"github.com/h3yow/fraud-alert-engine-go/internal/rule"
)

// Logger is the minimal logging surface used by the engine.
type Logger interface {
	Printf(format string, args ...any)
}

// nopLogger keeps the engine usable without an injected logger.
type nopLogger struct{}

func (nopLogger) Printf(string, ...any) {}

// Synthetic rule id used when a required rule is absent/disabled. It sorts
// ahead of every built-in rule so that fail-closed BLOCK always wins.
const (
	synthMissingRule  = "S000_MISSING_REQUIRED_RULE"
	synthInvalidInput = "S001_INVALID_INPUT"
)

// Engine evaluates transactions against the registered rule set.
type Engine struct {
	reg      *rule.Registry
	required []string
	logger   Logger
}

// New builds an Engine over the given registry; every built-in rule id is
// treated as required.
func New(reg *rule.Registry, logger Logger) *Engine {
	return NewWithRequired(reg, []string{
		rule.IDHighRiskCountry,
		rule.IDLargeAmount,
		rule.IDVelocity,
	}, logger)
}

// NewWithRequired builds an Engine with an explicit required-rule list.
// A missing or disabled required rule triggers fail-closed degradation.
func NewWithRequired(reg *rule.Registry, required []string, logger Logger) *Engine {
	if logger == nil {
		logger = nopLogger{}
	}
	return &Engine{reg: reg, required: append([]string(nil), required...), logger: logger}
}

// validateDetail returns a human-readable description of the first invalid
// field, or "" when the transaction is structurally sound.
func validateDetail(tx domain.Transaction) string {
	switch {
	case tx.EventID == "":
		return "empty EventID"
	case tx.TxID == "":
		return "empty TxID"
	case tx.AccountID == "":
		return "empty AccountID"
	case tx.Amount <= 0:
		return fmt.Sprintf("non-positive Amount: %v", tx.Amount)
	case tx.Currency == "":
		return "empty Currency"
	case tx.OccurredAt.IsZero():
		return "zero OccurredAt"
	default:
		return ""
	}
}

// Validate performs structural validation of a transaction and returns the
// failure reason (domain.FailureNone when valid).
func Validate(tx domain.Transaction) domain.FailureReason {
	if validateDetail(tx) != "" {
		return domain.FailureInvalidField
	}
	return domain.FailureNone
}

// Evaluate runs all enabled rules and resolves conflicts by fixed priority.
// It never panics: a panicking or erroring rule is converted into a degraded
// hit whose action is the rule's configured OnError fallback.
func (e *Engine) Evaluate(tx domain.Transaction, late bool) domain.Verdict {
	now := time.Now()
	v := domain.Verdict{
		EventID:     tx.EventID,
		TxID:        tx.TxID,
		IsLate:      late,
		Action:      domain.ActionAllow,
		Hits:        []domain.Hit{},
		EvaluatedAt: now,
	}

	e.logger.Printf("[engine] input event=%s tx=%s account=%s cluster=%s amount=%.2f %s country=%s occurred=%s ingested=%s late=%v",
		tx.EventID, tx.TxID, tx.AccountID, tx.ClusterID, tx.Amount, tx.Currency, tx.Country,
		tx.OccurredAt.Format(time.RFC3339), tx.IngestedAt.Format(time.RFC3339), late)

	// 1) Structural validation -> manual review, never a silent pass.
	if detail := validateDetail(tx); detail != "" {
		h := domain.Hit{
			RuleID:     synthInvalidInput,
			Priority:   50,
			Action:     domain.ActionReview,
			Reason:     "transaction failed structural validation",
			Detail:     detail,
			IsDegraded: true,
			Failure:    domain.FailureInvalidField,
		}
		v.Hits = append(v.Hits, h)
		v.WinningHit = &v.Hits[0]
		v.Action = h.Action
		v.Degraded = true
		v.Failure = h.Failure
		e.logger.Printf("[engine] degraded reason=%s detail=%s verdict=%s", h.Failure, h.Detail, v.Action)
		return v
	}

	// 2) Required rules present and enabled? Missing -> fail-closed BLOCK.
	for _, id := range e.required {
		rl, ok := e.reg.Get(id)
		if !ok || !rl.Enabled() {
			v.Hits = append(v.Hits, domain.Hit{
				RuleID:     synthMissingRule,
				Priority:   0,
				Action:     domain.ActionBlock,
				Reason:     "required rule is missing or disabled; fail-closed",
				Detail:     fmt.Sprintf("missing_rule=%s", id),
				IsDegraded: true,
				Failure:    domain.FailureRuleMissing,
			})
		}
	}

	// 3) Evaluate every enabled rule; panic/error -> degraded hit.
	for _, rl := range e.reg.All() {
		hit, err := e.safeEvaluate(rl, tx)
		if err != nil {
			v.Hits = append(v.Hits, domain.Hit{
				RuleID:     rl.ID(),
				Priority:   rl.Priority(),
				Action:     rl.OnError(),
				Reason:     "rule evaluation failed; applied OnError fallback",
				Detail:     err.Error(),
				IsDegraded: true,
				Failure:    domain.FailureRuleEval,
			})
			continue
		}
		if hit != nil {
			v.Hits = append(v.Hits, *hit)
		}
	}

	// 4) Deterministic winner: smallest Priority, then smallest RuleID.
	sort.SliceStable(v.Hits, func(i, j int) bool {
		if v.Hits[i].Priority != v.Hits[j].Priority {
			return v.Hits[i].Priority < v.Hits[j].Priority
		}
		return v.Hits[i].RuleID < v.Hits[j].RuleID
	})
	if len(v.Hits) > 0 {
		w := v.Hits[0]
		v.WinningHit = &w
		v.Action = w.Action
		if w.IsDegraded {
			v.Degraded = true
			v.Failure = w.Failure
		}
	}

	for _, h := range v.Hits {
		e.logger.Printf("[engine] hit rule=%s prio=%d action=%s degraded=%v failure=%s reason=%q detail=%s",
			h.RuleID, h.Priority, h.Action, h.IsDegraded, h.Failure, h.Reason, h.Detail)
	}
	if v.WinningHit != nil {
		e.logger.Printf("[engine] basis event=%s winner=%s(%d) verdict=%s degraded=%v failure=%s hits=%d",
			tx.EventID, v.WinningHit.RuleID, v.WinningHit.Priority, v.Action, v.Degraded, v.Failure, len(v.Hits))
	} else {
		e.logger.Printf("[engine] basis event=%s no-rule-fired verdict=%s", tx.EventID, v.Action)
	}
	return v
}

// safeEvaluate isolates panics and normalizes them to errors.
func (e *Engine) safeEvaluate(rl rule.Rule, tx domain.Transaction) (hit *domain.Hit, err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("rule %s panicked: %v", rl.ID(), rec)
			hit = nil
		}
	}()
	return rl.Evaluate(tx)
}
