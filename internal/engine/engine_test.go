package engine

import (
	"strings"
	"testing"
	"time"

	"github.com/h3yow/fraud-alert-engine-go/internal/domain"
	"github.com/h3yow/fraud-alert-engine-go/internal/logging"
	"github.com/h3yow/fraud-alert-engine-go/internal/rule"
)

func validTx() domain.Transaction {
	return domain.Transaction{
		EventID:    "e1",
		TxID:       "t1",
		AccountID:  "a1",
		Amount:     100,
		Currency:   "USD",
		Country:    "US",
		EventType:  "PAY",
		OccurredAt: time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC),
		IngestedAt: time.Now(),
	}
}

func TestValidate(t *testing.T) {
	cases := []struct {
		mutate func(*domain.Transaction)
		want   domain.FailureReason
	}{
		{func(x *domain.Transaction) { x.EventID = "" }, domain.FailureInvalidField},
		{func(x *domain.Transaction) { x.TxID = "" }, domain.FailureInvalidField},
		{func(x *domain.Transaction) { x.AccountID = "" }, domain.FailureInvalidField},
		{func(x *domain.Transaction) { x.Amount = 0 }, domain.FailureInvalidField},
		{func(x *domain.Transaction) { x.Currency = "" }, domain.FailureInvalidField},
		{func(x *domain.Transaction) { x.OccurredAt = time.Time{} }, domain.FailureInvalidField},
	}
	for i, c := range cases {
		tx := validTx()
		c.mutate(&tx)
		if got := Validate(tx); got != c.want {
			t.Fatalf("case %d: want %s got %s", i, c.want, got)
		}
	}
	if Validate(validTx()) != domain.FailureNone {
		t.Fatal("valid tx must pass validation")
	}
}

func TestConflictResolvedByPriority(t *testing.T) {
	// High-risk country (prio 100) and large amount (prio 200) both fire.
	reg := rule.NewDefaultRegistry()
	lg := &logging.BufferLogger{}
	eng := New(reg, lg)
	tx := validTx()
	tx.Country = "ZZ"
	tx.Amount = 50000

	v := eng.Evaluate(tx, false)
	if v.Action != domain.ActionBlock {
		t.Fatalf("conflicting BLOCK rules must yield BLOCK, got %s", v.Action)
	}
	if v.WinningHit == nil || v.WinningHit.RuleID != rule.IDHighRiskCountry {
		t.Fatalf("lower priority number must win, got %v", v.WinningHit)
	}
	if len(v.Hits) != 2 {
		t.Fatalf("both hits must be retained for explainability, got %d", len(v.Hits))
	}
	if !strings.Contains(lg.String(), "winner="+rule.IDHighRiskCountry) {
		t.Fatalf("log must print the winning basis; log:\n%s", lg.String())
	}
}

func TestInvalidFieldDegradesToReview(t *testing.T) {
	eng := New(rule.NewDefaultRegistry(), &logging.BufferLogger{})
	tx := validTx()
	tx.Amount = -1
	v := eng.Evaluate(tx, false)
	if v.Action != domain.ActionReview || !v.Degraded || v.Failure != domain.FailureInvalidField {
		t.Fatalf("invalid field => degraded REVIEW/INVALID_FIELD, got action=%s degraded=%v failure=%s",
			v.Action, v.Degraded, v.Failure)
	}
}

func TestMissingRequiredRuleFailsClosed(t *testing.T) {
	reg := rule.NewDefaultRegistry()
	reg.SetEnabled(rule.IDHighRiskCountry, false)
	eng := New(reg, &logging.BufferLogger{})
	tx := validTx()
	tx.Amount = 1 // ensure no other rule blocks

	v := eng.Evaluate(tx, false)
	if v.Action != domain.ActionBlock || !v.Degraded || v.Failure != domain.FailureRuleMissing {
		t.Fatalf("missing required rule => fail-closed BLOCK/RULE_MISSING, got action=%s degraded=%v failure=%s",
			v.Action, v.Degraded, v.Failure)
	}
	if v.WinningHit == nil || v.WinningHit.RuleID != synthMissingRule {
		t.Fatalf("synthetic missing-rule hit must win, got %v", v.WinningHit)
	}
}

// errorRule always returns an error; panicRule always panics. Both exercise
// the OnError degradation path with distinguishable failure reasons.
type errorRule struct{}

func (errorRule) ID() string             { return "X_ERROR" }
func (errorRule) Priority() int          { return 500 }
func (errorRule) OnError() domain.Action { return domain.ActionReview }
func (errorRule) Enabled() bool          { return true }
func (errorRule) Evaluate(domain.Transaction) (*domain.Hit, error) {
	return nil, errBoom
}

type panicRule struct{}

func (panicRule) ID() string             { return "X_PANIC" }
func (panicRule) Priority() int          { return 501 }
func (panicRule) OnError() domain.Action { return domain.ActionBlock }
func (panicRule) Enabled() bool          { return true }
func (panicRule) Evaluate(domain.Transaction) (*domain.Hit, error) {
	panic("kaboom")
}

// errBoom is a package-level sentinel so errorRule has a stable value.
var errBoom = newBoomErr()

type boomErr struct{}

func newBoomErr() error       { return boomErr{} }
func (boomErr) Error() string { return "boom" }

func TestRuleErrorAndPanicDegradation(t *testing.T) {
	reg := rule.NewRegistry()
	reg.Register(errorRule{})
	reg.Register(panicRule{})
	eng := NewWithRequired(reg, nil, &logging.BufferLogger{})

	v := eng.Evaluate(validTx(), false)
	if !v.Degraded || v.Failure != domain.FailureRuleEval {
		t.Fatalf("failing rules => degraded RULE_EVAL_ERROR, got degraded=%v failure=%s",
			v.Degraded, v.Failure)
	}
	// panicRule declares BLOCK OnError and has the smaller id tie-break...
	// but priority 500 (error, REVIEW) < 501 (panic, BLOCK): REVIEW wins.
	if v.Action != domain.ActionReview {
		t.Fatalf("OnError fallback of the winning (lowest prio) failing rule must apply, got %s", v.Action)
	}
	var sawPanic, sawError bool
	for _, h := range v.Hits {
		if h.RuleID == "X_PANIC" && strings.Contains(h.Detail, "panicked") {
			sawPanic = true
		}
		if h.RuleID == "X_ERROR" && strings.Contains(h.Detail, "boom") {
			sawError = true
		}
	}
	if !sawPanic || !sawError {
		t.Fatalf("both failure modes must be recorded distinctly: %+v", v.Hits)
	}
}

func TestLateFlagPropagated(t *testing.T) {
	v := New(rule.NewDefaultRegistry(), &logging.BufferLogger{}).Evaluate(validTx(), true)
	if !v.IsLate {
		t.Fatal("late flag must propagate into the verdict")
	}
}
