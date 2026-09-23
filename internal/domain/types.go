// Package domain defines the core data model for the local fraud-alert engine:
// transactions, rule hits, final verdicts and alerts.
package domain

import "time"

// Action is the final disposition of a transaction.
type Action string

const (
	// ActionAllow lets the transaction through.
	ActionAllow Action = "ALLOW"
	// ActionReview routes the transaction to manual review.
	ActionReview Action = "REVIEW"
	// ActionBlock blocks the transaction.
	ActionBlock Action = "BLOCK"
)

// FailureReason distinguishes different degradation causes so that the same
// surface symptom never hides different internal failures.
type FailureReason string

const (
	// FailureNone means no degradation happened; the verdict was produced by
	// normal rule evaluation.
	FailureNone FailureReason = ""
	// FailureInvalidField means a required transaction field was missing or
	// illegal (e.g. non-positive amount, malformed account id).
	FailureInvalidField FailureReason = "INVALID_FIELD"
	// FailureRuleMissing means evaluation required a rule that is not
	// registered or has been disabled. Fail-closed.
	FailureRuleMissing FailureReason = "RULE_MISSING"
	// FailureRuleEval means a registered rule panicked or otherwise failed
	// internally while evaluating the event.
	FailureRuleEval FailureReason = "RULE_EVAL_ERROR"
)

// Transaction is a single local ledger event fed to the engine.
type Transaction struct {
	EventID    string
	TxID       string
	AccountID  string
	ClusterID  string
	Amount     float64
	Currency   string
	Country    string
	EventType  string
	OccurredAt time.Time
	IngestedAt time.Time
}

// Hit records one rule firing for one transaction.
type Hit struct {
	RuleID     string
	Priority   int
	Action     Action
	Reason     string
	Detail     string
	IsDegraded bool
	Failure    FailureReason
}

// Verdict is the unique, deterministic final conclusion for one transaction.
type Verdict struct {
	EventID     string
	TxID        string
	Action      Action
	WinningHit  *Hit
	Hits        []Hit
	IsDuplicate bool
	IsLate      bool
	// AlertKey is the dedup key of the alert created/folded for this verdict;
	// empty when the verdict needs no alert (ALLOW).
	AlertKey    string
	Degraded    bool
	Failure     FailureReason
	EvaluatedAt time.Time
}

// AlertState is one node of the one-way alert lifecycle.
type AlertState string

const (
	// StateOpen: alert just fired, awaiting acknowledgment.
	StateOpen AlertState = "OPEN"
	// StateAcked: a human reviewer has acknowledged the alert.
	StateAcked AlertState = "ACKED"
	// StateResolved: the alert has been conclusively handled. Terminal.
	StateResolved AlertState = "RESOLVED"
)

// Alert is the deduplicated/aggregated unit shown to operators.
type Alert struct {
	Key       string
	RuleID    string
	AccountID string
	ClusterID string
	State     AlertState
	HitCount  int
	EventIDs  map[string]struct{}
	FirstSeen time.Time
	LastSeen  time.Time
	UpdatedAt time.Time
	Reasons   []string
}
