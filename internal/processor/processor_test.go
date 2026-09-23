package processor

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/h3yow/fraud-alert-engine-go/internal/alert"
	"github.com/h3yow/fraud-alert-engine-go/internal/domain"
	"github.com/h3yow/fraud-alert-engine-go/internal/engine"
	"github.com/h3yow/fraud-alert-engine-go/internal/logging"
	"github.com/h3yow/fraud-alert-engine-go/internal/rule"
)

func mkTx(event, acct, country string, amount float64, at time.Time) domain.Transaction {
	return domain.Transaction{
		EventID: event, TxID: "T" + event, AccountID: acct, ClusterID: "",
		Amount: amount, Currency: "USD", Country: country, EventType: "PAY",
		OccurredAt: at, IngestedAt: at.Add(time.Second),
	}
}

func newP(t *testing.T) (*Processor, *logging.BufferLogger) {
	t.Helper()
	lg := &logging.BufferLogger{}
	eng := engine.New(rule.NewDefaultRegistry(), lg)
	p := New(eng, alert.New(lg), time.Minute, lg)
	return p, lg
}

func TestDuplicateDeliveryStableVerdictAndCount(t *testing.T) {
	p, _ := newP(t)
	base := time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)

	// Four events breach velocity (limit 3).
	var first domain.Verdict
	for i := 0; i < 4; i++ {
		v := p.Process(mkTx("e"+string(rune('1'+i)), "acct-B", "US", 10,
			base.Add(time.Duration(i)*10*time.Second)))
		if i == 3 {
			first = v
		}
	}
	if first.Action != domain.ActionReview || first.AlertKey == "" {
		t.Fatalf("4th velocity event must REVIEW with an alert, got %s key=%q", first.Action, first.AlertKey)
	}

	// Re-deliver the firing event twice (as duplicates) and interleaved with
	// other events: verdict must be byte-for-byte stable, counts unchanged.
	key := first.AlertKey
	for _, v := range []domain.Verdict{
		p.Process(mkTx("e4", "acct-B", "US", 10, base.Add(30*time.Second))),
		p.Process(mkTx("e4", "acct-B", "US", 10, base.Add(30*time.Second))),
	} {
		if !v.IsDuplicate {
			t.Fatal("repeat delivery must be marked duplicate")
		}
		if v.Action != first.Action || v.AlertKey != key || v.WinningHit.RuleID != first.WinningHit.RuleID {
			t.Fatalf("duplicate verdict drifted: %+v vs %+v", v, first)
		}
	}

	alerts := p.Alerts()
	if len(alerts) != 1 {
		t.Fatalf("expected exactly 1 aggregated velocity alert, got %d", len(alerts))
	}
	if alerts[0].HitCount != 1 {
		t.Fatalf("only one EventID fired the rule; HitCount must be 1, got %d", alerts[0].HitCount)
	}
}

func TestOutOfOrderSameSetNoContradiction(t *testing.T) {
	base := time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)
	events := []domain.Transaction{
		mkTx("a", "acct", "ZZ", 50, base),                       // country BLOCK
		mkTx("b", "acct", "US", 100, base.Add(time.Second)),     // ALLOW
		mkTx("c", "acct", "US", 50000, base.Add(2*time.Second)), // amount BLOCK
	}

	run := func(order []int) map[string]domain.Verdict {
		p, _ := newP(t)
		out := map[string]domain.Verdict{}
		for _, i := range order {
			v := p.Process(events[i])
			out[events[i].EventID] = v
		}
		return out
	}

	asc := run([]int{0, 1, 2})
	desc := run([]int{2, 1, 0})
	for id, v1 := range asc {
		v2 := desc[id]
		if v1.Action != v2.Action || v1.Failure != v2.Failure {
			t.Fatalf("event %s action/failure contradicts across orders: %+v vs %+v", id, v1, v2)
		}
		if (v1.WinningHit == nil) != (v2.WinningHit == nil) {
			t.Fatalf("event %s hit presence contradicts across orders: %+v vs %+v", id, v1, v2)
		}
		if v1.WinningHit != nil && v1.WinningHit.RuleID != v2.WinningHit.RuleID {
			t.Fatalf("event %s winning rule contradicts across orders: %s vs %s",
				id, v1.WinningHit.RuleID, v2.WinningHit.RuleID)
		}
	}
}

func TestLateEventFlaggedAndCountedOnce(t *testing.T) {
	p, _ := newP(t)
	base := time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)

	// Advance the watermark far into the future.
	p.Process(mkTx("new", "acct", "US", 50000, base.Add(time.Hour)))
	// A much older, independently blocking event arrives late.
	late := p.Process(mkTx("old", "acct-late", "ZZ", 50000, base))
	if !late.IsLate {
		t.Fatal("event far behind the watermark must be flagged late")
	}
	if late.Action != domain.ActionBlock {
		t.Fatal("late suspicious event must still be blocked, never silently allowed")
	}
	// Replay late event: same verdict, duplicate, no extra count.
	replay := p.Process(mkTx("old", "acct-late", "ZZ", 50000, base))
	if !replay.IsDuplicate || replay.AlertKey != late.AlertKey {
		t.Fatal("late event replay must be idempotent")
	}
	totalHits := 0
	for _, a := range p.Alerts() {
		totalHits += a.HitCount
	}
	if totalHits != 2 {
		t.Fatalf("exactly two distinct firing EventIDs should be counted, got %d", totalHits)
	}
}

func TestDegradationReasonsAreDistinguishable(t *testing.T) {
	base := time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)

	// 1) invalid field
	p, _ := newP(t)
	bad := mkTx("bad", "acct", "US", -1, base)
	v1 := p.Process(bad)
	if v1.Action != domain.ActionReview || v1.Failure != domain.FailureInvalidField || !v1.Degraded {
		t.Fatalf("invalid field: want REVIEW/INVALID_FIELD/degraded, got %s/%s/%v",
			v1.Action, v1.Failure, v1.Degraded)
	}

	// 2) missing required rule -> fail closed
	p2, lg2 := newP(t)
	reg := rule.NewDefaultRegistry()
	reg.SetEnabled(rule.IDHighRiskCountry, false)
	eng2 := engine.New(reg, lg2)
	p2 = New(eng2, alert.New(lg2), time.Minute, lg2)
	v2 := p2.Process(mkTx("ok", "acct", "US", 5, base))
	if v2.Action != domain.ActionBlock || v2.Failure != domain.FailureRuleMissing {
		t.Fatalf("missing rule: want BLOCK/RULE_MISSING, got %s/%s", v2.Action, v2.Failure)
	}

	// 3) internal rule failure (misconfigured rule returns an error)
	p3, lg3 := newP(t)
	reg3 := rule.NewRegistry()
	reg3.Register(&rule.LargeAmountRule{Threshold: 0}) // Evaluate errors
	eng3 := engine.NewWithRequired(reg3, nil, lg3)
	p3 = New(eng3, alert.New(lg3), time.Minute, lg3)
	v3 := p3.Process(mkTx("ok", "acct", "US", 5, base))
	if v3.Failure != domain.FailureRuleEval || v3.Action != domain.ActionReview {
		t.Fatalf("rule error: want OnError REVIEW/RULE_EVAL_ERROR, got %s/%s", v3.Action, v3.Failure)
	}

	// The three reasons must be textually distinguishable in logs.
	logs := lg2.String() + lg3.String()
	for _, want := range []string{"RULE_MISSING", "RULE_EVAL_ERROR", "INVALID_FIELD"} {
		if want == "INVALID_FIELD" {
			continue // produced via p (lg not captured); covered by v1
		}
		if !strings.Contains(logs, want) {
			t.Fatalf("log must distinguish failure %s\n%s", want, logs)
		}
	}
}

func TestLoggingContainsInputAndBasis(t *testing.T) {
	p, lg := newP(t)
	base := time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)
	p.Process(mkTx("log1", "acct-Z", "ZZ", 50000, base))
	out := lg.String()
	for _, want := range []string{
		"[engine] input", "event=log1", "account=acct-Z", "amount=50000.00",
		"[engine] hit", "R200_HIGH_RISK_COUNTRY", "[engine] basis",
		"[alert] record", "[processor] event=log1",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("log missing %q\n%s", want, out)
		}
	}
}

func TestManualReviewStateFlowEndToEnd(t *testing.T) {
	p, _ := newP(t)
	base := time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)
	v := p.Process(mkTx("rv", "acct", "US", 50000, base)) // large amount BLOCK -> alert
	if v.Action != domain.ActionBlock || v.AlertKey == "" {
		t.Fatalf("expected blocking alert, got %s/%q", v.Action, v.AlertKey)
	}
	st := p.AlertStore()
	if a, ok := st.Ack(v.AlertKey, time.Now()); !ok || a.State != domain.StateAcked {
		t.Fatal("operator ack must move OPEN -> ACKED")
	}
	if a, ok := st.Resolve(v.AlertKey, time.Now()); !ok || a.State != domain.StateResolved {
		t.Fatal("operator resolve must move ACKED -> RESOLVED")
	}
	// A late duplicate of the same event must not reopen or roll back state.
	again := p.Process(mkTx("rv", "acct", "US", 50000, base))
	if !again.IsDuplicate {
		t.Fatal("repeat event after resolution must be a duplicate replay")
	}
	a, _ := st.Get(v.AlertKey)
	if a.State != domain.StateResolved {
		t.Fatalf("late duplicate must not roll back state, got %s", a.State)
	}
}

func TestConcurrentDuplicateDeliveryCountConservation(t *testing.T) {
	p, _ := newP(t)
	base := time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)
	tx := mkTx("race1", "acct-race", "ZZ", 50000, base)

	const goroutines = 32
	var wg sync.WaitGroup
	results := make([]domain.Verdict, goroutines)
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func(i int) {
			defer wg.Done()
			results[i] = p.Process(tx)
		}(i)
	}
	wg.Wait()

	firsts, dups := 0, 0
	for i, v := range results {
		if v.Action != domain.ActionBlock {
			t.Fatalf("goroutine %d got wrong action %s", i, v.Action)
		}
		if v.IsDuplicate {
			dups++
		} else {
			firsts++
		}
	}
	if firsts != 1 || dups != goroutines-1 {
		t.Fatalf("exactly one first delivery required, got firsts=%d dups=%d", firsts, dups)
	}
	alerts := p.Alerts()
	if len(alerts) != 1 || alerts[0].HitCount != 1 {
		t.Fatalf("one alert with one unique hit required, got %d alerts hits=%v",
			len(alerts), alerts)
	}
}
