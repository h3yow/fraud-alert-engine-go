package rule

import (
	"testing"
	"time"

	"github.com/h3yow/fraud-alert-engine-go/internal/domain"
)

func txAt(id, acct, country string, amount float64, at time.Time) domain.Transaction {
	return domain.Transaction{
		EventID: id, TxID: "T-" + id, AccountID: acct,
		Amount: amount, Currency: "USD", Country: country,
		OccurredAt: at, IngestedAt: at.Add(time.Second),
	}
}

func TestRegistryStableOrderAndToggle(t *testing.T) {
	reg := NewRegistry()
	reg.Register(&LargeAmountRule{Threshold: 10})
	reg.Register(&HighRiskCountryRule{Countries: map[string]struct{}{"ZZ": {}}})

	all := reg.All()
	if len(all) != 2 || all[0].ID() != IDHighRiskCountry {
		t.Fatalf("expected country rule (prio %d) first, got %v", PrioHighRiskCountry, all)
	}

	if !reg.SetEnabled(IDHighRiskCountry, false) {
		t.Fatal("SetEnabled returned false for registered rule")
	}
	all = reg.All()
	if len(all) != 1 || all[0].ID() != IDLargeAmount {
		t.Fatalf("disabled rule must not appear in All, got %v", all)
	}
	// Get still finds it (so the engine can distinguish missing vs disabled).
	if rl, ok := reg.Get(IDHighRiskCountry); !ok || rl.Enabled() {
		t.Fatal("disabled rule should remain retrievable but report Enabled=false")
	}
}

func TestLargeAmountRule(t *testing.T) {
	r := &LargeAmountRule{Threshold: 10000}
	at := time.Now()
	if h, err := r.Evaluate(txAt("e1", "a", "US", 9999.99, at)); err != nil || h != nil {
		t.Fatalf("below threshold must not fire, got hit=%v err=%v", h, err)
	}
	h, err := r.Evaluate(txAt("e2", "a", "US", 10001, at))
	if err != nil || h == nil || h.Action != domain.ActionBlock {
		t.Fatalf("above threshold must BLOCK, hit=%v err=%v", h, err)
	}
	if _, err := (&LargeAmountRule{Threshold: 0}).Evaluate(txAt("e3", "a", "US", 1, at)); err == nil {
		t.Fatal("misconfigured threshold must surface an error")
	}
}

func TestHighRiskCountryRule(t *testing.T) {
	r := &HighRiskCountryRule{Countries: map[string]struct{}{"ZZ": {}}}
	at := time.Now()
	if h, _ := r.Evaluate(txAt("e1", "a", "US", 1, at)); h != nil {
		t.Fatalf("safe country must not fire, got %v", h)
	}
	h, _ := r.Evaluate(txAt("e2", "a", "ZZ", 1, at))
	if h == nil || h.Action != domain.ActionBlock {
		t.Fatalf("high-risk country must BLOCK, got %v", h)
	}
}

func TestVelocityWindowAndIdempotence(t *testing.T) {
	base := time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)
	r := &VelocityRule{Window: 5 * time.Minute, MaxEvents: 3}

	evs := []domain.Transaction{
		txAt("e1", "acct", "US", 1, base),
		txAt("e2", "acct", "US", 1, base.Add(10*time.Second)),
		txAt("e3", "acct", "US", 1, base.Add(20*time.Second)),
	}
	for _, ev := range evs {
		if h, err := r.Evaluate(ev); err != nil || h != nil {
			t.Fatalf("event %s must not fire at/below limit, hit=%v err=%v", ev.EventID, h, err)
		}
	}
	h, err := r.Evaluate(txAt("e4", "acct", "US", 1, base.Add(30*time.Second)))
	if err != nil || h == nil || h.Action != domain.ActionReview {
		t.Fatalf("4th unique event in window must REVIEW, hit=%v err=%v", h, err)
	}

	// Replaying any previously ingested EventID must be absorbed: counting
	// uses the unique id set, so no error and no panic on re-entry.
	if _, err := r.Evaluate(evs[1]); err != nil {
		t.Fatalf("duplicate delivery must be idempotent: %v", err)
	}

	// An event outside the window does not contribute.
	if h, err := r.Evaluate(txAt("e5", "acct", "US", 1, base.Add(6*time.Minute))); err != nil || h != nil {
		t.Fatalf("event outside the window must not fire, hit=%v err=%v", h, err)
	}
}

func TestDefaultRegistry(t *testing.T) {
	reg := NewDefaultRegistry()
	ids := []string{IDHighRiskCountry, IDLargeAmount, IDVelocity}
	for _, id := range ids {
		if _, ok := reg.Get(id); !ok {
			t.Fatalf("default registry missing %s", id)
		}
	}
}
