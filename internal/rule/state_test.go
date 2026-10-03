package rule

import (
	"testing"
	"time"

	"github.com/h3yow/fraud-alert-engine-go/internal/domain"
)

func vtx(id, acct string, at time.Time) domain.Transaction {
	return domain.Transaction{EventID: id, TxID: "T" + id, AccountID: acct,
		Amount: 1, Currency: "USD", OccurredAt: at}
}

// TestVelocitySnapshotRestore proves the stateful-rule memento round trips:
// after restore the window tables are identical row for row, duplicates stay
// absorbed, and the next event breaches exactly when it would without a
// restart.
func TestVelocitySnapshotRestore(t *testing.T) {
	base := time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC)
	src := &VelocityRule{Window: 5 * time.Minute, MaxEvents: 3}
	for i := 0; i < 4; i++ {
		h, err := src.Evaluate(vtx("v"+string(rune('0'+i)), "acct-v",
			base.Add(time.Duration(i)*time.Minute)))
		if err != nil {
			t.Fatal(err)
		}
		if i < 3 && h != nil {
			t.Fatalf("event %d must not fire yet", i)
		}
		if i == 3 && h == nil {
			t.Fatal("4th event must breach the velocity limit")
		}
	}
	// Re-delivery of a known EventID is absorbed, not double-inserted.
	if h, _ := src.Evaluate(vtx("v1", "acct-v", base.Add(time.Minute))); h != nil {
		t.Fatal("re-delivery of v1 must stay absorbed")
	}

	m, err := src.SnapshotState()
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	dst := &VelocityRule{Window: 5 * time.Minute, MaxEvents: 3}
	if err := dst.RestoreState(m); err != nil {
		t.Fatalf("restore: %v", err)
	}

	// Row-for-row state equality right after restore.
	src.mu.Lock()
	dst.mu.Lock()
	if len(dst.account) != len(src.account) {
		t.Fatalf("account rows after restore: %d vs %d", len(dst.account), len(src.account))
	}
	for acct, events := range src.account {
		got := dst.account[acct]
		if len(got) != len(events) {
			t.Fatalf("account %s event rows: %d vs %d", acct, len(got), len(events))
		}
		for id, at := range events {
			if gat, ok := got[id]; !ok || !gat.Equal(at) {
				t.Fatalf("row %s/%s mismatch: %v vs %v", acct, id, gat, at)
			}
		}
	}
	dst.mu.Unlock()
	src.mu.Unlock()

	// A 5th unique event inside the window breaches on the restored rule.
	h, err := dst.Evaluate(vtx("v9", "acct-v", base.Add(4*time.Minute)))
	if err != nil {
		t.Fatal(err)
	}
	if h == nil {
		t.Fatal("restored rule must retain the 4 prior events and breach on #5")
	}
	// A foreign memento is rejected.
	bad := m
	bad.RuleID = "R999_OTHER"
	if err := (&VelocityRule{Window: time.Minute, MaxEvents: 1}).RestoreState(bad); err == nil {
		t.Fatal("foreign rule id memento must be rejected")
	}
	badVer := m
	badVer.Version = 42
	if err := (&VelocityRule{Window: time.Minute, MaxEvents: 1}).RestoreState(badVer); err == nil {
		t.Fatal("unknown snapshot version must be rejected")
	}
	// Registry enumerates the stateful rule even when wrapped by a toggle.
	reg := NewDefaultRegistry()
	reg.SetEnabled(IDVelocity, false)
	got := reg.StatefulRules()
	if len(got) != 1 || got[0].ID() != IDVelocity {
		t.Fatalf("disabled velocity rule must still be stateful, got %+v", got)
	}
	t.Log("scenario=velocity_snapshot_restore result=PASS rows_identical=true window_breach_preserved=true duplicates_absorbed=true foreign_and_bad_version_rejected=true disabled_toggle_visible=true")
}
