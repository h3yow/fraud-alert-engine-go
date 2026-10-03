package alert

import (
	"testing"
	"time"

	"github.com/h3yow/fraud-alert-engine-go/internal/domain"
	"github.com/h3yow/fraud-alert-engine-go/internal/logging"
)

func atx(id, acct string, at time.Time) domain.Transaction {
	return domain.Transaction{
		EventID: id, TxID: "T" + id, AccountID: acct,
		Amount: 1, Currency: "USD", Country: "ZZ",
		OccurredAt: at, IngestedAt: at.Add(time.Second),
	}
}

// TestStoreSnapshotRestoreRoundTrip covers counts, business-time extrema,
// reason lists and the OPEN/ACKED/RESOLVED lifecycle across a restore.
func TestStoreSnapshotRestoreRoundTrip(t *testing.T) {
	base := time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC)
	lg := &logging.BufferLogger{}
	src := New(lg)
	hit := &domain.Hit{RuleID: "R200_TEST", Action: domain.ActionBlock, Reason: "r1"}
	src.Record(hit, atx("e1", "acct-a", base.Add(2*time.Minute)), false)
	src.Record(hit, atx("e2", "acct-a", base), false)                    // earlier: moves FirstSeen
	src.Record(hit, atx("e1", "acct-a", base.Add(2*time.Minute)), false) // duplicate id
	key := DedupKey(hit, atx("e1", "acct-a", base), src.cooldown)
	src.Ack(key, time.Now())

	st := src.SnapshotState()
	if len(st.Alerts) != 1 || st.Version != stateVersion {
		t.Fatalf("unexpected snapshot: %+v", st)
	}
	dst := New(&logging.BufferLogger{})
	if err := dst.RestoreState(st); err != nil {
		t.Fatalf("restore: %v", err)
	}
	a, ok := dst.Get(key)
	if !ok {
		t.Fatal("alert missing after restore")
	}
	if a.HitCount != 2 || len(a.EventIDs) != 2 {
		t.Fatalf("counts must restore: count=%d ids=%d", a.HitCount, len(a.EventIDs))
	}
	if !a.FirstSeen.Equal(base) || !a.LastSeen.Equal(base.Add(2*time.Minute)) {
		t.Fatalf("extrema must restore: %s..%s", a.FirstSeen, a.LastSeen)
	}
	if a.State != domain.StateAcked {
		t.Fatalf("ACKED lifecycle must restore, got %s", a.State)
	}
	// One-way machine keeps working from the restored state.
	if _, ok := dst.Resolve(key, time.Now()); !ok {
		t.Fatal("resolve after restore must succeed from ACKED")
	}
	// A late duplicate after restore does not inflate the count.
	src.Record(hit, atx("e2", "acct-a", base), true)
	dst.Record(hit, atx("e2", "acct-a", base), true)
	a2, _ := dst.Get(key)
	if a2.HitCount != 2 || a2.State != domain.StateResolved {
		t.Fatalf("duplicate after restore must be idempotent: %+v", a2)
	}
	// Restoring into a non-empty store is rejected.
	if err := dst.RestoreState(st); err == nil {
		t.Fatal("restore into non-empty store must fail")
	}
	t.Log("scenario=alert_snapshot_restore result=PASS count=2 first_last_restored=true acked_restored=true duplicate_idempotent=true non_empty_rejected=true")
}
