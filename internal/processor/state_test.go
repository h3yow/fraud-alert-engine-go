package processor

import (
	"testing"
	"time"

	"github.com/h3yow/fraud-alert-engine-go/internal/alert"
	"github.com/h3yow/fraud-alert-engine-go/internal/domain"
	"github.com/h3yow/fraud-alert-engine-go/internal/engine"
	"github.com/h3yow/fraud-alert-engine-go/internal/logging"
	"github.com/h3yow/fraud-alert-engine-go/internal/rule"
)

func newTestProcessor(lg *logging.BufferLogger) *Processor {
	eng := engine.New(rule.NewDefaultRegistry(), lg)
	return New(eng, alert.New(lg), time.Minute, lg)
}

func ptx(id, acct, country string, amount float64, at time.Time) domain.Transaction {
	return domain.Transaction{
		EventID: id, TxID: "T" + id, AccountID: acct,
		Amount: amount, Currency: "USD", Country: country, EventType: "PAY",
		OccurredAt: at, IngestedAt: at.Add(time.Second),
	}
}

// TestProcessorSnapshotRestoreRoundTrip verifies that a full state snapshot
// taken after a mixed workload restores into a fresh processor well enough
// that later verdicts, alert counts and watermark behaviour are identical
// to an uninterrupted processor.
func TestProcessorSnapshotRestoreRoundTrip(t *testing.T) {
	base := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)

	build := func(p *Processor) {
		p.Process(ptx("a1", "acct-a", "ZZ", 100, base))
		p.Process(ptx("a2", "acct-a", "US", 10, base.Add(time.Minute)))
		p.Process(ptx("a3", "acct-b", "US", 50000, base.Add(2*time.Minute)))
		// Velocity burst: events 4..7 within the 5-minute window -> REVIEW
		// starting from event #4.
		for i := 0; i < 5; i++ {
			p.Process(ptx("v"+string(rune('0'+i)), "acct-v", "US", 5,
				base.Add(10*time.Minute+time.Duration(i)*30*time.Second)))
		}
		// Duplicate delivery before the snapshot.
		p.Process(ptx("a1", "acct-a", "ZZ", 100, base))
	}

	lg := &logging.BufferLogger{}
	src := newTestProcessor(lg)
	build(src)
	key := src.Alerts()[0].Key // any alert key; ack it to persist lifecycle
	if _, ok := src.AlertStore().Ack(key, time.Now()); !ok {
		t.Fatal("ack before snapshot")
	}
	st, err := src.SnapshotState()
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	// Fresh component: restore must reproduce every cached verdict, every
	// alert (count, extrema, state), the watermark and rule state.
	dst := newTestProcessor(&logging.BufferLogger{})
	if err := dst.RestoreState(st); err != nil {
		t.Fatalf("restore: %v", err)
	}
	srcAlerts, dstAlerts := map[string]domain.Alert{}, map[string]domain.Alert{}
	for _, a := range src.Alerts() {
		srcAlerts[a.Key] = a
	}
	for _, a := range dst.Alerts() {
		dstAlerts[a.Key] = a
	}
	if len(srcAlerts) != len(dstAlerts) {
		t.Fatalf("alert count after restore: %d vs %d", len(dstAlerts), len(srcAlerts))
	}
	for k, sa := range srcAlerts {
		da := dstAlerts[k]
		if da.HitCount != sa.HitCount || da.State != sa.State ||
			!da.FirstSeen.Equal(sa.FirstSeen) || !da.LastSeen.Equal(sa.LastSeen) {
			t.Fatalf("alert %s drift after restore: %+v vs %+v", k, da, sa)
		}
	}
	if a, _ := dst.AlertStore().Get(key); a.State != domain.StateAcked {
		t.Fatalf("ACKED state must survive snapshot/restore, got %s", a.State)
	}
	for _, id := range []string{"a1", "a2", "a3", "v0", "v1", "v2", "v3", "v4"} {
		gv, gok := dst.Verdict(id)
		sv, sok := src.Verdict(id)
		if gok != sok || gv.Action != sv.Action || gv.AlertKey != sv.AlertKey ||
			(gv.WinningHit != nil) != (sv.WinningHit != nil) ||
			(gv.WinningHit != nil && gv.WinningHit.RuleID != sv.WinningHit.RuleID) {
			t.Fatalf("verdict %s drift after restore: %+v vs %+v", id, gv, sv)
		}
	}

	// Re-delivery after restore replays the cached verdict as a duplicate.
	again := dst.Process(ptx("a1", "acct-a", "ZZ", 100, base))
	if !again.IsDuplicate || again.Action != domain.ActionBlock {
		t.Fatalf("restored processor must replay cached verdict, got %+v", again)
	}

	// Rule state restored: one more burst event immediately crosses the
	// velocity limit exactly as it would without the restart.
	extra := dst.Process(ptx("v9", "acct-v", "US", 5, base.Add(13*time.Minute)))
	if extra.Action != domain.ActionReview {
		t.Fatalf("velocity rule state must restore, got %+v", extra)
	}

	// Watermark restored: an old real-time event is late, a new one is not.
	if late := dst.Process(ptx("late", "acct-a", "US", 5, base.Add(-time.Hour))); !late.IsLate {
		t.Fatal("event behind restored watermark must be flagged late")
	}
	if fresh := dst.Process(ptx("fresh", "acct-a", "US", 5, base.Add(time.Hour))); fresh.IsLate {
		t.Fatal("current event must not be flagged late after restore")
	}
	t.Log("scenario=processor_snapshot_restore result=PASS verdicts=8 alerts_identical=true acked_survives=true velocity_restored=true watermark_restored=true")
}

// TestProcessorRestoreGuards checks that restore rejects non-empty targets
// and unknown versions, instead of silently overwriting live state.
func TestProcessorRestoreGuards(t *testing.T) {
	base := time.Now()
	lg := &logging.BufferLogger{}
	p := newTestProcessor(lg)
	p.Process(ptx("x1", "acct-a", "ZZ", 1, base))
	st, err := p.SnapshotState()
	if err != nil {
		t.Fatal(err)
	}
	if err := p.RestoreState(st); err == nil {
		t.Fatal("restoring into a non-empty processor must fail")
	}
	bad := st
	bad.Version = 999
	empty := newTestProcessor(&logging.BufferLogger{})
	if err := empty.RestoreState(bad); err == nil {
		t.Fatal("restoring an unknown version must fail")
	}
	if !empty.IsEmpty() {
		t.Fatal("failed restore must leave the processor empty")
	}
	t.Log("scenario=processor_restore_guards result=PASS non_empty_rejected=true bad_version_rejected=true")
}
