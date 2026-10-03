package backfill

import (
	"context"
	"testing"
	"time"

	"github.com/h3yow/fraud-alert-engine-go/internal/domain"
	"github.com/h3yow/fraud-alert-engine-go/internal/logging"
)

// TestBackfillEventIDCollisionAcrossChunks submits the same EventID several
// times with DIFFERENT OccurredAt values (and different payloads) across
// different chunks/runs, plus exact duplicates. Exactly one verdict must
// exist: the one derived from the earliest-business-time copy, replayed in
// business-time order. The same must hold after an empty-component restart
// resume.
func TestBackfillEventIDCollisionAcrossChunks(t *testing.T) {
	defer func() { markCase("TestBackfillEventIDCollisionAcrossChunks", !t.Failed()) }()
	base := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	dir := t.TempDir()

	// Four distinct benign events spread far beyond the 5-minute velocity
	// window so that rule cannot interfere with this case's intent.
	chunk1 := []domain.Transaction{
		mkTx("dup-A", "acct-c", "US", 10, base.Add(30*time.Minute)),
		mkTx("dup-B", "acct-c", "US", 10, base.Add(10*time.Minute)),
		mkTx("dup-C", "acct-c", "US", 10, base.Add(40*time.Minute)),
		mkTx("dup-D", "acct-c", "US", 10, base.Add(20*time.Minute)),
	}
	// Collisions: same EventIDs, later business times, suspicious payloads.
	mkLater := func(id string, at time.Time) domain.Transaction {
		tx := mkTx(id, "acct-c", "ZZ", 99999, at) // would BLOCK twice over
		return tx
	}
	chunk2 := []domain.Transaction{
		mkLater("dup-A", base.Add(200*time.Minute)),
		mkLater("dup-B", base.Add(210*time.Minute)),
		mkLater("dup-C", base.Add(220*time.Minute)),
		mkLater("dup-D", base.Add(230*time.Minute)),
		// exact copies of chunk1 entries too
		mkTx("dup-A", "acct-c", "US", 10, base.Add(30*time.Minute)),
		mkTx("dup-B", "acct-c", "US", 10, base.Add(10*time.Minute)),
	}
	want := chunk1 // canonical: earliest OccurredAt copies only

	lg := &logging.BufferLogger{}
	proc := newProcessor(lg)
	sess, err := New(proc, Options{Dir: dir, Logger: lg}).Begin("collision")
	if err != nil {
		t.Fatal(err)
	}
	// All chunks are already spilled to disk before the first run, as in the
	// real failure window (submit phase done; run interrupted half way).
	if err := sess.Submit(chunk1); err != nil {
		t.Fatal(err)
	}
	if err := sess.Submit(chunk2); err != nil {
		t.Fatalf("submit collisions: %v", err)
	}
	// Stop deterministically after the first two canonical events.
	sess.stopHook = func(applied int64) bool { return applied >= 2 }
	st1, err := sess.Run(context.Background())
	if err != nil {
		t.Fatalf("phase-1 run: %v", err)
	}
	if st1.Done || st1.Applied != 2 {
		t.Fatalf("phase 1 should stop after 2, got %+v", st1)
	}
	t.Logf("scenario=eventid_collision phase=1 applied=%d cursor=(%s,%s)",
		st1.Applied, st1.LastOccur.Format(time.RFC3339Nano), st1.LastEvent)

	// Restart into a brand-new empty component and run to completion.
	proc2 := newProcessor(&logging.BufferLogger{})
	sess2, err := New(proc2, Options{Dir: dir}).Begin("collision")
	if err != nil {
		t.Fatalf("restart: %v", err)
	}
	st2, err := sess2.Run(context.Background())
	if err != nil {
		t.Fatalf("phase-2 run: %v", err)
	}
	if !st2.Done || st2.Applied != int64(len(want)) {
		t.Fatalf("exactly %d unique identities must be applied, got %+v", len(want), st2)
	}
	t.Logf("scenario=eventid_collision phase=2 applied=%d skipped=%d (collisions_and_prefix)",
		st2.Applied, st2.Skipped)

	// Reference: earliest copies only, business-time order.
	refProc := newProcessor(&logging.BufferLogger{})
	ref := map[string]domain.Verdict{}
	for _, tx := range uniqueSorted(append(append([]domain.Transaction{}, chunk1...), chunk2...)) {
		ref[tx.EventID] = refProc.Process(tx)
	}
	assertVerdictsEquivalent(t, "eventid_collision", proc2, ref, want)

	// The later, suspicious copies must not have produced any ZZ / large
	// amount alert: every verdict is ALLOW and no alert exists.
	for _, tx := range want {
		v, ok := proc2.Verdict(tx.EventID)
		if !ok || v.Action != domain.ActionAllow || v.AlertKey != "" {
			t.Fatalf("event %s must keep the benign earliest verdict, got %+v", tx.EventID, v)
		}
	}
	if alerts := proc2.Alerts(); len(alerts) != 0 {
		t.Fatalf("collision copies must not create alerts, got %d: %+v", len(alerts), alerts)
	}
	// Re-delivering a collision copy after completion stays a duplicate of
	// the earliest verdict.
	again := proc2.Process(mkLater("dup-A", base.Add(500*time.Minute)))
	if !again.IsDuplicate || again.Action != domain.ActionAllow {
		t.Fatalf("collision re-delivery must replay the canonical verdict, got %+v", again)
	}
	t.Logf("scenario=eventid_collision result=PASS unique=%d collisions_suppressed=%d earliest_copy_wins=true restart_identical=true no_phantom_alerts=true",
		len(want), len(chunk2))
}
