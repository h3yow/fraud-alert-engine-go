package backfill

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/h3yow/fraud-alert-engine-go/internal/domain"
	"github.com/h3yow/fraud-alert-engine-go/internal/logging"
)

// business cases logged for the restart-resume coverage matrix.
var restartBusinessCases = []string{
	"restart1_stop_after_one_third",
	"restart2_empty_component_state_restored",
	"restart3_stop_after_two_thirds",
	"restart4_empty_component_again_to_completion",
	"cursor_skips_not_recounted",
	"in_batch_duplicates_applied_once",
	"same_eventID_collisions_applied_once",
	"velocity_state_restored_burst_verdicts_match",
	"alert_counts_match_sequential",
	"alert_first_last_seen_match_sequential",
	"alert_state_machine_survives_restart",
	"per_event_verdicts_match_sequential",
	"watermark_connects_late_judgement_after_resume",
	"no_history_event_flagged_late",
}

// assertAlertsEquivalent fails the test on any drift in alert set, counts,
// business-time extrema, lifecycle state or reason lists.
func assertAlertsEquivalent(t *testing.T, tag string, got, ref map[string]domain.Alert) {
	t.Helper()
	if len(got) != len(ref) {
		t.Fatalf("[%s] alert count mismatch: restart=%d sequential=%d", tag, len(got), len(ref))
	}
	keys := make([]string, 0, len(ref))
	for k := range ref {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, key := range keys {
		g, ok := got[key]
		if !ok {
			t.Fatalf("[%s] alert %s missing after restart resume", tag, key)
		}
		r := ref[key]
		if g.HitCount != r.HitCount {
			t.Fatalf("[%s] alert %s count drift: %d vs sequential %d", tag, key, g.HitCount, r.HitCount)
		}
		if len(g.EventIDs) != len(r.EventIDs) {
			t.Fatalf("[%s] alert %s EventID-set size drift: %d vs %d", tag, key, len(g.EventIDs), len(r.EventIDs))
		}
		if !g.FirstSeen.Equal(r.FirstSeen) || !g.LastSeen.Equal(r.LastSeen) {
			t.Fatalf("[%s] alert %s first/last drift: [%s,%s] vs [%s,%s]",
				tag, key, g.FirstSeen, g.LastSeen, r.FirstSeen, r.LastSeen)
		}
		if g.State != r.State {
			t.Fatalf("[%s] alert %s lifecycle state rolled back: %s vs %s", tag, key, g.State, r.State)
		}
		if fmt.Sprint(g.Reasons) != fmt.Sprint(r.Reasons) {
			t.Fatalf("[%s] alert %s reasons drift: %v vs %v", tag, key, g.Reasons, r.Reasons)
		}
	}
}

// assertVerdictsEquivalent fails on any per-event drift in action, winning
// rule, full hit-rule set, late flag or alert attribution.
func assertVerdictsEquivalent(t *testing.T, tag string, p interface {
	Verdict(string) (domain.Verdict, bool)
}, ref map[string]domain.Verdict, events []domain.Transaction) {
	t.Helper()
	for _, tx := range events {
		g, ok := p.Verdict(tx.EventID)
		if !ok {
			t.Fatalf("[%s] event %s missing after restart resume", tag, tx.EventID)
		}
		r := ref[tx.EventID]
		if g.Action != r.Action || g.IsLate != r.IsLate || g.AlertKey != r.AlertKey ||
			len(g.Hits) != len(r.Hits) {
			t.Fatalf("[%s] event %s verdict drift:\nrestart=%+v\nsequential=%+v", tag, tx.EventID, g, r)
		}
		for i := range r.Hits {
			if g.Hits[i].RuleID != r.Hits[i].RuleID || g.Hits[i].Action != r.Hits[i].Action ||
				g.Hits[i].Failure != r.Hits[i].Failure {
				t.Fatalf("[%s] event %s hit #%d drift: %+v vs %+v", tag, tx.EventID, i, g.Hits[i], r.Hits[i])
			}
		}
		if (g.WinningHit == nil) != (r.WinningHit == nil) ||
			(g.WinningHit != nil && g.WinningHit.RuleID != r.WinningHit.RuleID) {
			t.Fatalf("[%s] event %s winning rule drift: %+v vs %+v", tag, tx.EventID, g.WinningHit, r.WinningHit)
		}
	}
}

// TestBackfillRestartEmptyComponentResume is the headline regression for
// "ran half way, process stopped, restarted from empty state, continue with
// the same batch": the component is rebuilt from scratch twice, and the
// final per-event verdicts, hit rules, alert attribution, counts,
// first/last times and lifecycle state must be identical to feeding the
// unique events through the normal path once, in business-time order.
func TestBackfillRestartEmptyComponentResume(t *testing.T) {
	defer func() { markCase("TestBackfillRestartEmptyComponentResume", !t.Failed()) }()
	for _, bc := range restartBusinessCases {
		t.Logf("scenario=restart_resume coverage_case=%s plan=included", bc)
	}
	base := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	batch := buildHistoricalBatch(base)
	want := uniqueSorted(batch)
	dir := t.TempDir()
	t.Logf("scenario=restart_resume batch=%d unique=%d chunks=11 max_open_runs=6 restarts=2",
		len(batch), len(want))

	const sessionID = "history-job"

	// Phase 1: fresh component, run to ~1/3 then stop.
	lg1 := &logging.BufferLogger{}
	proc1 := newProcessor(lg1)
	sess1, err := New(proc1, Options{Dir: dir, Logger: lg1, MaxOpenRuns: 6}).Begin(sessionID)
	if err != nil {
		t.Fatal(err)
	}
	const chunks = 11
	size := (len(batch) + chunks - 1) / chunks
	for i := 0; i < len(batch); i += size {
		end := i + size
		if end > len(batch) {
			end = len(batch)
		}
		if err := sess1.Submit(batch[i:end]); err != nil {
			t.Fatalf("submit: %v", err)
		}
	}
	sess1.stopHook = func(applied int64) bool { return applied >= int64(len(want)/3) }
	st1, err := sess1.Run(context.Background())
	if err != nil {
		t.Fatalf("phase-1 run: %v", err)
	}
	if st1.Done || st1.Applied == 0 || st1.Applied >= int64(len(want)) {
		t.Fatalf("phase 1 should stop mid-way, got %+v", st1)
	}
	// Operator workflow actions on alerts produced before the crash must
	// survive the restart: ack one alert, resolve another. Record the same
	// intent to replay on the sequential reference.
	keys1 := alertKeys(proc1)
	if len(keys1) < 2 {
		t.Fatalf("test setup: expected >=2 alerts after phase 1, got %d", len(keys1))
	}
	ackKey, resolveKey := keys1[0], keys1[1]
	if _, ok := proc1.AlertStore().Ack(ackKey, time.Now()); !ok {
		t.Fatalf("ack %s before restart", ackKey)
	}
	if _, ok := proc1.AlertStore().Resolve(resolveKey, time.Now()); !ok {
		t.Fatalf("resolve %s before restart", resolveKey)
	}
	// Persist the workflow actions on disk so the restarted component sees
	// them too: force a checkpoint snapshot at the same cursor.
	sess1.mu.Lock()
	if err := sess1.saveCheckpointLocked(); err != nil {
		sess1.mu.Unlock()
		t.Fatalf("snapshot after ack: %v", err)
	}
	sess1.mu.Unlock()
	t.Logf("scenario=restart_resume phase=1 result=stopped applied=%d cursor=(%s,%s) ack=%s resolve=%s",
		st1.Applied, st1.LastOccur.Format(time.RFC3339Nano), st1.LastEvent, ackKey, resolveKey)

	// Phase 2: RESTART — brand-new empty component, same session dir.
	lg2 := &logging.BufferLogger{}
	proc2 := newProcessor(lg2)
	if !proc2.IsEmpty() {
		t.Fatal("freshly built processor must be empty")
	}
	sess2, err := New(proc2, Options{Dir: dir, Logger: lg2, MaxOpenRuns: 6}).Begin(sessionID)
	if err != nil {
		t.Fatalf("restart begin: %v", err)
	}
	if got := sess2.Progress().Applied; got != st1.Applied {
		t.Fatalf("restart lost applied progress: %d vs %d", got, st1.Applied)
	}
	if a, ok := proc2.AlertStore().Get(ackKey); !ok || a.State != domain.StateAcked {
		t.Fatalf("acked alert must be restored as ACKED, got ok=%v state=%s", ok, a.State)
	}
	if a, ok := proc2.AlertStore().Get(resolveKey); !ok || a.State != domain.StateResolved {
		t.Fatalf("resolved alert must be restored as RESOLVED, got ok=%v state=%s", ok, a.State)
	}
	sess2.stopHook = func(applied int64) bool { return applied >= int64(2*len(want)/3) }
	st2, err := sess2.Run(context.Background())
	if err != nil {
		t.Fatalf("phase-2 run: %v", err)
	}
	if st2.Done || st2.Applied <= st1.Applied || st2.Applied >= int64(len(want)) {
		t.Fatalf("phase 2 should stop further along, got %+v (phase1=%d)", st2, st1.Applied)
	}
	t.Logf("scenario=restart_resume phase=2 result=restarted_and_stopped applied=%d skipped=%d cursor=(%s,%s)",
		st2.Applied, st2.Skipped, st2.LastOccur.Format(time.RFC3339Nano), st2.LastEvent)

	// Phase 3: RESTART AGAIN — another empty component, run to completion.
	lg3 := &logging.BufferLogger{}
	proc3 := newProcessor(lg3)
	sess3, err := New(proc3, Options{Dir: dir, Logger: lg3, MaxOpenRuns: 6}).Begin(sessionID)
	if err != nil {
		t.Fatalf("second restart begin: %v", err)
	}
	st3, err := sess3.Run(context.Background())
	if err != nil {
		t.Fatalf("phase-3 run: %v", err)
	}
	if !st3.Done {
		t.Fatalf("phase 3 must complete, got %+v", st3)
	}
	if st3.Applied != int64(len(want)) {
		t.Fatalf("applied count must equal unique events: got %d want %d", st3.Applied, len(want))
	}
	if st3.Skipped == 0 {
		t.Fatalf("restarted run must skip the checkpointed prefix, got skipped=0")
	}
	t.Logf("scenario=restart_resume phase=3 result=completed_after_restart applied=%d prefix_skipped=%d",
		st3.Applied, st3.Skipped)

	// Sequential reference: unique events by business time through the
	// normal path, plus the same workflow actions at the end.
	refProc := newProcessor(&logging.BufferLogger{})
	refVerdicts := make(map[string]domain.Verdict, len(want))
	for _, tx := range want {
		refVerdicts[tx.EventID] = refProc.Process(tx)
	}
	refProc.AlertStore().Ack(ackKey, time.Now())
	refProc.AlertStore().Resolve(resolveKey, time.Now())

	// Full equivalence: every verdict ...
	assertVerdictsEquivalent(t, "restart_resume", proc3, refVerdicts, want)
	// ... and every alert.
	assertAlertsEquivalent(t, "restart_resume", alertMap(proc3), alertMap(refProc))
	t.Logf("scenario=restart_resume comparison verdict_identical=true alerts=%d counts_identical=true lifecycle_identical=true",
		len(alertMap(proc3)))

	// State only moves forward: no historical event was flagged late ...
	for _, tx := range want {
		if v, _ := proc3.Verdict(tx.EventID); v.IsLate {
			t.Fatalf("historical event %s must never be flagged late", tx.EventID)
		}
	}
	// ... and real-time late/out-of-order judgement now connects to the
	// backfilled watermark: an old straggler is late, a fresh one is not.
	straggler := proc3.Process(mkTx("rt-straggler", "acct-norm", "US", 5, base.Add(-48*time.Hour)))
	if !straggler.IsLate {
		t.Fatal("real-time straggler behind backfilled watermark must be flagged late")
	}
	fresh := proc3.Process(mkTx("rt-fresh", "acct-norm", "US", 5, base.Add(200*24*time.Hour)))
	if fresh.IsLate {
		t.Fatal("fresh real-time event must not be flagged late")
	}

	// A further re-run is a no-op (nothing re-counted after completion).
	st4, err := sess3.Run(context.Background())
	if err != nil {
		t.Fatalf("phase-4 rerun: %v", err)
	}
	if st4.Applied != st3.Applied || !st4.Done {
		t.Fatalf("rerun after completion must be a no-op: %+v vs %+v", st4, st3)
	}
	t.Logf("scenario=restart_resume result=PASS applied=%d skipped=%d restarts=2 verdict_identical=true hit_rules_identical=true alert_attribution_identical=true counts_identical=true state_forward_only=true watermark_connected=true",
		st3.Applied, st3.Skipped)
}

// TestBackfillRestartRejectsForeignNonEmptyProcessor guards the
// same-process resume path: attaching a session with applied progress to a
// processor that does NOT contain the checkpointed state must fail loudly
// instead of silently forking two histories.
func TestBackfillRestartRejectsForeignNonEmptyProcessor(t *testing.T) {
	defer func() { markCase("TestBackfillRestartRejectsForeignNonEmptyProcessor", !t.Failed()) }()
	dir := t.TempDir()
	base := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	batch := buildHistoricalBatch(base)
	want := uniqueSorted(batch)

	lg := &logging.BufferLogger{}
	proc := newProcessor(lg)
	sess, err := New(proc, Options{Dir: dir, Logger: lg}).Begin("job")
	if err != nil {
		t.Fatal(err)
	}
	if err := sess.Submit(batch); err != nil {
		t.Fatal(err)
	}
	sess.stopHook = func(applied int64) bool { return applied >= int64(len(want)/4) }
	if _, err := sess.Run(context.Background()); err != nil {
		t.Fatalf("phase-1 run: %v", err)
	}

	// A different, non-empty processor (unrelated real-time traffic) must
	// be rejected rather than silently skipping the prefix.
	foreign := newProcessor(&logging.BufferLogger{})
	foreign.Process(mkTx("unrelated-rt", "acct-x", "ZZ", 1, base.Add(999*24*time.Hour)))
	if _, err := New(foreign, Options{Dir: dir}).Begin("job"); err == nil {
		t.Fatal("resuming an applied session on a foreign non-empty processor must fail")
	} else {
		t.Logf("scenario=foreign_processor_guard result=PASS err=%q", err)
	}

	// The genuine empty-processor restart still works and finishes.
	proc2 := newProcessor(&logging.BufferLogger{})
	sess2, err := New(proc2, Options{Dir: dir}).Begin("job")
	if err != nil {
		t.Fatalf("empty-processor restart: %v", err)
	}
	st, err := sess2.Run(context.Background())
	if err != nil {
		t.Fatalf("restart run: %v", err)
	}
	if !st.Done || st.Applied != int64(len(want)) {
		t.Fatalf("restart must complete all unique events, got %+v want=%d", st, len(want))
	}
	t.Logf("scenario=foreign_processor_guard restart_completed applied=%d result=PASS", st.Applied)
}

// alertKeys returns the alert keys in stable sorted order.
func alertKeys(p interface {
	Alerts() []domain.Alert
}) []string {
	var keys []string
	for _, a := range p.Alerts() {
		keys = append(keys, a.Key)
	}
	sort.Strings(keys)
	return keys
}
