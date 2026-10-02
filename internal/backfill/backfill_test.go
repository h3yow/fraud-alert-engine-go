package backfill

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/h3yow/fraud-alert-engine-go/internal/alert"
	"github.com/h3yow/fraud-alert-engine-go/internal/domain"
	"github.com/h3yow/fraud-alert-engine-go/internal/engine"
	"github.com/h3yow/fraud-alert-engine-go/internal/logging"
	"github.com/h3yow/fraud-alert-engine-go/internal/processor"
	"github.com/h3yow/fraud-alert-engine-go/internal/rule"
)

func newProcessor(lg *logging.BufferLogger) *processor.Processor {
	eng := engine.New(rule.NewDefaultRegistry(), lg)
	return processor.New(eng, alert.New(lg), time.Minute, lg)
}

func mkTx(event, acct, country string, amount float64, at time.Time) domain.Transaction {
	return domain.Transaction{
		EventID: event, TxID: "T" + event, AccountID: acct,
		Amount: amount, Currency: "USD", Country: country, EventType: "PAY",
		OccurredAt: at, IngestedAt: at.Add(time.Second),
	}
}

// buildHistoricalBatch fabricates a wide-span, unordered, duplicate-laden
// historical batch: normal events, high-risk-country blocks, large-amount
// blocks and velocity-breaching bursts.
func buildHistoricalBatch(base time.Time) []domain.Transaction {
	var batch []domain.Transaction
	// Spread ordinary traffic across 30 days.
	for i := 0; i < 120; i++ {
		batch = append(batch, mkTx(fmt.Sprintf("norm-%03d", i), "acct-norm",
			"US", 10+float64(i%50), base.Add(time.Duration(i)*6*time.Hour)))
	}
	// High-risk country and large-amount blocks interleaved in time.
	for i := 0; i < 20; i++ {
		batch = append(batch, mkTx(fmt.Sprintf("risk-%02d", i), "acct-risk",
			"ZZ", 100, base.Add(time.Duration(i)*36*time.Hour)))
		batch = append(batch, mkTx(fmt.Sprintf("big-%02d", i), "acct-big",
			"US", 20000+float64(i), base.Add(time.Duration(i)*36*time.Hour+time.Hour)))
	}
	// Velocity bursts: 5 events inside 5 minutes for the same account,
	// at several points in history (limit is 3 per 5-minute window).
	for burst := 0; burst < 4; burst++ {
		start := base.Add(time.Duration(burst) * 7 * 24 * time.Hour)
		for i := 0; i < 5; i++ {
			batch = append(batch, mkTx(fmt.Sprintf("vel-%d-%d", burst, i), "acct-vel",
				"US", 5, start.Add(time.Duration(i)*time.Minute)))
		}
	}
	// In-batch duplicates of a sample of events (exact copies).
	rng := rand.New(rand.NewSource(42))
	for i := 0; i < 30; i++ {
		batch = append(batch, batch[rng.Intn(len(batch))])
	}
	// Shuffle the whole batch: arrival order carries no information.
	rng.Shuffle(len(batch), func(i, j int) { batch[i], batch[j] = batch[j], batch[i] })
	return batch
}

// uniqueSorted returns each EventID once, ordered by (OccurredAt, EventID).
func uniqueSorted(batch []domain.Transaction) []domain.Transaction {
	seen := map[string]bool{}
	var out []domain.Transaction
	for _, tx := range batch {
		if seen[tx.EventID] {
			continue
		}
		seen[tx.EventID] = true
		out = append(out, tx)
	}
	sort.SliceStable(out, func(i, j int) bool {
		if !out[i].OccurredAt.Equal(out[j].OccurredAt) {
			return out[i].OccurredAt.Before(out[j].OccurredAt)
		}
		return out[i].EventID < out[j].EventID
	})
	return out
}

func alertMap(p *processor.Processor) map[string]domain.Alert {
	out := map[string]domain.Alert{}
	for _, a := range p.Alerts() {
		out[a.Key] = a
	}
	return out
}

// TestBackfillMatchesSequentialByBusinessTime is the core equivalence test:
// verdicts, winning rules, alert attribution and counts after a chunked,
// compacted backfill must equal feeding the unique events sorted by
// business time through the normal real-time path one by one.
func TestBackfillMatchesSequentialByBusinessTime(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	batch := buildHistoricalBatch(base)
	want := uniqueSorted(batch)
	t.Logf("scenario=equivalence batch=%d unique=%d chunks=23 max_open_runs=4",
		len(batch), len(want))

	// Reference: normal real-time processing in business-time order.
	refProc := newProcessor(&logging.BufferLogger{})
	refVerdicts := map[string]domain.Verdict{}
	for _, tx := range want {
		refVerdicts[tx.EventID] = refProc.Process(tx)
	}

	// Backfill: same data, shuffled, duplicated, in 23 chunks, with run
	// compaction forced by a tiny MaxOpenRuns.
	lg := &logging.BufferLogger{}
	proc := newProcessor(lg)
	bf := New(proc, Options{Dir: t.TempDir(), Logger: lg, MaxOpenRuns: 4})
	sess, err := bf.Begin("equiv")
	if err != nil {
		t.Fatal(err)
	}
	const chunks = 23
	for i := 0; i < len(batch); i += (len(batch) + chunks - 1) / chunks {
		end := i + (len(batch)+chunks-1)/chunks
		if end > len(batch) {
			end = len(batch)
		}
		if err := sess.Submit(batch[i:end]); err != nil {
			t.Fatalf("submit chunk: %v", err)
		}
	}
	st, err := sess.Run(context.Background())
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if !st.Done || st.Applied != int64(len(want)) {
		t.Fatalf("expected done with %d applied, got %+v", len(want), st)
	}
	t.Logf("scenario=equivalence backfill_stats=%+v", st)

	// Per-event verdict equivalence.
	for _, tx := range want {
		got, ok := proc.Verdict(tx.EventID)
		if !ok {
			t.Fatalf("event %s missing after backfill", tx.EventID)
		}
		ref := refVerdicts[tx.EventID]
		if got.Action != ref.Action || got.IsLate != ref.IsLate ||
			(got.WinningHit == nil) != (ref.WinningHit == nil) ||
			(got.WinningHit != nil && got.WinningHit.RuleID != ref.WinningHit.RuleID) ||
			got.AlertKey != ref.AlertKey || len(got.Hits) != len(ref.Hits) {
			t.Fatalf("event %s verdict mismatch:\nbackfill=%+v\nsequential=%+v",
				tx.EventID, got, ref)
		}
	}

	// Alert-level equivalence: keys, counts, first/last seen, state, reasons.
	gotAlerts, refAlerts := alertMap(proc), alertMap(refProc)
	if len(gotAlerts) != len(refAlerts) {
		t.Fatalf("alert count mismatch: backfill=%d sequential=%d", len(gotAlerts), len(refAlerts))
	}
	for key, ref := range refAlerts {
		got, ok := gotAlerts[key]
		if !ok {
			t.Fatalf("alert %s missing after backfill", key)
		}
		if got.HitCount != ref.HitCount || !got.FirstSeen.Equal(ref.FirstSeen) ||
			!got.LastSeen.Equal(ref.LastSeen) || got.State != ref.State ||
			fmt.Sprint(got.Reasons) != fmt.Sprint(ref.Reasons) {
			t.Fatalf("alert %s mismatch:\nbackfill=%+v\nsequential=%+v", key, got, ref)
		}
	}
	t.Logf("scenario=equivalence result=PASS alerts=%d verdicts=%d identical_to_sequential=true",
		len(gotAlerts), len(want))
}

// TestBackfillStopAndResumeCountConservation stops a run mid-way via
// context cancellation, then "restarts" (a brand-new Backfiller and Session
// over the same session directory) and resumes. Already-applied events must
// not be counted twice and nothing may be skipped.
func TestBackfillStopAndResumeCountConservation(t *testing.T) {
	base := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	batch := buildHistoricalBatch(base)
	want := uniqueSorted(batch)
	dir := t.TempDir()
	t.Logf("scenario=stop_resume unique=%d plan=stop_midway_then_restart_and_resume", len(want))

	lg := &logging.BufferLogger{}
	proc := newProcessor(lg)

	// Phase 1: submit everything, then run with a context that is cancelled
	// after a fraction of the events has been applied.
	sess, err := New(proc, Options{Dir: dir, Logger: lg, MaxOpenRuns: 8}).Begin("job")
	if err != nil {
		t.Fatal(err)
	}
	const chunks = 7
	for i := 0; i < len(batch); i += (len(batch) + chunks - 1) / chunks {
		end := i + (len(batch)+chunks-1)/chunks
		if end > len(batch) {
			end = len(batch)
		}
		if err := sess.Submit(batch[i:end]); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	stopAfter := len(want) / 3
	// Cancel from a watcher goroutine once enough events were applied.
	go func() {
		for {
			if sess.Progress().Applied >= int64(stopAfter) {
				cancel()
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()
	st1, err := sess.Run(ctx)
	if err != nil {
		t.Fatalf("phase-1 run: %v", err)
	}
	if st1.Done {
		t.Fatalf("phase 1 should have stopped early, got %+v", st1)
	}
	if st1.Applied == 0 || st1.Applied >= int64(len(want)) {
		t.Fatalf("phase 1 applied count out of expected range: %+v", st1)
	}
	hitsAfterPhase1 := 0
	for _, a := range proc.Alerts() {
		hitsAfterPhase1 += a.HitCount
	}
	t.Logf("scenario=stop_resume phase1=stopped applied=%d alert_hits=%d cursor=(%s,%s)",
		st1.Applied, hitsAfterPhase1, st1.LastOccur.Format(time.RFC3339Nano), st1.LastEvent)

	// Phase 2: simulate a restart — new Backfiller, new Session object, same
	// session directory and same (still running) processor.
	sess2, err := New(proc, Options{Dir: dir, Logger: lg, MaxOpenRuns: 8}).Begin("job")
	if err != nil {
		t.Fatal(err)
	}
	if got := sess2.Progress().Applied; got != st1.Applied {
		t.Fatalf("resumed session lost progress: want applied=%d got %d", st1.Applied, got)
	}
	st2, err := sess2.Run(context.Background())
	if err != nil {
		t.Fatalf("phase-2 run: %v", err)
	}
	if !st2.Done {
		t.Fatalf("phase 2 must complete, got %+v", st2)
	}
	// Count conservation: total applied across both phases equals the number
	// of unique events — nothing re-applied, nothing missed.
	if st2.Applied != int64(len(want)) {
		t.Fatalf("count not conserved: applied=%d unique=%d", st2.Applied, len(want))
	}
	t.Logf("scenario=stop_resume phase2=resumed applied_total=%d skipped_already_applied=%d",
		st2.Applied, st2.Skipped)

	// Final state must equal the uninterrupted sequential reference.
	refProc := newProcessor(&logging.BufferLogger{})
	for _, tx := range want {
		refProc.Process(tx)
	}
	gotAlerts, refAlerts := alertMap(proc), alertMap(refProc)
	if len(gotAlerts) != len(refAlerts) {
		t.Fatalf("alert count mismatch after resume: got %d want %d", len(gotAlerts), len(refAlerts))
	}
	for key, ref := range refAlerts {
		got := gotAlerts[key]
		if got.HitCount != ref.HitCount || !got.FirstSeen.Equal(ref.FirstSeen) ||
			!got.LastSeen.Equal(ref.LastSeen) {
			t.Fatalf("alert %s drifted across stop/resume: got %+v want %+v", key, got, ref)
		}
	}
	// And every verdict must match the reference too.
	for _, tx := range want {
		got, ok := proc.Verdict(tx.EventID)
		if !ok {
			t.Fatalf("event %s missing after resume", tx.EventID)
		}
		ref, _ := refProc.Verdict(tx.EventID)
		if got.Action != ref.Action || got.AlertKey != ref.AlertKey {
			t.Fatalf("event %s verdict drifted across stop/resume: got %s/%s want %s/%s",
				tx.EventID, got.Action, got.AlertKey, ref.Action, ref.AlertKey)
		}
	}
	t.Logf("scenario=stop_resume result=PASS applied=%d alerts=%d count_conserved=true no_recount=true no_miss=true",
		st2.Applied, len(gotAlerts))
}

// TestBackfillConcurrentWithRealtime runs a historical backfill while
// real-time traffic (including redeliveries of events that are also in the
// backfill batch) keeps arriving. It asserts: one stable verdict per
// EventID, conserved alert counts, no late-flagging of backfilled history,
// no regression of pre-existing alerts, and smooth watermark connection
// afterwards.
func TestBackfillConcurrentWithRealtime(t *testing.T) {
	lg := &logging.BufferLogger{}
	proc := newProcessor(lg)
	now := time.Date(2026, 3, 10, 12, 0, 0, 0, time.UTC)

	// Pre-existing real-time alerts before the backfill starts.
	pre1 := proc.Process(mkTx("rt-pre-1", "acct-live", "ZZ", 100, now.Add(-time.Minute)))
	pre2 := proc.Process(mkTx("rt-pre-2", "acct-live2", "US", 50000, now.Add(-30*time.Second)))
	preAlerts := alertMap(proc)
	t.Logf("scenario=concurrent preexisting_alerts=%d pre1=%s pre2=%s",
		len(preAlerts), pre1.AlertKey, pre2.AlertKey)

	// Historical batch: 3 months of old traffic, plus events whose EventIDs
	// will ALSO be delivered through the real-time path below (overlap).
	var hist []domain.Transaction
	for i := 0; i < 150; i++ {
		country, amount := "US", 20.0
		if i%9 == 0 {
			country = "ZZ" // historical blocks
		}
		if i%11 == 0 {
			amount = 30000 // historical large amounts
		}
		hist = append(hist, mkTx(fmt.Sprintf("hist-%03d", i), fmt.Sprintf("acct-h%d", i%7),
			country, amount, now.Add(-90*24*time.Hour).Add(time.Duration(i)*12*time.Hour)))
	}
	overlap := []domain.Transaction{
		mkTx("shared-1", "acct-live", "ZZ", 100, now.Add(-2*time.Hour)),
		mkTx("shared-2", "acct-live2", "US", 60000, now.Add(-time.Hour)),
	}
	hist = append(hist, overlap...)
	rng := rand.New(rand.NewSource(7))
	rng.Shuffle(len(hist), func(i, j int) { hist[i], hist[j] = hist[j], hist[i] })

	sess, err := New(proc, Options{Dir: t.TempDir(), Logger: lg, MaxOpenRuns: 4}).Begin("conc")
	if err != nil {
		t.Fatal(err)
	}
	if err := sess.Submit(hist); err != nil {
		t.Fatal(err)
	}

	// Run the backfill in the background while real-time events arrive.
	done := make(chan Stats, 1)
	go func() {
		st, err := sess.Run(context.Background())
		if err != nil {
			t.Errorf("backfill run: %v", err)
		}
		done <- st
	}()

	// Real-time stream: fresh events plus redeliveries of the overlap set.
	rtVerdicts := map[string]domain.Verdict{}
	for i := 0; i < 40; i++ {
		tx := mkTx(fmt.Sprintf("rt-%02d", i), "acct-live", "US", 15,
			now.Add(time.Duration(i)*time.Second))
		if i%10 == 0 {
			tx.Country = "ZZ"
		}
		rtVerdicts[tx.EventID] = proc.Process(tx)
	}
	for _, tx := range overlap {
		rtVerdicts[tx.EventID] = proc.Process(tx)
	}
	st := <-done
	if !st.Done {
		t.Fatalf("backfill must complete, got %+v", st)
	}
	t.Logf("scenario=concurrent backfill_stats=%+v realtime_events=%d", st, len(rtVerdicts))

	// 1) One stable verdict per EventID: whatever the real-time caller got
	//    is what is cached, and re-delivery replays it.
	for id, v := range rtVerdicts {
		cached, ok := proc.Verdict(id)
		if !ok {
			t.Fatalf("real-time event %s has no cached verdict", id)
		}
		if cached.Action != v.Action || cached.AlertKey != v.AlertKey {
			t.Fatalf("event %s has two conclusions: realtime=%s/%s cached=%s/%s",
				id, v.Action, v.AlertKey, cached.Action, cached.AlertKey)
		}
	}
	for _, tx := range overlap {
		again := proc.Process(tx)
		cached, _ := proc.Verdict(tx.EventID)
		if !again.IsDuplicate || again.Action != cached.Action || again.AlertKey != cached.AlertKey {
			t.Fatalf("overlap event %s verdict unstable across paths: %+v vs %+v",
				tx.EventID, again, cached)
		}
	}

	// 2) Backfilled history is never flagged late; fresh real-time events
	//    are not flagged late either. Overlap events are excluded here:
	//    their single verdict belongs to whichever path won the race, and a
	//    real-time-first verdict may legitimately carry the late flag.
	isOverlap := map[string]bool{}
	for _, tx := range overlap {
		isOverlap[tx.EventID] = true
	}
	for _, tx := range hist {
		v, ok := proc.Verdict(tx.EventID)
		if !ok {
			t.Fatalf("historical event %s missing", tx.EventID)
		}
		if v.IsLate && !isOverlap[tx.EventID] {
			t.Fatalf("backfilled event %s must not be flagged late", tx.EventID)
		}
	}

	// 3) Count conservation: every alert's HitCount equals its unique
	//    EventID set, and the totals equal the number of unique events whose
	//    verdict is non-ALLOW.
	nonAllow := map[string]bool{}
	for _, v := range []domain.Verdict{pre1, pre2} {
		if v.Action != domain.ActionAllow {
			nonAllow[v.EventID] = true
		}
	}
	for _, tx := range append(append([]domain.Transaction{}, hist...), overlap...) {
		if v, _ := proc.Verdict(tx.EventID); v.Action != domain.ActionAllow {
			nonAllow[tx.EventID] = true
		}
	}
	for id, v := range rtVerdicts {
		if v.Action != domain.ActionAllow {
			nonAllow[id] = true
		}
	}
	totalHits, totalIDs := 0, 0
	for _, a := range proc.Alerts() {
		if a.HitCount != len(a.EventIDs) {
			t.Fatalf("alert %s HitCount=%d but %d unique EventIDs", a.Key, a.HitCount, len(a.EventIDs))
		}
		totalHits += a.HitCount
		totalIDs += len(a.EventIDs)
	}
	if totalHits != len(nonAllow) || totalIDs != len(nonAllow) {
		t.Fatalf("alert counts not conserved: hits=%d ids=%d unique_non_allow=%d",
			totalHits, totalIDs, len(nonAllow))
	}

	// 4) No regression of pre-existing alerts: counts never shrank, state
	//    never moved backwards, first/last seen stayed within min/max
	//    semantics (FirstSeen may only move earlier, LastSeen only later).
	for key, before := range preAlerts {
		after, ok := alertMap(proc)[key]
		if !ok {
			t.Fatalf("pre-existing alert %s vanished", key)
		}
		if after.HitCount < before.HitCount {
			t.Fatalf("alert %s count regressed: %d -> %d", key, before.HitCount, after.HitCount)
		}
		if after.State != before.State {
			t.Fatalf("alert %s state changed by backfill: %s -> %s", key, before.State, after.State)
		}
		if after.FirstSeen.After(before.FirstSeen) || after.LastSeen.Before(before.LastSeen) {
			t.Fatalf("alert %s time range regressed: %+v -> %+v", key, before, after)
		}
	}

	// 5) Smooth connection: after the backfill, watermark-based late
	//    judgement works off the new business time — a genuinely old
	//    real-time straggler is late, a current one is not.
	late := proc.Process(mkTx("rt-straggler", "acct-live", "US", 5, now.Add(-48*time.Hour)))
	if !late.IsLate {
		t.Fatal("real-time straggler far behind the watermark must be flagged late")
	}
	fresh := proc.Process(mkTx("rt-fresh", "acct-live", "US", 5, now.Add(time.Minute)))
	if fresh.IsLate {
		t.Fatal("current real-time event must not be flagged late after backfill")
	}
	t.Logf("scenario=concurrent result=PASS alerts=%d unique_non_allow=%d stable_verdicts=true counts_conserved=true no_regression=true",
		len(proc.Alerts()), len(nonAllow))
}

// TestBackfillChunkedStreamingAndGuards pushes a batch far larger than one
// chunk through many tiny submits with aggressive run compaction, verifies
// the result still equals the sequential reference, and checks the session
// guards: submitting older-than-cursor data after progress is rejected, and
// re-running a finished session is a no-op.
func TestBackfillChunkedStreamingAndGuards(t *testing.T) {
	base := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)
	batch := buildHistoricalBatch(base)
	want := uniqueSorted(batch)
	t.Logf("scenario=chunked_streaming unique=%d chunk_size=7 max_open_runs=3", len(want))

	lg := &logging.BufferLogger{}
	proc := newProcessor(lg)
	sess, err := New(proc, Options{Dir: t.TempDir(), Logger: lg, MaxOpenRuns: 3}).Begin("stream")
	if err != nil {
		t.Fatal(err)
	}
	const chunkSize = 7
	nChunks := 0
	for i := 0; i < len(batch); i += chunkSize {
		end := i + chunkSize
		if end > len(batch) {
			end = len(batch)
		}
		if err := sess.Submit(batch[i:end]); err != nil {
			t.Fatalf("submit chunk %d: %v", nChunks, err)
		}
		nChunks++
	}
	t.Logf("scenario=chunked_streaming submitted_chunks=%d", nChunks)

	st, err := sess.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !st.Done || st.Applied != int64(len(want)) {
		t.Fatalf("want done/%d applied, got %+v", len(want), st)
	}

	// Equivalence with the sequential reference still holds under heavy
	// chunking and compaction.
	refProc := newProcessor(&logging.BufferLogger{})
	for _, tx := range want {
		refProc.Process(tx)
	}
	gotAlerts, refAlerts := alertMap(proc), alertMap(refProc)
	if len(gotAlerts) != len(refAlerts) {
		t.Fatalf("alert count mismatch: got %d want %d", len(gotAlerts), len(refAlerts))
	}
	for key, ref := range refAlerts {
		got := gotAlerts[key]
		if got.HitCount != ref.HitCount || !got.FirstSeen.Equal(ref.FirstSeen) ||
			!got.LastSeen.Equal(ref.LastSeen) {
			t.Fatalf("alert %s mismatch under chunked streaming: got %+v want %+v", key, got, ref)
		}
	}

	// Guard 1: after progress, a chunk reaching back before the applied
	// cursor must be rejected loudly, not silently dropped.
	err = sess.Submit([]domain.Transaction{
		mkTx("too-old", "acct-norm", "US", 5, base.Add(-time.Hour)),
	})
	if err == nil {
		t.Fatal("submitting data older than the applied cursor must fail")
	}
	t.Logf("scenario=chunked_streaming guard_old_chunk_rejected err=%q", err)

	// Guard 2: re-running a finished session applies nothing new.
	st2, err := sess.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if st2.Applied != st.Applied {
		t.Fatalf("re-run of finished session must be a no-op: %+v -> %+v", st, st2)
	}

	// Guard 3: appending a NEWER chunk after completion still works and is
	// applied exactly once on the next run.
	extra := mkTx("extra-new", "acct-norm", "ZZ", 5, base.Add(400*24*time.Hour))
	if err := sess.Submit([]domain.Transaction{extra}); err != nil {
		t.Fatalf("submitting newer data after completion must be allowed: %v", err)
	}
	st3, err := sess.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if st3.Applied != st.Applied+1 {
		t.Fatalf("expected exactly one more applied event, got %+v (before %+v)", st3, st)
	}
	v, ok := proc.Verdict("extra-new")
	if !ok || v.Action != domain.ActionBlock || v.IsLate {
		t.Fatalf("extra event verdict wrong: %+v ok=%v", v, ok)
	}
	t.Logf("scenario=chunked_streaming result=PASS applied=%d chunks=%d compaction_exercised=true guards_ok=true",
		st3.Applied, nChunks)
}
