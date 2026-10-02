package backfill

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/h3yow/fraud-alert-engine-go/internal/alert"
	"github.com/h3yow/fraud-alert-engine-go/internal/domain"
	"github.com/h3yow/fraud-alert-engine-go/internal/engine"
	"github.com/h3yow/fraud-alert-engine-go/internal/logging"
	"github.com/h3yow/fraud-alert-engine-go/internal/processor"
	"github.com/h3yow/fraud-alert-engine-go/internal/rule"
)

// ---- fixtures -----------------------------------------------------------

func mkRec(seq int64, event, acct, country string, amount float64, at time.Time) Record {
	return Record{
		Seq: seq,
		Transaction: domain.Transaction{
			EventID: event, TxID: "T" + event, AccountID: acct,
			Amount: amount, Currency: "USD", Country: country, EventType: "PAY",
			OccurredAt: at, IngestedAt: at.Add(time.Second),
		},
	}
}

func newStack(t *testing.T) (*processor.Processor, *logging.BufferLogger) {
	t.Helper()
	lg := &logging.BufferLogger{}
	eng := engine.New(rule.NewDefaultRegistry(), lg)
	p := processor.New(eng, alert.New(lg), time.Minute, lg)
	return p, lg
}

// assertSameVerdicts compares the essential, externally visible parts of two
// verdicts for the same EventID.
func assertSameVerdicts(t *testing.T, tag string, got, want map[string]domain.Verdict) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("[%s] unique event count differs: backfill=%d reference=%d", tag, len(got), len(want))
	}
	for id, w := range want {
		g, ok := got[id]
		if !ok {
			t.Fatalf("[%s] event %s missing after backfill", tag, id)
		}
		if g.Action != w.Action || g.Failure != w.Failure || g.Degraded != w.Degraded {
			t.Fatalf("[%s] event %s action/failure/degraded differs: got %s/%s/%v want %s/%s/%v",
				tag, id, g.Action, g.Failure, g.Degraded, w.Action, w.Failure, w.Degraded)
		}
		if (g.WinningHit == nil) != (w.WinningHit == nil) {
			t.Fatalf("[%s] event %s winning-hit presence differs", tag, id)
		}
		if g.WinningHit != nil && g.WinningHit.RuleID != w.WinningHit.RuleID {
			t.Fatalf("[%s] event %s winning rule differs: got %s want %s",
				tag, id, g.WinningHit.RuleID, w.WinningHit.RuleID)
		}
		if g.AlertKey != w.AlertKey {
			t.Fatalf("[%s] event %s alert key differs: got %q want %q", tag, id, g.AlertKey, w.AlertKey)
		}
		if len(g.Hits) != len(w.Hits) {
			t.Fatalf("[%s] event %s hit count differs: got %d want %d", tag, id, len(g.Hits), len(w.Hits))
		}
	}
}

func alertKeyByEvent(alerts []domain.Alert) map[string]string {
	m := map[string]string{}
	for _, a := range alerts {
		for id := range a.EventIDs {
			m[id] = a.Key
		}
	}
	return m
}

func assertAlertsEqual(t *testing.T, tag string, got, want []domain.Alert) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("[%s] alert count differs: backfill=%d reference=%d\n got=%v\nwant=%v",
			tag, len(got), len(want), keys(got), keys(want))
	}
	wm := map[string]domain.Alert{}
	for _, a := range want {
		wm[a.Key] = a
	}
	for _, g := range got {
		w, ok := wm[g.Key]
		if !ok {
			t.Fatalf("[%s] unexpected alert %s", tag, g.Key)
		}
		if g.HitCount != w.HitCount {
			t.Fatalf("[%s] alert %s HitCount differs: got %d want %d", tag, g.Key, g.HitCount, w.HitCount)
		}
		if !g.FirstSeen.Equal(w.FirstSeen) || !g.LastSeen.Equal(w.LastSeen) {
			t.Fatalf("[%s] alert %s first/last differs: got [%s,%s] want [%s,%s]",
				tag, g.Key, g.FirstSeen, g.LastSeen, w.FirstSeen, w.LastSeen)
		}
		if g.State != w.State {
			t.Fatalf("[%s] alert %s state differs: got %s want %s", tag, g.State, g.State, w.State)
		}
	}
}

func keys(as []domain.Alert) []string {
	out := make([]string, 0, len(as))
	for _, a := range as {
		out = append(out, a.Key)
	}
	return out
}

// uniqueEventsOf returns one record per EventID keeping the earliest key.
func uniqueEventsOf(recs []Record) []Record {
	best := map[string]Record{}
	for _, r := range recs {
		b, ok := best[r.Transaction.EventID]
		if !ok || r.Key().Before(b.Key()) {
			best[r.Transaction.EventID] = r
		}
	}
	return recordsOfMap(best)
}

func recordsOfMap(m map[string]Record) []Record {
	out := make([]Record, 0, len(m))
	for _, r := range m {
		out = append(out, r)
	}
	sortRecords(out)
	return out
}

func sortRecords(rs []Record) {
	for i := 1; i < len(rs); i++ {
		for j := i; j > 0 && rs[j].Key().Before(rs[j-1].Key()); j-- {
			rs[j], rs[j-1] = rs[j-1], rs[j]
		}
	}
}

// scenarioRecords builds a deliberately messy historical batch: a wide time
// span, in-batch duplicates and shuffled arrival order. It covers:
//   - 6 events of one account within the 5min velocity window (limit 3),
//   - high-risk-country BLOCKs,
//   - large-amount BLOCKs,
//   - plain ALLOW events.
func scenarioRecords(base time.Time) []Record {
	recs := []Record{}
	add := func(event, acct, country string, amount float64, offset time.Duration) {
		recs = append(recs, mkRec(int64(len(recs))+1, event, acct, country, amount, base.Add(offset)))
	}
	// Velocity burst: acct-V, six events within 4 minutes -> events 4..6 REVIEW.
	for i := 0; i < 6; i++ {
		add(fmt.Sprintf("v%d", i+1), "acct-V", "US", 10, time.Duration(i)*40*time.Second)
	}
	// Country blocks far apart in business time.
	add("z1", "acct-Z", "ZZ", 10, -30*24*time.Hour)
	add("z2", "acct-Z", "ZZ", 10, 30*24*time.Hour)
	// Large amount block + allow in the middle.
	add("big1", "acct-L", "US", 50000, 2*time.Hour)
	add("ok1", "acct-L", "US", 50, 2*time.Hour+time.Second)
	// Cluster members merge into one cluster alert.
	c1 := mkRec(int64(len(recs))+1, "cl1", "acct-C", "ZZ", 5, base.Add(6*time.Hour))
	c1.Transaction.ClusterID = "CL-1"
	recs = append(recs, c1)
	c2 := mkRec(int64(len(recs))+1, "cl2", "acct-C", "ZZ", 5, base.Add(6*time.Hour+30*time.Second))
	c2.Transaction.ClusterID = "CL-1"
	recs = append(recs, c2)

	// In-batch duplicates: repeat v4 (firing event), z1 and an ALLOW event,
	// including one duplicate with a *later* business time but same EventID
	// (earliest must win, exactly as in the ascending reference run).
	dupV4 := recs[3]
	dupV4.Seq = 101
	recs = append(recs, dupV4)
	dupZ1 := recs[6]
	dupZ1.Seq = 102
	dupZ1.Transaction.OccurredAt = base.Add(40 * 24 * time.Hour) // same id, later time
	recs = append(recs, dupZ1)

	// Shuffle hard so the source itself is out of order.
	order := []int{12, 2, 9, 0, 11, 4, 7, 13, 1, 10, 5, 3, 8, 6}
	shuffled := make([]Record, len(recs))
	for i, idx := range order {
		shuffled[i] = recs[idx]
	}
	return shuffled
}

// referenceRun processes the de-duplicated batch in business-time ascending
// order through a normal real-time processor, exactly the semantics the
// backfill must reproduce.
func referenceRun(t *testing.T, recs []Record) (map[string]domain.Verdict, []domain.Alert) {
	t.Helper()
	p, _ := newStack(t)
	uniq := uniqueEventsOf(recs)
	out := map[string]domain.Verdict{}
	for _, r := range uniq {
		v := p.Process(r.Transaction)
		if v.IsDuplicate {
			t.Fatalf("reference run unexpectedly saw duplicate %s", r.Transaction.EventID)
		}
		out[r.Transaction.EventID] = v
	}
	return out, p.Alerts()
}

func backfillRun(t *testing.T, ctx context.Context, recs []Record, chunkSize, microBatch int) (*processor.Processor, Result) {
	t.Helper()
	p, _ := newStack(t)
	rn := NewRunner(NewMemStore(), &scenarioLogger{t: t})
	res, err := rn.Run(ctx, p, Config{
		JobID:          "job-1",
		ChunkSize:      chunkSize,
		MicroBatchSize: microBatch,
		Source:         SliceReader(recs),
	})
	if err != nil {
		t.Fatalf("backfill run failed: %v", err)
	}
	return p, res
}

type scenarioLogger struct{ t *testing.T }

func (l scenarioLogger) Printf(format string, args ...any) {
	l.t.Logf(format, args...)
}

// ---- tests --------------------------------------------------------------

func TestBackfillEquivalentToAscendingReference(t *testing.T) {
	t.Log("=== scenario: messy historical batch (out-of-order + duplicates + wide span) ===")
	base := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	recs := scenarioRecords(base)

	wantV, wantAlerts := referenceRun(t, recs)

	// Tiny chunk/microbatch sizes to force many spills, checkpoints and
	// k-way merge paths while keeping memory bounded.
	p, res := backfillRun(t, context.Background(), recs, 3, 2)
	t.Logf("backfill result: applied=%d duplicates=%d chunks_spilled=%d peak_buffered=%d completed=%v",
		res.Applied, res.Duplicates, res.ChunksSpilled, res.PeakBuffered, res.Completed)

	if !res.Completed {
		t.Fatal("backfill must report completion")
	}
	if res.Applied != int64(len(wantV)) {
		t.Fatalf("applied unique events: got %d want %d", res.Applied, len(wantV))
	}
	if res.Duplicates != 2 { // the two in-batch repeats (v4, z1)
		t.Fatalf("duplicate records absorbed: got %d want 2", res.Duplicates)
	}
	// Memory must stay bounded by the chunk size, never the full batch:
	// 15 source records, chunk size 3 -> peak buffered <= 3 (+ zero merge
	// retention beyond heap heads, which is #chunks <= 5).
	if res.PeakBuffered > 3 {
		t.Fatalf("peak records buffered must stay <= chunkSize, got %d", res.PeakBuffered)
	}

	gotV := map[string]domain.Verdict{}
	for _, r := range uniqueEventsOf(recs) {
		// Re-deliver through the live API to retrieve cached verdicts.
		v := p.Process(r.Transaction)
		if !v.IsDuplicate {
			t.Fatalf("post-backfill delivery of %s must replay cached verdict (duplicate)", r.Transaction.EventID)
		}
		gotV[r.Transaction.EventID] = v
	}
	assertSameVerdicts(t, "equiv", gotV, wantV)

	gotAlerts := p.Alerts()
	assertAlertsEqual(t, "equiv", gotAlerts, wantAlerts)

	// Watermark must have been advanced to the highest historical time.
	late := p.Process(mkRec(9001, "late-probe", "acct-Z", "US", 5, base.Add(41*24*time.Hour)).Transaction)
	if late.IsLate {
		t.Fatal("event just after historical high watermark must not be late")
	}
	veryLate := p.Process(mkRec(9002, "late-probe-2", "acct-Z", "US", 5, base.Add(-100*24*time.Hour)).Transaction)
	if !veryLate.IsLate {
		t.Fatal("event far behind the connected watermark must still be flagged late")
	}
	t.Log("result: verdicts, hit rules, alert assignments, counts, first/last and late seam all match the ascending reference")
}

func TestBackfillBoundedMemoryManyChunks(t *testing.T) {
	t.Log("=== scenario: batch far larger than one chunk, bounded memory ===")
	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	const n = 500
	recs := make([]Record, 0, n)
	for i := 0; i < n; i++ {
		// Reverse business order on purpose.
		at := base.Add(time.Duration(n-1-i) * time.Minute)
		recs = append(recs, mkRec(int64(i)+1, fmt.Sprintf("m%04d", i), "acct", "US", 10, at))
	}
	p, res := backfillRun(t, context.Background(), recs, 37, 50)
	t.Logf("processed=%d chunks=%d peak_buffered=%d", n, res.ChunksSpilled, res.PeakBuffered)
	if !res.Completed || res.Applied != n || res.Duplicates != 0 {
		t.Fatalf("unexpected result: %+v", res)
	}
	if res.PeakBuffered > 37 {
		t.Fatalf("memory grew beyond chunk size: peak=%d", res.PeakBuffered)
	}
	// All events belong to one account one minute apart: velocity (window
	// 5min, limit 3) fires from the 4th event onward. The alert is keyed by
	// (rule, account, 10min cooldown bucket), so the unique-hit sum must
	// equal the number of firing EventIDs (500 - 3 = 497), proving the
	// stateful rule sees the ascending merged stream exactly like a reference
	// run despite chunked, reverse-order ingestion.
	sum := 0
	for _, a := range p.Alerts() {
		sum += a.HitCount
	}
	if sum != n-3 {
		t.Fatalf("velocity firing unique events: got %d want %d", sum, n-3)
	}
}

func TestBackfillDiskStoreRoundTrip(t *testing.T) {
	t.Log("=== scenario: DiskStore persists chunks as JSONL and reloads them ===")
	dir := t.TempDir()
	store, err := NewDiskStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 3, 3, 8, 0, 0, 0, time.UTC)
	recs := []Record{
		mkRec(2, "b", "acct", "US", 5, base.Add(time.Minute)),
		mkRec(1, "a", "acct", "ZZ", 5, base),
	}
	id, err := store.WriteChunk(context.Background(), recs)
	if err != nil {
		t.Fatal(err)
	}
	it, err := store.OpenChunk(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()
	got := []Record{}
	for {
		r, ok, err := it.Next()
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		got = append(got, r)
	}
	if len(got) != 2 || got[0].Transaction.EventID != "b" || got[1].Transaction.EventID != "a" {
		t.Fatalf("chunk roundtrip order/content wrong: %+v", got)
	}
	m := Manifest{JobID: "jp", Phase: PhaseMerge, Chunks: []string{id},
		Cursor: recs[0].Key()}
	if err := store.SaveManifest(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	// Re-open from scratch (simulating process restart).
	store2, err := NewDiskStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	m2, ok, err := store2.LoadManifest(context.Background(), "jp")
	if err != nil || !ok {
		t.Fatalf("manifest reload: ok=%v err=%v", ok, err)
	}
	if m2.Phase != PhaseMerge || len(m2.Chunks) != 1 || m2.Cursor.EventID != "b" {
		t.Fatalf("manifest content wrong: %+v", m2)
	}
	t.Log("result: chunks and manifest survive a fresh DiskStore at the same path")
}

func TestBackfillIdempotentWithLiveState(t *testing.T) {
	t.Log("=== scenario: historical EventID already processed live is never double counted ===")
	base := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	p, _ := newStack(t)
	// Live has already seen z-live (e.g. a prior partial run).
	live := mkRec(1, "z-live", "acct-Z", "ZZ", 10, base)
	first := p.Process(live.Transaction)
	if first.IsDuplicate {
		t.Fatal("seed live event must be a first delivery")
	}

	rn := NewRunner(NewMemStore(), scenarioLogger{t})
	recs := []Record{
		live, // same EventID inside the historical batch
		mkRec(2, "z-new", "acct-Z2", "ZZ", 10, base.Add(time.Hour)),
	}
	res, err := rn.Run(context.Background(), p, Config{JobID: "j-idem", ChunkSize: 10, Source: SliceReader(recs)})
	if err != nil {
		t.Fatal(err)
	}
	if res.Applied != 1 || res.Duplicates != 1 {
		t.Fatalf("want applied=1 dup=1, got %+v", res)
	}
	total := 0
	for _, a := range p.Alerts() {
		total += a.HitCount
	}
	// Two distinct firing events total; different accounts -> two alerts.
	if total != 2 {
		t.Fatalf("count conservation broken: unique firing events=%d want 2", total)
	}
	t.Log("result: overlapping EventID replayed once, no double count")
}

func TestBackfillConcurrentWithLive(t *testing.T) {
	t.Log("=== scenario: backfill running while real-time events flow concurrently ===")
	base := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	p, _ := newStack(t)
	store := NewMemStore()
	rn := NewRunner(store, scenarioLogger{t})

	// Historical batch: older business times, one velocity burst acct-H.
	hist := []Record{}
	for i := 0; i < 5; i++ {
		hist = append(hist, mkRec(int64(i)+1, fmt.Sprintf("h%d", i), "acct-H", "US", 10,
			base.Add(time.Duration(i)*30*time.Second)))
	}
	hist = append(hist,
		mkRec(6, "hz", "acct-HZ", "ZZ", 10, base.Add(-time.Hour)),
	)

	var wg sync.WaitGroup
	var liveFirstVerdict domain.Verdict
	wg.Add(2)

	// Live traffic: EventIDs disjoint from history but on a different account,
	// plus one duplicate delivery of the same live event.
	go func() {
		defer wg.Done()
		v1 := p.Process(mkRec(201, "live1", "acct-N", "US", 50000, base.Add(48*time.Hour)).Transaction)
		v2 := p.Process(mkRec(201, "live1", "acct-N", "US", 50000, base.Add(48*time.Hour)).Transaction)
		if v1.IsDuplicate || !v2.IsDuplicate || v1.AlertKey != v2.AlertKey {
			t.Errorf("live duplicate verdict unstable: %+v vs %+v", v1, v2)
		}
		liveFirstVerdict = v1
		// Live event far newer than history must never be marked late during
		// backfill (live watermark keeps advancing independently).
		v3 := p.Process(mkRec(202, "live2", "acct-N", "US", 5, base.Add(49*time.Hour)).Transaction)
		if v3.IsLate {
			t.Errorf("new live event wrongly marked late during backfill")
		}
	}()

	go func() {
		defer wg.Done()
		res, err := rn.Run(context.Background(), p, Config{
			JobID: "j-conc", ChunkSize: 2, MicroBatchSize: 1, Source: SliceReader(hist),
		})
		if err != nil {
			t.Errorf("concurrent backfill failed: %v", err)
			return
		}
		if !res.Completed || res.Applied != 6 || res.Duplicates != 0 {
			t.Errorf("unexpected concurrent backfill result: %+v", res)
		}
	}()
	wg.Wait()

	// Count conservation across the union of both streams.
	total := 0
	for _, a := range p.Alerts() {
		total += a.HitCount
	}
	// Firing unique events: h3,h4 (velocity) + hz (country) + live1 (amount) = 4.
	if total != 4 {
		t.Fatalf("union count conservation: got %d want 4; alerts=%+v", total, p.Alerts())
	}
	// Live verdict immutable afterwards.
	replay := p.Process(mkRec(201, "live1", "acct-N", "US", 50000, base.Add(48*time.Hour)).Transaction)
	if !replay.IsDuplicate || replay.AlertKey != liveFirstVerdict.AlertKey || replay.Action != liveFirstVerdict.Action {
		t.Fatalf("live verdict changed after backfill: %+v vs %+v", replay, liveFirstVerdict)
	}
	t.Logf("result: concurrent live + backfill consistent, unique firing count=%d, live verdict stable", total)
}

// resumeRecords builds a batch whose reference outcome is easy to account
// for: alternating country blocks and allows plus a velocity burst.
func resumeRecords(base time.Time) []Record {
	recs := []Record{}
	for i := 0; i < 12; i++ {
		country := "US"
		if i%3 == 0 {
			country = "ZZ"
		}
		recs = append(recs, mkRec(int64(i)+1,
			fmt.Sprintf("r%02d", i), "acct-r", country, 10,
			base.Add(time.Duration(i)*20*time.Second)))
	}
	// One in-batch duplicate of r00 (a ZZ firing event) at the tail.
	dup := recs[0]
	dup.Seq = 99
	recs = append(recs, dup)
	// Reverse the source order to make the runner earn its merge.
	out := make([]Record, len(recs))
	for i := range recs {
		out[i] = recs[len(recs)-1-i]
	}
	return out
}

// expectedResumeCounts returns (unique firing events, unique total events)
// for resumeRecords. Twelve events 20s apart sit inside one 5-minute
// velocity window, so velocity (limit 3) fires from the 4th event on
// (indices 3..11 = 9 EventIDs). The ZZ events are indices 0,3,6,9; three
// of them are already in the velocity set, leaving index 0 as the only
// additional firing EventID. Union size: 9 + 1 = 10.
func expectedResumeCounts() (firing, total int) {
	zz := map[int]bool{0: true, 3: true, 6: true, 9: true}
	firingIDs := map[int]bool{}
	for i := 0; i < 12; i++ {
		if zz[i] || i >= 3 {
			firingIDs[i] = true
		}
	}
	return len(firingIDs), 12
}

func TestBackfillPauseAndResumeConservesCounts(t *testing.T) {
	t.Log("=== scenario: cancel mid-merge repeatedly, resume on same processor, counts conserved ===")
	base := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	recs := resumeRecords(base)
	wantFiring, wantTotal := expectedResumeCounts()

	p, _ := newStack(t)
	rn := NewRunner(NewMemStore(), scenarioLogger{t})
	const job = "j-resume"

	// Run in up to 4 segments; each non-final segment cancels right after
	// its 2nd committed checkpoint. Merge skips the committed cursor on
	// resume, replaying at most one micro-batch through idempotence.
	segApplied, segDups := segmentedRun(t, rn, p, job, recs, 4, 2, 4)

	totalFiring := 0
	for _, a := range p.Alerts() {
		totalFiring += a.HitCount
	}
	if totalFiring != wantFiring {
		t.Fatalf("after resume, unique firing events=%d want %d", totalFiring, wantFiring)
	}
	// 13 records streamed, 12 unique EventIDs: exactly one duplicate overall.
	if segApplied != int64(wantTotal) || segDups != 1 {
		t.Fatalf("segments totals: applied=%d dup=%d, want applied=%d dup=1",
			segApplied, segDups, wantTotal)
	}
	t.Logf("result: resumed across segments, applied=%d dup=%d, unique firing=%d, no double count, no loss",
		segApplied, segDups, totalFiring)
}

// segmentedRun runs a job in at most maxSegments Run() calls, cancelling ctx
// after cancelAfterBatches committed micro-batches of each segment. It
// returns totals applied/duplicated observed by the runner across segments.
func segmentedRun(t *testing.T, rn *Runner, p *processor.Processor, job string,
	recs []Record, chunkSize, microBatch, maxSegments int) (int64, int64) {
	t.Helper()
	var applied, dups int64
	for seg := 0; seg < maxSegments; seg++ {
		ctx, cancel := context.WithCancel(context.Background())
		committed := int64(0)
		// Cancel right after the 2nd MERGE checkpoint of each non-final
		// segment. Sort-phase manifest saves must NOT cancel: they are the
		// spill checkpoints themselves and the sort loop already cooperates
		// with ctx on chunk boundaries, but pausing there changes chunk
		// accounting; merge-phase pausing is what resume is designed for.
		hooked := &cancelStore{ProgressStore: rn.store, onSave: func(m Manifest) bool {
			if m.Phase != PhaseMerge {
				return false
			}
			committed++
			if seg < maxSegments-1 && committed == 2 {
				cancel()
				return true
			}
			return false
		}}
		rnSeg := NewRunner(hooked, rn.logger)
		res, err := rnSeg.Run(ctx, p, Config{
			JobID: job, ChunkSize: chunkSize, MicroBatchSize: microBatch,
			Source: SliceReader(recs),
		})
		applied += res.Applied
		dups += res.Duplicates
		if err != nil && err != context.Canceled {
			t.Fatalf("segment %d failed: %v", seg, err)
		}
		if res.Completed {
			cancel()
			return applied, dups
		}
		cancel()
	}
	t.Fatal("job did not complete within maxSegments")
	return applied, dups
}

// cancelStore wraps a ProgressStore to trigger context cancellation right
// after a chosen MERGE-phase checkpoint SaveManifest.
type cancelStore struct {
	ProgressStore
	// onSave may return true just for logging; cancellation is driven by the
	// caller closing the captured ctx inside the callback.
	onSave func(m Manifest) bool
}

func (c *cancelStore) SaveManifest(ctx context.Context, m Manifest) error {
	err := c.ProgressStore.SaveManifest(ctx, m)
	if c.onSave != nil {
		c.onSave(m)
	}
	return err
}

func TestBackfillRestartRebuildsProcessor(t *testing.T) {
	t.Log("=== scenario: process restart: fresh processor rebuilt via RestoreInto, state identical ===")
	base := time.Date(2026, 5, 15, 0, 0, 0, 0, time.UTC)
	recs := scenarioRecords(base)
	wantV, wantAlerts := referenceRun(t, recs)

	dir := t.TempDir()
	store, err := NewDiskStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	// Simulate: first process ran partway, crashed mid-merge.
	p1, _ := newStack(t)
	ctx, cancel := context.WithCancel(context.Background())
	committed := 0
	hooked := &cancelStore{ProgressStore: store, onSave: func(m Manifest) bool {
		if m.Phase != PhaseMerge {
			return false
		}
		committed++
		if committed == 3 { // crash after 3 merge checkpoints
			cancel()
		}
		return false
	}}
	rnHook := NewRunner(hooked, scenarioLogger{t})
	res1, err := rnHook.Run(ctx, p1, Config{
		JobID: "j-restart", ChunkSize: 3, MicroBatchSize: 2, Source: SliceReader(recs),
	})
	if err != context.Canceled || res1.Completed {
		t.Fatalf("partial run setup unexpected: res=%+v err=%v", res1, err)
	}
	cancel()
	partialAlerts := len(p1.Alerts())
	t.Logf("before restart: runner applied=%d dup=%d partial_alerts=%d",
		res1.Applied, res1.Duplicates, partialAlerts)

	// Simulate restart: brand-new processor and brand-new runner on the same
	// dir; replay chunks to rebuild engine+alert state from scratch.
	p2, _ := newStack(t)
	rn2 := NewRunner(store, scenarioLogger{t})
	if err := rn2.RestoreInto(context.Background(), p2, "j-restart"); err != nil {
		t.Fatalf("restore failed: %v", err)
	}
	// Finish the job on the rebuilt processor.
	res2, err := rn2.Run(context.Background(), p2, Config{
		JobID: "j-restart", ChunkSize: 3, MicroBatchSize: 2, Source: SliceReader(recs),
	})
	if err != nil {
		t.Fatalf("post-restart run failed: %v", err)
	}
	if !res2.Completed {
		t.Fatal("job must complete after restart")
	}

	// The rebuilt+finished processor must equal the reference exactly.
	gotV := map[string]domain.Verdict{}
	for _, r := range uniqueEventsOf(recs) {
		v := p2.Process(r.Transaction)
		gotV[r.Transaction.EventID] = v
	}
	assertSameVerdicts(t, "restart", gotV, wantV)
	assertAlertsEqual(t, "restart", p2.Alerts(), wantAlerts)
	totalFiring := 0
	for _, a := range p2.Alerts() {
		totalFiring += a.HitCount
	}
	t.Logf("result: restart replay+resume gives identical verdicts/alerts; unique firing events=%d; run2 applied=%d dup=%d",
		totalFiring, res2.Applied, res2.Duplicates)
}

func TestBackfillRestartPreservesAlertState(t *testing.T) {
	t.Log("=== scenario: operator ACK before crash survives RestoreInto (no state rollback) ===")
	base := time.Date(2026, 4, 10, 0, 0, 0, 0, time.UTC)
	dir := t.TempDir()
	store, err := NewDiskStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	recs := []Record{
		mkRec(1, "zz1", "acct", "ZZ", 10, base),
		mkRec(2, "ok1", "acct", "US", 10, base.Add(time.Minute)),
		mkRec(3, "zz2", "acct2", "ZZ", 10, base.Add(2*time.Minute)),
	}
	p1, _ := newStack(t)
	rn := NewRunner(store, scenarioLogger{t})
	res, err := rn.Run(context.Background(), p1, Config{
		JobID: "j-state", ChunkSize: 2, MicroBatchSize: 1, Source: SliceReader(recs),
	})
	if err != nil || !res.Completed {
		t.Fatalf("setup: %+v %v", res, err)
	}
	// Completed jobs drop chunks; this test instead models a paused job:
	// rerun with smaller cancellation to leave a merge checkpoint.
	store = mustReopen(t, dir)
	p1b, _ := newStack(t)
	rn = NewRunner(store, scenarioLogger{t})
	ctx, cancel := context.WithCancel(context.Background())
	n := 0
	hooked := &cancelStore{ProgressStore: store, onSave: func(m Manifest) bool {
		if m.Phase != PhaseMerge {
			return false
		}
		n++
		if n == 2 {
			cancel()
		}
		return false
	}}
	rnH := NewRunner(hooked, scenarioLogger{t})
	if _, err := rnH.Run(ctx, p1b, Config{
		JobID: "j-state2", ChunkSize: 2, MicroBatchSize: 1, Source: SliceReader(recs),
	}); err != context.Canceled {
		t.Fatalf("want cancellation, got %v", err)
	}
	cancel()

	// Find the alert for zz1 and ack it on the checkpointed process.
	zzKey := ""
	for _, a := range p1b.Alerts() {
		if _, ok := a.EventIDs["zz1"]; ok {
			zzKey = a.Key
		}
	}
	if zzKey == "" {
		t.Fatal("zz1 alert missing in partial run")
	}
	// Force one more checkpoint so the ACK is captured, then stop.
	m, ok, err := store.LoadManifest(context.Background(), "j-state2")
	if err != nil || !ok {
		t.Fatalf("load: %v %v", ok, err)
	}
	m.Alerts = toAlertSnapshots(p1b.SnapshotAlertStates())
	if err := store.SaveManifest(context.Background(), *m); err != nil {
		t.Fatal(err)
	}
	if a, ok := p1b.AlertStore().Ack(zzKey, time.Now()); !ok || a.State != domain.StateAcked {
		t.Fatalf("ack setup failed: %+v %v", a, ok)
	}
	m.Alerts = toAlertSnapshots(p1b.SnapshotAlertStates())
	if err := store.SaveManifest(context.Background(), *m); err != nil {
		t.Fatal(err)
	}

	// Restart into a fresh processor: state replay + operational restore.
	p2, _ := newStack(t)
	rn2 := NewRunner(mustReopen(t, dir), scenarioLogger{t})
	if err := rn2.RestoreInto(context.Background(), p2, "j-state2"); err != nil {
		t.Fatal(err)
	}
	a, ok := p2.AlertStore().Get(zzKey)
	if !ok {
		t.Fatal("rebuilt alert missing")
	}
	if a.State != domain.StateAcked {
		t.Fatalf("alert state rolled back across restart: got %s want ACKED", a.State)
	}
	res2, err := rn2.Run(context.Background(), p2, Config{
		JobID: "j-state2", ChunkSize: 2, MicroBatchSize: 1, Source: SliceReader(recs),
	})
	if err != nil || !res2.Completed {
		t.Fatalf("finish after restart: %+v %v", res2, err)
	}
	a, _ = p2.AlertStore().Get(zzKey)
	if a.State != domain.StateAcked {
		t.Fatalf("resume completion moved state: %s", a.State)
	}
	// Counts still conserved: zz1 and zz2 fire (different buckets/accounts).
	if a, _ := p2.AlertStore().Get(zzKey); a.HitCount != 1 {
		t.Fatalf("zz1 alert must keep HitCount=1 after restart, got %d", a.HitCount)
	}
	t.Log("result: ACKED state survived crash+replay, counts conserved, terminal workflow intact")
}

func mustReopen(t *testing.T, dir string) *DiskStore {
	t.Helper()
	s, err := NewDiskStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func toAlertSnapshots(recs []domain.AlertStateRecord) []AlertSnapshot {
	out := make([]AlertSnapshot, 0, len(recs))
	for _, r := range recs {
		out = append(out, AlertSnapshot{Key: r.Key, State: r.State, UpdatedAt: r.UpdatedAt})
	}
	return out
}
