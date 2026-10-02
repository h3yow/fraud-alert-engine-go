package processor

import (
	"testing"
	"time"

	"github.com/h3yow/fraud-alert-engine-go/internal/domain"
)

// Historical ingestion never flags events late merely because the live
// watermark is far ahead; otherwise backfilling old data would turn every
// normal historical event into a "late" anomaly.
func TestHistoricalIngestionNotFlaggedLateAndSeamsWatermark(t *testing.T) {
	p, _ := newP(t)
	base := time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)

	// Live traffic far in the future advances the real watermark.
	live := p.Process(mkTx("live", "acct-live", "US", 50000, base.Add(30*24*time.Hour)))
	if live.IsDuplicate || live.Action != domain.ActionBlock {
		t.Fatalf("seed live verdict unexpected: %+v", live)
	}

	// An old historical event processed via the historical path must not be
	// late even though it is weeks behind the live watermark.
	hv := p.IngestHistorical(mkTx("hist", "acct-h", "US", 5, base))
	if hv.IsLate {
		t.Fatal("historical ingestion must not mark an event late vs live watermark")
	}
	if p.HistoricalHighWatermark() != base {
		t.Fatalf("historical watermark should track backfill time, got %s", p.HistoricalHighWatermark())
	}

	// Before the seam is closed, a new live event in the recent era is still
	// measured against the live watermark (unchanged during backfill). It is
	// 2 minutes behind the watermark, beyond the 1-minute late grace.
	recent := p.Process(mkTx("recent", "acct-r", "US", 5, base.Add(30*24*time.Hour-2*time.Minute)))
	if !recent.IsLate {
		t.Fatal("live late detection must keep working with the untouched live watermark")
	}

	// After backfill completes, monotonically connect to historical time.
	// The historical high (30d in this fixture) never exceeds live, so the
	// live watermark must NOT roll back.
	p.AdvanceWatermark(p.HistoricalHighWatermark())
	probe := p.Process(mkTx("probe", "acct-p", "US", 5, base.Add(30*24*time.Hour-2*time.Minute)))
	if !probe.IsLate {
		t.Fatal("watermark must not roll back after a purely-older backfill")
	}
}

// When a backfill contains data newer than anything live has seen, the seam
// advances the watermark forward to the historical high; events immediately
// after it are on time, events far behind are late.
func TestWatermarkAdvancedForwardAfterNewerBackfill(t *testing.T) {
	p, _ := newP(t)
	base := time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)

	p.IngestHistorical(mkTx("h1", "acct", "US", 5, base))
	p.IngestHistorical(mkTx("h2", "acct", "US", 5, base.Add(10*24*time.Hour)))
	p.AdvanceWatermark(p.HistoricalHighWatermark())

	onTime := p.Process(mkTx("n1", "acct-n", "US", 5, base.Add(10*24*time.Hour+30*time.Second)))
	if onTime.IsLate {
		t.Fatal("event just after historical high must be on time after the seam")
	}
	late := p.Process(mkTx("n2", "acct-n2", "US", 5, base.Add(9*24*time.Hour)))
	if !late.IsLate {
		t.Fatal("event far behind the connected watermark must be flagged late")
	}
}

// Historical and live paths share one idempotence table: an event ingested
// historically replays identically through the live API and counts once.
func TestHistoricalAndLiveShareIdempotence(t *testing.T) {
	p, _ := newP(t)
	base := time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)
	tx := mkTx("shared", "acct-z", "ZZ", 50000, base)

	hv := p.IngestHistorical(tx)
	if hv.Action != domain.ActionBlock || hv.AlertKey == "" {
		t.Fatalf("historical firing event must block with alert, got %+v", hv)
	}
	lv := p.Process(tx)
	if !lv.IsDuplicate || lv.Action != hv.Action || lv.AlertKey != hv.AlertKey {
		t.Fatalf("historical verdict must replay through live API: %+v vs %+v", lv, hv)
	}
	if got := p.Alerts()[0].HitCount; got != 1 {
		t.Fatalf("exactly one unique hit, got %d", got)
	}

	// And the reverse ordering: live first, historical replay.
	p2, _ := newP(t)
	lv2 := p2.Process(tx)
	hv2 := p2.IngestHistorical(tx)
	if !hv2.IsDuplicate || hv2.AlertKey != lv2.AlertKey {
		t.Fatalf("historical replay of a live-first event must be idempotent: %+v vs %+v", hv2, lv2)
	}
}

// Alert operational states can be snapshotted and restored (used by
// checkpoints), and restore is monotone.
func TestAlertStateSnapshotRestoreRoundTrip(t *testing.T) {
	p, _ := newP(t)
	base := time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)
	v := p.Process(mkTx("sx", "acct", "ZZ", 5, base))
	if _, ok := p.AlertStore().Ack(v.AlertKey, base.Add(time.Hour)); !ok {
		t.Fatal("ack")
	}
	snap := p.SnapshotAlertStates()
	if len(snap) != 1 || snap[0].State != domain.StateAcked {
		t.Fatalf("snapshot wrong: %+v", snap)
	}

	// Fresh processor, restore before replay, then replay hits: the restored
	// ACKED state must survive the subsequent alert fold.
	p2, _ := newP(t)
	p2.RestoreAlertStates(snap)
	p2.IngestHistorical(mkTx("sx", "acct", "ZZ", 5, base))
	a, ok := p2.AlertStore().Get(v.AlertKey)
	if !ok || a.State != domain.StateAcked {
		t.Fatalf("restored operational state lost on fold: %+v ok=%v", a, ok)
	}
	if a.HitCount != 1 {
		t.Fatalf("restored alert must receive the replayed hit, count=%d", a.HitCount)
	}
}

// Concurrent first deliveries of one EventID across the historical and live
// paths must produce exactly one authoritative verdict.
func TestConcurrentHistoricalAndLiveSameEvent(t *testing.T) {
	p, _ := newP(t)
	base := time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)
	tx := mkTx("race-x", "acct-race", "ZZ", 5, base)

	const n = 24
	results := make([]domain.Verdict, n)
	done := make(chan struct{}, n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			if i%2 == 0 {
				results[i] = p.Process(tx)
			} else {
				results[i] = p.IngestHistorical(tx)
			}
			done <- struct{}{}
		}()
	}
	for i := 0; i < n; i++ {
		<-done
	}
	firsts, dups := 0, 0
	for _, v := range results {
		if v.Action != domain.ActionBlock {
			t.Fatalf("wrong action %s", v.Action)
		}
		if v.IsDuplicate {
			dups++
		} else {
			firsts++
		}
	}
	if firsts != 1 || dups != n-1 {
		t.Fatalf("exactly one first delivery required, firsts=%d dups=%d", firsts, dups)
	}
	if p.Alerts()[0].HitCount != 1 {
		t.Fatal("single unique hit required across mixed-path concurrency")
	}
}
