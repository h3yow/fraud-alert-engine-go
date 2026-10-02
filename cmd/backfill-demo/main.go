// Command backfill-demo is the local end-to-end demonstration of historical
// backfill: it generates a messy JSONL batch (wide business-time span,
// out-of-order, duplicated), backfills it in bounded-memory chunks against
// a live processor, shows a mid-way pause/resume, and finally compares the
// result with a business-time-ascending reference run.
//
// Usage:
//
//	go run ./cmd/backfill-demo            # use a temp dir, full run
//	go run ./cmd/backfill-demo <workdir>  # keep spill files/manifests here
//	go run ./cmd/backfill-demo <workdir> --pause   # cancel, then resume
package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/h3yow/fraud-alert-engine-go/internal/alert"
	"github.com/h3yow/fraud-alert-engine-go/internal/backfill"
	"github.com/h3yow/fraud-alert-engine-go/internal/domain"
	"github.com/h3yow/fraud-alert-engine-go/internal/engine"
	"github.com/h3yow/fraud-alert-engine-go/internal/logging"
	"github.com/h3yow/fraud-alert-engine-go/internal/processor"
	"github.com/h3yow/fraud-alert-engine-go/internal/rule"
)

func main() {
	workdir := os.TempDir()
	pause := false
	for _, a := range os.Args[1:] {
		if a == "--pause" {
			pause = true
		} else {
			workdir = a
		}
	}
	if err := os.MkdirAll(workdir, 0o755); err != nil {
		fatal(err)
	}
	inputPath := filepath.Join(workdir, "history.jsonl")
	spillDir := filepath.Join(workdir, "backfill-state")

	logger := logging.NewStdLogger(os.Stdout)
	base := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)

	// 1) Generate a messy historical batch to disk.
	recs := generateBatch(base)
	if err := writeJSONL(inputPath, recs); err != nil {
		fatal(err)
	}
	fmt.Printf(">>> wrote %d raw historical records to %s (out-of-order, with repeats)\n\n",
		len(recs), inputPath)

	// 2) Reference run: dedup + ascending through the normal real-time API.
	ref := newProcessor(logger)
	refVerdicts, refAlerts := referenceAscending(ref, recs)
	fmt.Printf(">>> reference ascending run: %d unique events, %d alerts\n\n",
		len(refVerdicts), len(refAlerts))

	// 3) Backfill run (optionally paused after two merge checkpoints).
	store, err := backfill.NewDiskStore(spillDir)
	if err != nil {
		fatal(err)
	}
	p := newProcessor(logger)
	rn := backfill.NewRunner(store, logger)

	cfg := backfill.Config{
		JobID: "demo-job", ChunkSize: 5, MicroBatchSize: 4,
		Source: backfill.NewJSONLFileReader(inputPath),
	}

	if pause {
		ctx, cancel := context.WithCancel(context.Background())
		n := 0
		hook := &cancelOnMergeCheckpoint{ProgressStore: store, threshold: 2,
			onThreshold: cancel, onSave: func(m backfill.Manifest) {
				if m.Phase == backfill.PhaseMerge {
					n++
					_ = n
				}
			}}
		rn = backfill.NewRunner(hook, logger)
		res, err := rn.Run(ctx, p, cfg)
		cancel()
		fmt.Printf("\n>>> segment 1: applied=%d dup=%d paused=%v err=%v\n",
			res.Applied, res.Duplicates, res.Paused, err)

		// Resume with a fresh runner on the same on-disk state.
		rn = backfill.NewRunner(store, logger)
		res2, err := rn.Run(context.Background(), p, cfg)
		if err != nil {
			fatal(err)
		}
		fmt.Printf(">>> segment 2: applied=%d dup=%d completed=%v\n\n",
			res2.Applied, res2.Duplicates, res2.Completed)
	} else {
		res, err := rn.Run(context.Background(), p, cfg)
		if err != nil {
			fatal(err)
		}
		fmt.Printf("\n>>> backfill applied=%d dup=%d peak_buffered=%d completed=%v\n\n",
			res.Applied, res.Duplicates, res.PeakBuffered, res.Completed)
	}

	// 4) Compare.
	diffs := compare(ref, p, refVerdicts, recs)
	sumHits := 0
	for _, a := range p.Alerts() {
		sumHits += a.HitCount
	}
	fmt.Printf(">>> backfill alerts=%d sum(unique_hits)=%d verdict diffs=%d\n",
		len(p.Alerts()), sumHits, diffs)
	if diffs != 0 {
		os.Exit(1)
	}
	fmt.Println(">>> OK: backfill result identical to business-time-ascending reference")
}

type backfillLogger interface {
	Printf(format string, args ...any)
}

func newProcessor(logger backfillLogger) *processor.Processor {
	eng := engine.New(rule.NewDefaultRegistry(), logger)
	return processor.New(eng, alert.New(logger), time.Minute, logger)
}

func generateBatch(base time.Time) []backfill.Record {
	recs := []backfill.Record{}
	add := func(id, acct, country string, amount float64, off time.Duration) {
		at := base.Add(off)
		recs = append(recs, backfill.Record{
			Seq: int64(len(recs)) + 1,
			Transaction: domain.Transaction{
				EventID: id, TxID: "T" + id, AccountID: acct, Amount: amount,
				Currency: "USD", Country: country, EventType: "PAY",
				OccurredAt: at, IngestedAt: at.Add(time.Second),
			},
		})
	}
	// Velocity burst inside five minutes (events #4+ REVIEW).
	for i := 0; i < 6; i++ {
		add(fmt.Sprintf("v%d", i+1), "acct-V", "US", 10, time.Duration(i)*40*time.Second)
	}
	add("z-old", "acct-Z", "ZZ", 10, -20*24*time.Hour)
	add("z-new", "acct-Z", "ZZ", 10, 20*24*time.Hour)
	add("big", "acct-L", "US", 50000, 3*time.Hour)
	add("ok", "acct-L", "US", 5, 3*time.Hour+time.Second)

	// In-batch duplicates, one with a later business time.
	dup := recs[3]
	dup.Seq = 101
	recs = append(recs, dup)
	dup2 := recs[6]
	dup2.Seq = 102
	dup2.Transaction.OccurredAt = base.Add(30 * 24 * time.Hour)
	recs = append(recs, dup2)

	// Reverse to make the source itself out of order.
	for i, j := 0, len(recs)-1; i < j; i, j = i+1, j-1 {
		recs[i], recs[j] = recs[j], recs[i]
	}
	return recs
}

func writeJSONL(path string, recs []backfill.Record) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := bufio.NewWriter(f)
	for _, r := range recs {
		t := r.Transaction
		fmt.Fprintf(w,
			`{"seq":%d,"tx":{"event_id":%q,"tx_id":%q,"account_id":%q,"cluster_id":%q,"amount":%g,"currency":%q,"country":%q,"event_type":%q,"occurred_at":%q,"ingested_at":%q}}`+"\n",
			r.Seq, t.EventID, t.TxID, t.AccountID, t.ClusterID, t.Amount, t.Currency,
			t.Country, t.EventType,
			t.OccurredAt.Format(time.RFC3339), t.IngestedAt.Format(time.RFC3339))
	}
	return w.Flush()
}

func referenceAscending(p *processor.Processor, recs []backfill.Record) (map[string]domain.Verdict, []domain.Alert) {
	earliest := map[string]backfill.Record{}
	for _, r := range recs {
		b, ok := earliest[r.Transaction.EventID]
		if !ok || r.Transaction.OccurredAt.Before(b.Transaction.OccurredAt) {
			earliest[r.Transaction.EventID] = r
		}
	}
	ordered := make([]backfill.Record, 0, len(earliest))
	for _, r := range earliest {
		ordered = append(ordered, r)
	}
	sort.Slice(ordered, func(i, j int) bool {
		return ordered[i].Transaction.OccurredAt.Before(ordered[j].Transaction.OccurredAt)
	})
	out := map[string]domain.Verdict{}
	for _, r := range ordered {
		out[r.Transaction.EventID] = p.Process(r.Transaction)
	}
	return out, p.Alerts()
}

func compare(ref, got *processor.Processor, refV map[string]domain.Verdict, recs []backfill.Record) int {
	diffs := 0
	seen := map[string]bool{}
	for _, r := range recs {
		id := r.Transaction.EventID
		if seen[id] {
			continue
		}
		seen[id] = true
		gv := got.Process(r.Transaction) // replay cached backfill verdict
		rv := refV[id]
		if gv.Action != rv.Action || gv.AlertKey != rv.AlertKey ||
			(gv.WinningHit == nil) != (rv.WinningHit == nil) ||
			(gv.WinningHit != nil && rv.WinningHit != nil && gv.WinningHit.RuleID != rv.WinningHit.RuleID) {
			diffs++
			fmt.Printf("DIFF event=%s backfill=%s/%s reference=%s/%s\n", id,
				gv.Action, winner(gv), rv.Action, winner(rv))
		}
	}
	return diffs
}

func winner(v domain.Verdict) string {
	if v.WinningHit == nil {
		return "-"
	}
	return v.WinningHit.RuleID
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(1)
}

// cancelOnMergeCheckpoint wraps the store so the demo deterministically
// pauses after `threshold` merge checkpoints.
type cancelOnMergeCheckpoint struct {
	backfill.ProgressStore
	threshold   int
	mergeSaves  int
	onThreshold func()
	onSave      func(backfill.Manifest)
}

func (c *cancelOnMergeCheckpoint) SaveManifest(ctx context.Context, m backfill.Manifest) error {
	err := c.ProgressStore.SaveManifest(ctx, m)
	if c.onSave != nil {
		c.onSave(m)
	}
	if m.Phase == backfill.PhaseMerge {
		c.mergeSaves++
		if c.mergeSaves == c.threshold && c.onThreshold != nil {
			c.onThreshold()
		}
	}
	return err
}
