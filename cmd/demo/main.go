// Command demo runs a small in-memory scenario through the fraud-alert
// pipeline, including duplicate, out-of-order, late and degraded events.
package main

import (
	"context"
	"fmt"
	"os"
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
	logger := logging.NewStdLogger(os.Stdout)
	reg := rule.NewDefaultRegistry()
	eng := engine.New(reg, logger)
	store := alert.New(logger)
	p := processor.New(eng, store, 30*time.Second, logger)

	base := time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)
	tx := func(id string, at time.Time, acct, country string, amount float64) domain.Transaction {
		return domain.Transaction{
			EventID:    "E-" + id,
			TxID:       "T-" + id,
			AccountID:  acct,
			Amount:     amount,
			Currency:   "USD",
			Country:    country,
			EventType:  "PAY",
			OccurredAt: at,
			IngestedAt: time.Now(),
		}
	}

	run := func(label string, t domain.Transaction) domain.Verdict {
		v := p.Process(t)
		fmt.Printf("\n>>> %s\n    event=%s verdict=%s winner=%v degraded=%v failure=%s duplicate=%v late=%v alert=%s\n",
			label, t.EventID, v.Action, winner(v), v.Degraded, v.Failure, v.IsDuplicate, v.IsLate, v.AlertKey)
		return v
	}

	// 1) Normal events: no rule fires.
	run("normal low-risk payment", tx("001", base, "acct-A", "US", 50))

	// 2) Velocity: 4 events of one account within 5 minutes (limit 3).
	run("velocity #1", tx("010", base.Add(10*time.Second), "acct-B", "US", 10))
	run("velocity #2", tx("011", base.Add(20*time.Second), "acct-B", "US", 10))
	run("velocity #3", tx("012", base.Add(30*time.Second), "acct-B", "US", 10))
	v013 := run("velocity #4 -> REVIEW", tx("013", base.Add(40*time.Second), "acct-B", "US", 10))

	// 3) Conflict: high-risk country (prio 100) vs large amount (prio 200),
	//    both BLOCK, but the winner and its basis must be deterministic.
	run("conflict country vs amount", tx("020", base.Add(2*time.Minute), "acct-C", "ZZ", 50000))

	// 4) Cluster aggregation: two transactions sharing one cluster id fold
	//    into a single cluster alert.
	c1 := tx("030", base.Add(3*time.Minute), "acct-D", "ZZ", 20)
	c1.ClusterID = "CL-77"
	run("cluster member 1", c1)
	c2 := tx("031", base.Add(3*time.Minute+5*time.Second), "acct-D", "ZZ", 30)
	c2.ClusterID = "CL-77"
	run("cluster member 2 -> merged", c2)

	// 5) Duplicate delivery: same EventID again -> identical verdict,
	//    counted once.
	run("duplicate replay of velocity #4", tx("013", base.Add(40*time.Second), "acct-B", "US", 10))

	// 6) Out-of-order delivery: an older event arriving after newer ones is
	//    still evaluated deterministically.
	run("out-of-order older event", tx("009", base.Add(5*time.Second), "acct-A", "US", 60))

	// 7) Late event: far behind the watermark, flagged late but processed.
	run("late event", tx("000", base.Add(-10*time.Minute), "acct-A", "US", 70))

	// 8) Degradation: invalid field -> manual review, never silent allow.
	bad := tx("040", base.Add(4*time.Minute), "acct-E", "US", -5)
	run("invalid amount -> degraded REVIEW", bad)

	// 9) State machine: acknowledge then resolve the velocity alert;
	//    repeated/out-of-order transitions must be rejected.
	if v013.AlertKey != "" {
		if _, ok := store.Resolve(v013.AlertKey, time.Now()); ok {
			fmt.Println(">>> resolved velocity alert directly (OPEN -> RESOLVED)")
		}
		if _, ok := store.Ack(v013.AlertKey, time.Now()); !ok {
			fmt.Println(">>> ack after resolve rejected as expected (no state rollback)")
		}
		if _, ok := store.Resolve(v013.AlertKey, time.Now()); !ok {
			fmt.Println(">>> duplicate resolve rejected as expected")
		}
	}

	fmt.Println("\n===== final alerts (deduped/aggregated) =====")
	totalUniqueHits := 0
	for _, a := range p.Alerts() {
		totalUniqueHits += a.HitCount
		fmt.Printf("key=%-42s rule=%-24s account=%s state=%-8s unique_hits=%d reasons=%v\n",
			a.Key, a.RuleID, a.AccountID, a.State, a.HitCount, a.Reasons)
	}
	fmt.Printf("sum(unique_hits across alerts)=%d; number of alerts=%d\n",
		totalUniqueHits, len(p.Alerts()))

	// 10) Historical batch backfill: an unordered, duplicate-laden chunk of
	//     old transactions is spilled to disk, replayed in business-time
	//     order and merged with the live pipeline; a second run of the same
	//     session is a no-op (resumable, nothing re-counted).
	dir, err := os.MkdirTemp("", "backfill-demo")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)
	bf := backfill.New(p, backfill.Options{Dir: dir, Logger: logger})
	sess, err := bf.Begin("demo-history")
	if err != nil {
		panic(err)
	}
	hist := []domain.Transaction{
		tx("H-003", base.Add(-30*24*time.Hour), "acct-H", "ZZ", 500), // old block
		tx("H-001", base.Add(-32*24*time.Hour), "acct-H", "US", 60000),
		tx("H-002", base.Add(-31*24*time.Hour), "acct-H", "US", 25),
		tx("H-003", base.Add(-30*24*time.Hour), "acct-H", "ZZ", 500), // in-batch duplicate
		tx("013", base.Add(40*time.Second), "acct-B", "US", 10),      // already processed: absorbed
	}
	if err := sess.Submit(hist); err != nil {
		panic(err)
	}
	st, err := sess.Run(context.Background())
	if err != nil {
		panic(err)
	}
	fmt.Printf("\n>>> backfill run #1: submitted=%d applied=%d skipped=%d done=%v\n",
		st.Submitted, st.Applied, st.Skipped, st.Done)
	for _, id := range []string{"E-H-001", "E-H-002", "E-H-003"} {
		if v, ok := p.Verdict(id); ok {
			fmt.Printf("    backfilled event=%s verdict=%s late=%v alert=%s\n", id, v.Action, v.IsLate, v.AlertKey)
		}
	}
	st2, err := sess.Run(context.Background())
	if err != nil {
		panic(err)
	}
	fmt.Printf(">>> backfill run #2 (resume after completion): applied=%d done=%v (no re-count)\n",
		st2.Applied, st2.Done)

	fmt.Println("\n===== alerts after backfill =====")
	for _, a := range p.Alerts() {
		fmt.Printf("key=%-42s rule=%-24s account=%s state=%-8s unique_hits=%d first=%s last=%s\n",
			a.Key, a.RuleID, a.AccountID, a.State, a.HitCount,
			a.FirstSeen.Format(time.RFC3339), a.LastSeen.Format(time.RFC3339))
	}

	// 11) Interrupted backfill + restart-from-empty resume: run half of a
	//     bursty historical batch, then throw away the whole component
	//     (fresh registry/engine/store/processor = "new process"), resume
	//     the same session id and show the final verdicts/counts equal the
	//     uninterrupted reference.
	fmt.Println("\n===== restart-from-empty resume demo =====")
	rdir, err := os.MkdirTemp("", "backfill-restart-demo")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(rdir)
	newProc := func() *processor.Processor {
		lg := logging.NewStdLogger(os.Stdout)
		return processor.New(engine.New(rule.NewDefaultRegistry(), lg), alert.New(lg), time.Minute, lg)
	}
	rbase := time.Date(2026, 1, 15, 0, 0, 0, 0, time.UTC)
	mkHist := func(id string, i int) domain.Transaction {
		return domain.Transaction{
			EventID: "R-" + id, TxID: "TR-" + id, AccountID: "acct-restart",
			Amount: 10, Currency: "USD", Country: "US", EventType: "PAY",
			OccurredAt: rbase.Add(time.Duration(i) * time.Minute), IngestedAt: time.Now(),
		}
	}
	var rbatch []domain.Transaction
	for i := 0; i < 8; i++ { // 8 events within 7 minutes: velocity breach from #4 on
		rbatch = append(rbatch, mkHist(fmt.Sprintf("%02d", i), i))
	}
	rbatch = append(rbatch, rbatch[2]) // one in-batch duplicate

	// Reference: uninterrupted one-shot business-time processing.
	ref := newProc()
	refVerdict := map[string]domain.Action{}
	for i := 0; i < 8; i++ {
		refVerdict[rbatch[i].EventID] = ref.Process(rbatch[i]).Action
	}

	// Phase 1: process the first 4, then "kill the process".
	p1 := newProc()
	s1, err := backfill.New(p1, backfill.Options{Dir: rdir, Logger: logging.NewStdLogger(os.Stdout)}).Begin("restart-demo")
	if err != nil {
		panic(err)
	}
	if err := s1.Submit(rbatch); err != nil {
		panic(err)
	}
	s1.SetStopHook(func(applied int64) bool { return applied >= 4 })
	rst1, err := s1.Run(context.Background())
	if err != nil {
		panic(err)
	}
	fmt.Printf(">>> phase1 stopped at applied=%d; component now discarded (simulated process exit)\n", rst1.Applied)

	// Phase 2: brand-new empty component, same session dir, run to done.
	p2 := newProc()
	s2, err := backfill.New(p2, backfill.Options{Dir: rdir, Logger: logging.NewStdLogger(os.Stdout)}).Begin("restart-demo")
	if err != nil {
		panic(err)
	}
	rst2, err := s2.Run(context.Background())
	if err != nil {
		panic(err)
	}
	fmt.Printf(">>> phase2 resumed into EMPTY component: applied=%d skipped=%d done=%v\n",
		rst2.Applied, rst2.Skipped, rst2.Done)
	allMatch := true
	for i := 0; i < 8; i++ {
		id := rbatch[i].EventID
		got, ok := p2.Verdict(id)
		mark := "ok"
		if !ok || got.Action != refVerdict[id] {
			mark = "MISMATCH"
			allMatch = false
		}
		fmt.Printf("    event=%s restarted=%v sequential=%v %s\n", id, got.Action, refVerdict[id], mark)
	}
	fmt.Printf(">>> restart resume identical to one-shot sequential: %v (alerts=%d)\n",
		allMatch, len(p2.Alerts()))
}

func winner(v domain.Verdict) string {
	if v.WinningHit == nil {
		return "-"
	}
	return v.WinningHit.RuleID
}
