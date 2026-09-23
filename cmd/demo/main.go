// Command demo runs a small in-memory scenario through the fraud-alert
// pipeline, including duplicate, out-of-order, late and degraded events.
package main

import (
	"fmt"
	"os"
	"time"

	"github.com/h3yow/fraud-alert-engine-go/internal/alert"
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
}

func winner(v domain.Verdict) string {
	if v.WinningHit == nil {
		return "-"
	}
	return v.WinningHit.RuleID
}
