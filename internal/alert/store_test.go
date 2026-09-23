package alert

import (
	"testing"
	"time"

	"github.com/h3yow/fraud-alert-engine-go/internal/domain"
)

func baseTx(event, acct, cluster string, at time.Time) domain.Transaction {
	return domain.Transaction{
		EventID: event, TxID: "T" + event, AccountID: acct, ClusterID: cluster,
		Amount: 1, Currency: "USD", Country: "US", OccurredAt: at,
	}
}

func blockHit(ruleID string) *domain.Hit {
	return &domain.Hit{RuleID: ruleID, Priority: 100, Action: domain.ActionBlock, Reason: "r-" + ruleID}
}

func TestRecordDedupCountConservation(t *testing.T) {
	s := New(&loggingStub{t: t})
	at := time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)
	hit := blockHit("R1")

	// Three unique events, then every event delivered two more times.
	events := []string{"e1", "e2", "e3"}
	for _, id := range events {
		if _, dup := s.Record(hit, baseTx(id, "acct", "", at), false); dup {
			t.Fatalf("first delivery of %s must not be duplicate", id)
		}
	}
	for round := 0; round < 2; round++ {
		for _, id := range events {
			if _, dup := s.Record(hit, baseTx(id, "acct", "", at), true); !dup {
				t.Fatalf("repeat delivery of %s must be duplicate", id)
			}
		}
	}
	snap := s.Snapshot()
	if len(snap) != 1 {
		t.Fatalf("same (rule, account, bucket) must aggregate to 1 alert, got %d", len(snap))
	}
	if snap[0].HitCount != 3 || len(snap[0].EventIDs) != 3 {
		t.Fatalf("count conservation violated: HitCount=%d EventIDs=%d, want 3/3",
			snap[0].HitCount, len(snap[0].EventIDs))
	}
}

func TestCooldownBucketing(t *testing.T) {
	cooldown := 10 * time.Minute
	s := NewWithCooldown(nil, cooldown)
	// Buckets truncate to absolute 10-minute boundaries; pick a boundary
	// instant so same-bucket vs next-bucket is unambiguous.
	boundary := time.Unix(1790157600, 0).UTC() // 10:00:00 UTC, aligned
	at := boundary.Add(5 * time.Minute)        // 10:05, bucket 10:00
	k1 := DedupKey(blockHit("R1"), baseTx("e1", "a", "", at), cooldown)
	k2 := DedupKey(blockHit("R1"), baseTx("e2", "a", "", at.Add(time.Minute)), cooldown)   // 10:06 same bucket
	k3 := DedupKey(blockHit("R1"), baseTx("e3", "a", "", at.Add(6*time.Minute)), cooldown) // 10:11 next bucket
	if k1 != k2 {
		t.Fatal("events truncating to the same bucket must share a key")
	}
	if k2 == k3 {
		t.Fatal("events truncating to different buckets must not share a key")
	}
	// The store must use exactly the same keying rule.
	if a, _ := s.Record(blockHit("R1"), baseTx("e1", "a", "", at), false); a.Key != k1 {
		t.Fatalf("store key %s != computed key %s", a.Key, k1)
	}
}

func TestClusterMergesAcrossRules(t *testing.T) {
	s := New(nil)
	at := time.Now()
	a1, d1 := s.Record(blockHit("R1"), baseTx("e1", "acct", "CL-9", at), false)
	a2, d2 := s.Record(blockHit("R2"), baseTx("e2", "acct", "CL-9", at.Add(time.Second)), false)
	if d1 || d2 {
		t.Fatal("first two unique cluster events are not duplicates")
	}
	if a1.Key != a2.Key || a2.HitCount != 2 {
		t.Fatalf("cluster events must merge: keys=%s/%s count=%d", a1.Key, a2.Key, a2.HitCount)
	}
}

func TestStateMachineOneWay(t *testing.T) {
	s := New(nil)
	at := time.Now()
	a, _ := s.Record(blockHit("R1"), baseTx("e1", "a", "", at), false)

	if a.State != domain.StateOpen {
		t.Fatal("new alert must be OPEN")
	}
	if _, ok := s.Resolve("missing", at); ok {
		t.Fatal("resolving a missing key must fail")
	}
	// OPEN -> ACKED
	got, ok := s.Ack(a.Key, at)
	if !ok || got.State != domain.StateAcked {
		t.Fatalf("OPEN must ack to ACKED, ok=%v state=%s", ok, got.State)
	}
	// ACKED -> ACKED rejected (no rollback / no duplicate transition)
	if _, ok := s.Ack(a.Key, at); ok {
		t.Fatal("repeated ack must be rejected")
	}
	// ACKED -> RESOLVED
	if got, ok := s.Resolve(a.Key, at); !ok || got.State != domain.StateResolved {
		t.Fatalf("ACKED must resolve, ok=%v state=%s", ok, got.State)
	}
	// RESOLVED terminal: resolve again and ack both rejected
	if _, ok := s.Resolve(a.Key, at); ok {
		t.Fatal("resolve on RESOLVED must be rejected (terminal)")
	}
	if _, ok := s.Ack(a.Key, at); ok {
		t.Fatal("ack on RESOLVED must be rejected (no backward flow)")
	}
	final, _ := s.Get(a.Key)
	if final.State != domain.StateResolved {
		t.Fatalf("state must remain RESOLVED, got %s", final.State)
	}
}

// loggingStub satisfies the store logger without capturing output.
type loggingStub struct{ t *testing.T }

func (loggingStub) Printf(string, ...any) {}
