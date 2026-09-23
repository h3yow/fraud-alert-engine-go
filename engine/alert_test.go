package engine

import (
	"context"
	"testing"
	"time"
)

func vBlock(txnID string) *Verdict {
	return &Verdict{TxnID: txnID, Action: ActionBlock, Hits: []RuleHit{
		{RuleID: "R1", Priority: 10, Reason: "r", RiskScore: 0.9},
	}}
}

func TestAlertStore_StateMachineOneWay(t *testing.T) {
	s := NewAlertStore()
	now := time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)
	txn := Transaction{TxnID: "X", AccountID: "A", EventTime: now}

	a, created := s.Record(vBlock("X"), txn, "k", now)
	if !created || a.Status != StatusOpen || a.Version != 1 {
		t.Fatalf("新建告警状态异常: %+v", a)
	}

	// OPEN -> IN_REVIEW 合法。
	a, err := s.Transition(context.Background(), "k", StatusInReview, now)
	if err != nil || a.Status != StatusInReview || a.Version != 2 {
		t.Fatalf("OPEN->IN_REVIEW 失败: %v %+v", err, a)
	}
	// 重复流转同一状态：明确拒绝、版本不变。
	if _, err := s.Transition(context.Background(), "k", StatusInReview, now); err != ErrDuplicateTransition {
		t.Fatalf("期望 ErrDuplicateTransition，实际 %v", err)
	}
	// 回退 IN_REVIEW -> OPEN：明确拒绝。
	if _, err := s.Transition(context.Background(), "k", StatusOpen, now); err != ErrIllegalTransition {
		t.Fatalf("期望 ErrIllegalTransition，实际 %v", err)
	}
	// IN_REVIEW -> RESOLVED 合法。
	a, err = s.Transition(context.Background(), "k", StatusResolved, now)
	if err != nil || a.Status != StatusResolved || a.Version != 3 {
		t.Fatalf("IN_REVIEW->RESOLVED 失败: %v %+v", err, a)
	}
	// RESOLVED 为终态：任何流转都拒绝，状态不回退。
	if _, err := s.Transition(context.Background(), "k", StatusInReview, now); err != ErrIllegalTransition {
		t.Fatalf("终态回退应被拒绝，实际 %v", err)
	}
	if _, err := s.Transition(context.Background(), "k", StatusOpen, now); err != ErrIllegalTransition {
		t.Fatalf("终态回退 OPEN 应被拒绝，实际 %v", err)
	}
	// 未知键（迟到/乱序命令引用不存在的簇）：独立错误原因。
	if _, err := s.Transition(context.Background(), "missing", StatusInReview, now); err != ErrAlertNotFound {
		t.Fatalf("期望 ErrAlertNotFound，实际 %v", err)
	}
}

// 合法流转边的穷举校验。
func TestCanTransition(t *testing.T) {
	cases := []struct {
		from, to AlertStatus
		want     bool
	}{
		{StatusOpen, StatusInReview, true},
		{StatusOpen, StatusResolved, true},
		{StatusOpen, StatusOpen, false},
		{StatusInReview, StatusResolved, true},
		{StatusInReview, StatusOpen, false},
		{StatusInReview, StatusInReview, false},
		{StatusResolved, StatusOpen, false},
		{StatusResolved, StatusInReview, false},
		{StatusResolved, StatusResolved, false},
	}
	for _, c := range cases {
		if got := CanTransition(c.from, c.to); got != c.want {
			t.Fatalf("CanTransition(%s->%s)=%v want %v", c.from, c.to, got, c.want)
		}
	}
}

// 幂等 upsert：ALLOW 首次不建告警；同一交易重评为可疑时补建且不重复计数；
// 已计入成员重复 Record 不增加 Count，Count 与 TxnIDs 守恒。
func TestAlertStore_IdempotentUpsertAndConservation(t *testing.T) {
	s := NewAlertStore()
	now := time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)
	txn := Transaction{TxnID: "X", AccountID: "A", EventTime: now}

	// 首次 ALLOW：无告警。
	if a, created := s.Record(&Verdict{TxnID: "X", Action: ActionAllow}, txn, "k", now); a != nil || created {
		t.Fatal("ALLOW 不应产生告警")
	}
	// 同一笔重评为 REVIEW：补建告警，计数 1。
	rev := &Verdict{TxnID: "X", Action: ActionReview}
	a, created := s.Record(rev, txn, "k", now)
	if !created || a.Count != 1 || a.Severity != ActionReview {
		t.Fatalf("重评补建异常: created=%t %+v", created, a)
	}
	// 再次 Record 同笔（模拟重评循环）：计数不增加。
	if a, _ := s.Record(rev, txn, "k", now); a.Count != 1 {
		t.Fatalf("同笔重复记录导致计数失真: %d", a.Count)
	}
	// 第二笔新交易聚合同簇。
	txn2 := Transaction{TxnID: "Y", AccountID: "A", EventTime: now}
	if a, _ := s.Record(vBlock("Y"), txn2, "k", now); a.Count != 2 || a.Severity != ActionBlock {
		t.Fatalf("聚合/升级异常: %+v", a)
	}
	// 守恒校验。
	a, _ = s.Get("k")
	if a.Count != len(a.TxnIDs) || a.Count != 2 {
		t.Fatalf("计数不守恒: count=%d txnIDs=%d", a.Count, len(a.TxnIDs))
	}
	// 快照不可被外部修改污染内部状态。
	a.TxnIDs[0] = "MUTATED"
	again, _ := s.Get("k")
	if again.TxnIDs[0] == "MUTATED" {
		t.Fatal("Get 必须返回深拷贝快照")
	}
}
