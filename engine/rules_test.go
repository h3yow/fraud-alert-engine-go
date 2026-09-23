package engine

import (
	"context"
	"testing"
	"time"
)

func TestAmountThresholdRule(t *testing.T) {
	r := NewAmountThresholdRule("AMT", 20, 10000)
	txn := testTxn()
	ec := newEvalContext(NewVelocityIndex(), NewMemoryBlacklistStore(nil))

	hit := r.Evaluate(context.Background(), withAmount(txn, 20000), ec)
	if !hit.Hit || hit.RiskScore != 0.9 {
		t.Fatalf("达阈值应命中: %+v", hit)
	}
	miss := r.Evaluate(context.Background(), withAmount(txn, 9999), ec)
	if miss.Hit {
		t.Fatalf("未达阈值不应命中: %+v", miss)
	}
	if r.Severity() != ActionBlock {
		t.Fatal("大额规则严重度应为 BLOCK")
	}
}

func TestCountryRule(t *testing.T) {
	r := NewCountryRule("CTY", 30, []string{"KP", "XX"})
	ec := newEvalContext(NewVelocityIndex(), NewMemoryBlacklistStore(nil))
	txn := testTxn()

	hit := r.Evaluate(context.Background(), withCountry(txn, "KP"), ec)
	if !hit.Hit {
		t.Fatalf("高风险地区应命中: %+v", hit)
	}
	miss := r.Evaluate(context.Background(), withCountry(txn, "US"), ec)
	if miss.Hit {
		t.Fatalf("普通地区不应命中")
	}
}

func TestBlacklistRule_DistinguishesFailureFromAbsence(t *testing.T) {
	r := NewBlacklistRule("BL", 10)
	txn := testTxn()

	// 正常不在名单：未命中、非降级。
	okEC := newEvalContext(NewVelocityIndex(), NewMemoryBlacklistStore(nil))
	res := r.Evaluate(context.Background(), txn, okEC)
	if res.Hit || res.Degraded {
		t.Fatalf("不在名单应正常未命中: %+v", res)
	}
	// 命中名单。
	hitEC := newEvalContext(NewVelocityIndex(), NewMemoryBlacklistStore([]string{"ACCT-1"}))
	res = r.Evaluate(context.Background(), txn, hitEC)
	if !res.Hit || r.Severity() != ActionBlock {
		t.Fatalf("名单命中应为 BLOCK: %+v", res)
	}
	// 存储失败：必须标记降级且原因可区分，不得当成“不在名单”。
	failEC := newEvalContext(NewVelocityIndex(), &FailBlacklistStore{})
	res = r.Evaluate(context.Background(), txn, failEC)
	if res.Hit || !res.Degraded || res.Degradation.Reason != DegradeDependencyFailed {
		t.Fatalf("存储失败必须降级为 DEPENDENCY_FAILED: %+v", res)
	}
}

func TestVelocityRule_StrictlyPriorCount(t *testing.T) {
	base := time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)
	vi := NewVelocityIndex()
	// 两笔更早历史。
	for i := 1; i <= 2; i++ {
		p := testTxn()
		p.TxnID = "P" + string(rune('0'+i))
		p.EventTime = base.Add(time.Duration(i) * time.Minute)
		vi.Add(p)
	}
	r := NewVelocityRule("VEL", 40, 10*time.Minute, 2)
	cur := testTxn()
	cur.EventTime = base.Add(5 * time.Minute)
	ec := newEvalContext(vi, NewMemoryBlacklistStore(nil))
	res := r.Evaluate(context.Background(), cur, ec)
	if !res.Hit || r.Severity() != ActionReview {
		t.Fatalf("本笔之前已有 2 笔，应命中 REVIEW: %+v", res)
	}

	// 仅 1 笔更早历史 -> 未命中。
	vi2 := NewVelocityIndex()
	one := testTxn()
	one.EventTime = base.Add(1 * time.Minute)
	vi2.Add(one)
	ec2 := newEvalContext(vi2, NewMemoryBlacklistStore(nil))
	if r.Evaluate(context.Background(), cur, ec2).Hit {
		t.Fatal("本笔之前仅 1 笔不应命中")
	}
}

func TestRegistry_OrderingAndDuplicate(t *testing.T) {
	reg := NewRegistry()
	a := NewBlacklistRule("RULE_A", 20)
	b := NewBlacklistRule("RULE_B", 10)
	c := NewBlacklistRule("RULE_C", 10)
	for _, rule := range []Rule{a, b, c} {
		if err := reg.Register(rule); err != nil {
			t.Fatal(err)
		}
	}
	if err := reg.Register(a); err != ErrDuplicateRule {
		t.Fatalf("重复注册应返回 ErrDuplicateRule，实际 %v", err)
	}
	got := reg.Ordered()
	want := []string{"RULE_B", "RULE_C", "RULE_A"} // 优先级升序，并列按 ID 字典序
	for i, w := range want {
		if got[i].ID() != w {
			t.Fatalf("稳定排序位置 %d 期望 %s 实际 %s", i, w, got[i].ID())
		}
	}
	if _, ok := reg.Get("RULE_A"); !ok {
		t.Fatal("Get 应能找到已注册规则")
	}
	if id, ok := reg.MustIDs(context.Background(), []string{"RULE_A", "GHOST"}); ok || id != "GHOST" {
		t.Fatalf("MustIDs 应报告首个缺失规则，实际 id=%s ok=%t", id, ok)
	}
}

func withAmount(txn Transaction, amount float64) Transaction  { txn.Amount = amount; return txn }
func withCountry(txn Transaction, country string) Transaction { txn.Country = country; return txn }
