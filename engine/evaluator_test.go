package engine

import (
	"context"
	"testing"
	"time"
)

func testTxn() Transaction {
	return Transaction{
		TxnID: "X1", AccountID: "ACCT-1", Amount: 100,
		Currency: "USD", Country: "US", EventTime: time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC),
	}
}

// 多规则同时命中且结论冲突时，按最高优先级给出唯一定论，并列时按 ID 字典序。
func TestEvaluator_RuleConflictResolvedByPriority(t *testing.T) {
	reg := NewRegistry()
	// 低频规则(REVIEW) 优先级 10，大额(BLOCK) 优先级 20 -> REVIEW 胜出。
	if err := reg.Register(NewVelocityRule("VEL", 10, 10*time.Minute, 1)); err != nil {
		t.Fatal(err)
	}
	if err := reg.Register(NewAmountThresholdRule("AMT", 20, 10000)); err != nil {
		t.Fatal(err)
	}
	e := NewEvaluator(reg, ConservativeDegradePolicy())
	e.now = fixedClock

	txn := testTxn()
	txn.Amount = 20000
	// 预置一笔严格更早的窗口内历史（计数不含本笔自身），使高频规则命中。
	vi := NewVelocityIndex()
	prior := testTxn()
	prior.EventTime = txn.EventTime.Add(-1 * time.Minute)
	vi.Add(prior)
	ec := newEvalContext(vi, NewMemoryBlacklistStore(nil))

	v := e.Evaluate(context.Background(), txn, ec)
	if v.Action != ActionReview {
		t.Fatalf("期望高优先级 VEL 决定 REVIEW，实际 %s", v.Action)
	}
	if len(v.Hits) != 2 {
		t.Fatalf("期望两条命中依据，实际 %d", len(v.Hits))
	}
	if v.Hits[0].RuleID != "VEL" {
		t.Fatalf("命中依据须按优先级稳定排序，首条=%s", v.Hits[0].RuleID)
	}
}

// 优先级相同取规则 ID 字典序较小者的定论，保证确定。
func TestEvaluator_TieBrokenByRuleID(t *testing.T) {
	reg := NewRegistry()
	if err := reg.Register(NewAmountThresholdRule("RULE_Z", 10, 10000)); err != nil {
		t.Fatal(err)
	}
	if err := reg.Register(NewCountryRule("RULE_A", 10, []string{"KP"})); err != nil {
		t.Fatal(err)
	}
	e := NewEvaluator(reg, ConservativeDegradePolicy())
	e.now = fixedClock

	txn := testTxn()
	txn.Amount = 20000
	txn.Country = "KP"
	v := e.Evaluate(context.Background(), txn, newEvalContext(NewVelocityIndex(), NewMemoryBlacklistStore(nil)))
	if v.Action != ActionBlock {
		t.Fatalf("两条 BLOCK 规则结论一致，实际 %s", v.Action)
	}
	if v.Hits[0].RuleID != "RULE_A" {
		t.Fatalf("并列优先级期望 RULE_A 在前，实际 %s", v.Hits[0].RuleID)
	}
}

// 黑名单命中（最高优先级）压过大额与高频。
func TestEvaluator_BlacklistWins(t *testing.T) {
	reg := NewRegistry()
	if err := reg.Register(NewBlacklistRule("BL", 10)); err != nil {
		t.Fatal(err)
	}
	if err := reg.Register(NewAmountThresholdRule("AMT", 20, 10000)); err != nil {
		t.Fatal(err)
	}
	e := NewEvaluator(reg, ConservativeDegradePolicy())
	e.now = fixedClock

	txn := testTxn()
	txn.Amount = 20000
	ec := newEvalContext(NewVelocityIndex(), NewMemoryBlacklistStore([]string{"ACCT-1"}))
	v := e.Evaluate(context.Background(), txn, ec)
	if v.Action != ActionBlock {
		t.Fatalf("期望 BLOCK，实际 %s", v.Action)
	}
	if v.Hits[0].RuleID != "BL" {
		t.Fatalf("期望黑名单规则为决定性命中，实际 %s", v.Hits[0].RuleID)
	}
}

// 全部正常且未命中 -> 放行。
func TestEvaluator_AllowWhenNoHit(t *testing.T) {
	reg := NewRegistry()
	if err := reg.Register(NewBlacklistRule("BL", 10)); err != nil {
		t.Fatal(err)
	}
	e := NewEvaluator(reg, ConservativeDegradePolicy())
	e.now = fixedClock
	v := e.Evaluate(context.Background(), testTxn(),
		newEvalContext(NewVelocityIndex(), NewMemoryBlacklistStore(nil)))
	if v.Action != ActionAllow || v.Degraded {
		t.Fatalf("期望正常放行，实际 action=%s degraded=%t", v.Action, v.Degraded)
	}
}

// 降级三原因必须可区分。
func TestEvaluator_DegradationReasons(t *testing.T) {
	policy := ConservativeDegradePolicy()

	// 1) 字段非法 -> INVALID_FIELD，策略拦截。
	e := NewEvaluator(NewRegistry(), policy)
	e.now = fixedClock
	bad := testTxn()
	bad.Amount = -5
	v := e.Evaluate(context.Background(), bad, nil)
	if !v.Degraded || v.Degradation.Reason != DegradeInvalidField || v.Action != ActionBlock {
		t.Fatalf("字段非法降级不符: %+v", v)
	}

	// 2) 规则缺失（空注册表）-> RULE_MISSING，策略人工复核。
	e = NewEvaluator(NewRegistry(), policy)
	e.now = fixedClock
	v = e.Evaluate(context.Background(), testTxn(), nil)
	if !v.Degraded || v.Degradation.Reason != DegradeRuleMissing || v.Action != ActionReview {
		t.Fatalf("规则缺失降级不符: %+v", v)
	}

	// 3) 内部环节失败 -> DEPENDENCY_FAILED，策略人工复核。
	reg := NewRegistry()
	if err := reg.Register(NewBlacklistRule("BL", 10)); err != nil {
		t.Fatal(err)
	}
	e = NewEvaluator(reg, policy)
	e.now = fixedClock
	v = e.Evaluate(context.Background(), testTxn(),
		newEvalContext(NewVelocityIndex(), &FailBlacklistStore{}))
	if !v.Degraded || v.Degradation.Reason != DegradeDependencyFailed || v.Action != ActionReview {
		t.Fatalf("依赖失败降级不符: %+v", v)
	}
	if v.Degradation.RuleID != "BL" {
		t.Fatalf("降级须记录来源规则，实际 %q", v.Degradation.RuleID)
	}
}

// 高优先级规则正常命中时，低优先级规则的依赖失败不得改变定论，
// 但也不得静默：Verdict 仍记录命中依据。
func TestEvaluator_HitOverridesLowerDegradation(t *testing.T) {
	reg := NewRegistry()
	if err := reg.Register(NewAmountThresholdRule("AMT", 10, 10000)); err != nil {
		t.Fatal(err)
	}
	if err := reg.Register(NewBlacklistRule("BL", 20)); err != nil {
		t.Fatal(err)
	}
	e := NewEvaluator(reg, ConservativeDegradePolicy())
	e.now = fixedClock
	txn := testTxn()
	txn.Amount = 20000
	v := e.Evaluate(context.Background(), txn,
		newEvalContext(NewVelocityIndex(), &FailBlacklistStore{}))
	if v.Action != ActionBlock {
		t.Fatalf("高优先级大额命中应决定 BLOCK，实际 %s", v.Action)
	}
	if v.Degraded {
		t.Fatalf("存在正常命中时不应整体标记降级")
	}
}

func fixedClock() time.Time { return time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC) }
