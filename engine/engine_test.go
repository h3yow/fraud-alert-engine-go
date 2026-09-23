package engine

import (
	"context"
	"testing"
	"time"
)

func newTestEngine(t *testing.T, bl BlacklistStore) *Engine {
	t.Helper()
	reg := NewRegistry()
	if err := reg.Register(NewBlacklistRule("RULE_BLACKLIST", 10)); err != nil {
		t.Fatal(err)
	}
	if err := reg.Register(NewAmountThresholdRule("RULE_LARGE", 20, 10000)); err != nil {
		t.Fatal(err)
	}
	if err := reg.Register(NewCountryRule("RULE_COUNTRY", 30, []string{"KP"})); err != nil {
		t.Fatal(err)
	}
	if err := reg.Register(NewVelocityRule("RULE_VELOCITY", 40, 10*time.Minute, 2)); err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)
	return NewEngine(reg, Config{
		Blacklist: bl,
		Policy:    ConservativeDegradePolicy(),
		Logger:    NopLogger(),
		Now:       func() time.Time { return base },
		// 宽容忍窗口：测试中的乱序/逆序事件均落在窗口内，即时结论即可收敛。
		LateGrace: time.Hour,
	})
}

// 重复投递：无论重复多少次，返回同一判定对象，且告警计数不增加。
func TestEngine_DuplicateIngestIsIdempotent(t *testing.T) {
	e := newTestEngine(t, NewMemoryBlacklistStore(nil))
	txn := Transaction{TxnID: "D1", AccountID: "ACCT-D", Amount: 20000,
		Currency: "USD", Country: "US", EventTime: time.Date(2026, 9, 23, 10, 1, 0, 0, time.UTC)}

	v1, a1 := e.Ingest(context.Background(), txn)
	for i := 0; i < 5; i++ {
		vi, ai := e.Ingest(context.Background(), txn)
		if vi != v1 {
			t.Fatalf("第 %d 次重复投递得到不同判定对象", i+1)
		}
		if vi.Action != ActionBlock {
			t.Fatalf("重复投递结论漂移: %s", vi.Action)
		}
		if ai.Count != 1 {
			t.Fatalf("重复投递导致计数失真: count=%d", ai.Count)
		}
		if a1.Key != ai.Key {
			t.Fatal("重复投递告警键漂移")
		}
	}
	if len(e.Alerts()) != 1 || e.Alerts()[0].Count != 1 {
		t.Fatal("重复交易不得产生重复告警")
	}
}

// 乱序：同一组事件分别“按顺序”和“完全逆序”喂入两个引擎，
// 待流平息后，每个 TxnID 的最终定论必须一致。
// 乱序投递过程中即时暂定论允许从放行收敛为可疑，但只允许单调增强，
// 绝不允许出现相互矛盾的回退。
func TestEngine_OutOfOrderSameVerdict(t *testing.T) {
	mkTxns := func() []Transaction {
		base := time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)
		return []Transaction{
			{TxnID: "V1", AccountID: "ACCT-V", Amount: 10, Currency: "USD", Country: "US", EventTime: base.Add(1 * time.Minute)},
			{TxnID: "V2", AccountID: "ACCT-V", Amount: 10, Currency: "USD", Country: "US", EventTime: base.Add(2 * time.Minute)},
			{TxnID: "V3", AccountID: "ACCT-V", Amount: 10, Currency: "USD", Country: "US", EventTime: base.Add(3 * time.Minute)},
			{TxnID: "V4", AccountID: "ACCT-V", Amount: 10, Currency: "USD", Country: "US", EventTime: base.Add(4 * time.Minute)},
		}
	}

	txns := mkTxns()

	// 按序引擎：记录最终定论，并验证按序流下即时结论单调。
	ordered := newTestEngine(t, NewMemoryBlacklistStore(nil))
	inOrder := map[string]Action{}
	for _, txn := range txns {
		v, _ := ordered.Ingest(context.Background(), txn)
		inOrder[txn.TxnID] = v.Action
	}

	// 逆序引擎：流平息后比对最终缓存定论；同时断言同一 TxnID 被后续事件
	// 触发重评时，结论只会 ALLOW -> REVIEW/BLOCK（单调增强），不回退。
	reversed := newTestEngine(t, NewMemoryBlacklistStore(nil))
	rank := map[Action]int{ActionAllow: 0, ActionReview: 1, ActionBlock: 2}
	last := map[string]Action{}
	for i := len(txns) - 1; i >= 0; i-- {
		reversed.Ingest(context.Background(), txns[i])
		for id, p := range reversed.pending {
			if prev, seen := last[id]; seen && rank[p.verdict.Action] < rank[prev] {
				t.Fatalf("交易 %s 暂定论发生回退: %s -> %s", id, prev, p.verdict.Action)
			}
			last[id] = p.verdict.Action
		}
	}
	for _, txn := range txns {
		got, ok := reversed.lookupVerdict(txn.TxnID)
		if !ok {
			t.Fatalf("交易 %s 缺少最终定论", txn.TxnID)
		}
		if inOrder[txn.TxnID] != got.Action {
			t.Fatalf("交易 %s 顺序最终结论 %s 与逆序最终结论 %s 不一致",
				txn.TxnID, inOrder[txn.TxnID], got.Action)
		}
	}
	// 期望的确定结论：V1/V2 放行，V3/V4 高频复核（阈值：本笔之前 >=2 笔）。
	want := map[string]Action{"V1": ActionAllow, "V2": ActionAllow, "V3": ActionReview, "V4": ActionReview}
	for id, a := range want {
		if got, _ := reversed.lookupVerdict(id); got.Action != a {
			t.Fatalf("交易 %s 期望 %s，实际 %s", id, a, got.Action)
		}
	}
}

// 迟到事件：事件时间更早的交易在后续才到达，其判定不得看到“未来”历史；
// 且速度规则恰好在第 3 笔触发，结论可解释。
func TestEngine_LateEventDoesNotSeeFuture(t *testing.T) {
	e := newTestEngine(t, NewMemoryBlacklistStore(nil))
	base := time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)
	ingest := func(id string, m time.Duration, amount float64, country string) *Verdict {
		v, _ := e.Ingest(context.Background(), Transaction{
			TxnID: id, AccountID: "ACCT-L", Amount: amount, Currency: "USD",
			Country: country, EventTime: base.Add(m)})
		return v
	}
	// 先投递 10:03、10:04 两笔正常。
	ingest("L3", 3*time.Minute, 10, "US")
	ingest("L4", 4*time.Minute, 10, "US")
	// 迟到的 10:00 事件：此前该时点无历史，必须放行。
	late := ingest("L0", 0, 10, "US")
	if late.Action != ActionAllow {
		t.Fatalf("迟到事件不得受未来事件影响，实际 %s", late.Action)
	}
	// 10:05 的事件在 10 分钟窗口内看到 L3/L4 两笔 -> 高频命中 REVIEW。
	hit := ingest("L5", 5*time.Minute, 10, "US")
	if hit.Action != ActionReview {
		t.Fatalf("第 3 笔窗口内交易应命中高频 REVIEW，实际 %s", hit.Action)
	}
	if len(hit.Hits) != 1 || hit.Hits[0].RuleID != "RULE_VELOCITY" {
		t.Fatalf("高频命中依据不符: %+v", hit.Hits)
	}
}

// IngestBatch 对乱序批自动按事件时间排序，返回与输入位置一一对应的结论。
func TestEngine_BatchOrderingDeterministic(t *testing.T) {
	base := time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)
	txns := []Transaction{
		{TxnID: "B4", AccountID: "ACCT-B", Amount: 10, Currency: "USD", Country: "US", EventTime: base.Add(4 * time.Minute)},
		{TxnID: "B1", AccountID: "ACCT-B", Amount: 10, Currency: "USD", Country: "US", EventTime: base.Add(1 * time.Minute)},
		{TxnID: "B3", AccountID: "ACCT-B", Amount: 10, Currency: "USD", Country: "US", EventTime: base.Add(3 * time.Minute)},
		{TxnID: "B2", AccountID: "ACCT-B", Amount: 10, Currency: "USD", Country: "US", EventTime: base.Add(2 * time.Minute)},
	}
	e := newTestEngine(t, NewMemoryBlacklistStore(nil))
	vs := e.IngestBatch(context.Background(), txns)
	if len(vs) != len(txns) {
		t.Fatal("返回切片必须与输入位置一一对应")
	}
	if vs[0].TxnID != "B4" || vs[1].TxnID != "B1" {
		t.Fatal("批量返回顺序被破坏")
	}
	// 排序摄入 B1->B4：宽容忍窗口下，B1 虽最晚被处理，但其到达会重评
	// 窗口内待决事件——B2/B3/B4 最终都看到各自时点之前的完整历史：
	// B2(第2笔)=ALLOW，B3/B4(>=3笔)=REVIEW。
	if vs[1].Action != ActionAllow {
		t.Fatalf("B2 应为 ALLOW，实际 %s", vs[1].Action)
	}
	if vs[2].Action != ActionReview {
		t.Fatalf("B3 应命中高频 REVIEW，实际 %s", vs[2].Action)
	}
	if vs[0].Action != ActionReview {
		t.Fatalf("B4 应命中高频 REVIEW，实际 %s", vs[0].Action)
	}
	if vs[3].Action != ActionAllow {
		t.Fatalf("B1（最早事件，无前置历史）应为 ALLOW，实际 %s", vs[3].Action)
	}
}

// 去重聚合：同主体同日多笔命中合并为一个告警，Count 与 TxnIDs 守恒；
// 不同主体产生不同告警；重复交易不计数。
func TestEngine_AlertDedupeAndCountConservation(t *testing.T) {
	e := newTestEngine(t, NewMemoryBlacklistStore(nil))
	base := time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)
	mk := func(id, acct string, m time.Duration) Transaction {
		return Transaction{TxnID: id, AccountID: acct, Amount: 20000,
			Currency: "USD", Country: "US", EventTime: base.Add(m)}
	}
	stream := []Transaction{
		mk("A1", "ACCT-A", 1*time.Minute),
		mk("A2", "ACCT-A", 2*time.Minute),
		mk("A1", "ACCT-A", 1*time.Minute), // 重复
		mk("B1", "ACCT-B", 1*time.Minute),
	}
	for _, txn := range stream {
		e.Ingest(context.Background(), txn)
	}
	alerts := e.Alerts()
	if len(alerts) != 2 {
		t.Fatalf("期望 2 个按主体聚合的告警，实际 %d", len(alerts))
	}
	var acctA *Alert
	for _, a := range alerts {
		if a.Count != len(a.TxnIDs) {
			t.Fatalf("告警 %s 计数不守恒 count=%d txnIDs=%d", a.Key, a.Count, len(a.TxnIDs))
		}
		if a.AccountID == "ACCT-A" {
			acctA = a
		}
	}
	if acctA == nil || acctA.Count != 2 {
		t.Fatalf("ACCT-A 应聚合并去重为 2 笔，实际 %+v", acctA)
	}
}

// 严重度单调增强：先 REVIEW 后 BLOCK 的同一簇告警，severity 只能变强不能变弱。
func TestEngine_SeverityMonotonic(t *testing.T) {
	e := newTestEngine(t, NewMemoryBlacklistStore(nil))
	base := time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)
	// 三笔小额触发高频 REVIEW。
	for i, id := range []string{"S1", "S2", "S3"} {
		e.Ingest(context.Background(), Transaction{
			TxnID: id, AccountID: "ACCT-S", Amount: 10, Currency: "USD",
			Country: "US", EventTime: base.Add(time.Duration(i+1) * time.Minute)})
	}
	a, _ := e.Alert(dedupeKey(Transaction{AccountID: "ACCT-S", EventTime: base}))
	if a.Severity != ActionReview {
		t.Fatalf("初始严重度应为 REVIEW，实际 %s", a.Severity)
	}
	// 大额命中并入同一簇 -> 升级 BLOCK。S1/S2 当时放行、未产生告警，
	// 故告警只聚合可疑定论 S3/S4，计数为 2。
	e.Ingest(context.Background(), Transaction{
		TxnID: "S4", AccountID: "ACCT-S", Amount: 50000, Currency: "USD",
		Country: "US", EventTime: base.Add(4 * time.Minute)})
	a, _ = e.Alert(dedupeKey(Transaction{AccountID: "ACCT-S", EventTime: base}))
	if a.Severity != ActionBlock || a.Count != 2 {
		t.Fatalf("严重度应升级 BLOCK 且计数为 2，实际 %s count=%d", a.Severity, a.Count)
	}
}

// 并发重复投递：结论一致、计数守恒（数据竞争由 -race 检测）。
func TestEngine_ConcurrentDuplicates(t *testing.T) {
	e := newTestEngine(t, NewMemoryBlacklistStore(nil))
	txn := Transaction{TxnID: "C1", AccountID: "ACCT-C", Amount: 20000,
		Currency: "USD", Country: "US", EventTime: time.Date(2026, 9, 23, 10, 1, 0, 0, time.UTC)}
	done := make(chan *Verdict, 20)
	for i := 0; i < 20; i++ {
		go func() {
			v, _ := e.Ingest(context.Background(), txn)
			done <- v
		}()
	}
	first := <-done
	for i := 1; i < 20; i++ {
		if (<-done) != first {
			t.Fatal("并发重复投递产生了不同判定")
		}
	}
	if a := e.Alerts(); len(a) != 1 || a[0].Count != 1 {
		t.Fatalf("并发重复投递计数失真: %+v", a)
	}
}

// 超出容忍窗口的迟到事件：立即按其时点已知历史做最终定论，且该定论不可变；
// 后续重复投递返回同一对象。
func TestEngine_LateEventBeyondGraceFinalized(t *testing.T) {
	reg := NewRegistry()
	if err := reg.Register(NewVelocityRule("RULE_VELOCITY", 40, 10*time.Minute, 2)); err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)
	e := NewEngine(reg, Config{
		Blacklist: NewMemoryBlacklistStore(nil),
		Policy:    ConservativeDegradePolicy(),
		Logger:    NopLogger(),
		Now:       func() time.Time { return base },
		LateGrace: 1 * time.Minute,
	})
	ingest := func(id string, m time.Duration) *Verdict {
		v, _ := e.Ingest(context.Background(), Transaction{
			TxnID: id, AccountID: "ACCT-G", Amount: 10, Currency: "USD",
			Country: "US", EventTime: base.Add(m)})
		return v
	}
	// 先推进 watermark 到 10:10。
	ingest("F1", 10*time.Minute)
	ingest("F2", 11*time.Minute)
	// 10:00 的事件迟到超过 1 分钟容忍窗口：其时点无历史 -> 最终 ALLOW。
	late := ingest("OLD", 0)
	if late.Action != ActionAllow {
		t.Fatalf("超窗迟到事件应按其时点历史定论 ALLOW，实际 %s", late.Action)
	}
	// 定论已进入最终缓存，重复投递返回同一对象。
	again, _ := e.Ingest(context.Background(), Transaction{
		TxnID: "OLD", AccountID: "ACCT-G", Amount: 10, Currency: "USD",
		Country: "US", EventTime: base})
	if again != late {
		t.Fatal("超窗迟到事件的最终定论重复投递必须返回同一对象")
	}
}
