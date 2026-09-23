// Command fraud-demo 用本地交易流水与规则配置模拟实时风控监测：
// 顺序、重复、乱序、迟到事件以及内部环节失败均被覆盖，并演示告警
// 去重聚合与状态单向流转。
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/h3yow/fraud-alert-engine-go/engine"
)

func main() {
	ctx := context.Background()
	logger := log.New(os.Stdout, "", log.LstdFlags|log.Lmicroseconds)
	base := time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)
	now := func() time.Time { return base }

	// 规则配置：Priority 数值越小优先级越高，多规则命中时取最高优先级规则的定论。
	reg := engine.NewRegistry()
	must(reg.Register(engine.NewBlacklistRule("RULE_BLACKLIST", 10)))
	must(reg.Register(engine.NewAmountThresholdRule("RULE_LARGE_AMOUNT", 20, 10000)))
	must(reg.Register(engine.NewCountryRule("RULE_HIGH_RISK_COUNTRY", 30, []string{"KP", "XX"})))
	must(reg.Register(engine.NewVelocityRule("RULE_VELOCITY", 40, 10*time.Minute, 2)))

	bl := engine.NewMemoryBlacklistStore([]string{"ACCT-BLACK"})
	e := engine.NewEngine(reg, engine.Config{
		Blacklist: bl,
		Policy:    engine.ConservativeDegradePolicy(),
		Logger:    logger,
		Now:       now,
	})

	// 本地交易流水：故意混入重复(T2)、乱序(T7 早于 T4 投递)与迟到事件。
	stream := []engine.Transaction{
		{TxnID: "T1", AccountID: "ACCT-A", Amount: 50, Currency: "USD", Country: "US", EventTime: base.Add(1 * time.Minute)},
		{TxnID: "T2", AccountID: "ACCT-A", Amount: 20000, Currency: "USD", Country: "US", EventTime: base.Add(2 * time.Minute)}, // 大额拦截
		{TxnID: "T3", AccountID: "ACCT-B", Amount: 100, Currency: "USD", Country: "KP", EventTime: base.Add(3 * time.Minute)},   // 高风险地区
		{TxnID: "T4", AccountID: "ACCT-V", Amount: 20, Currency: "USD", Country: "US", EventTime: base.Add(4 * time.Minute)},
		{TxnID: "T5", AccountID: "ACCT-V", Amount: 20, Currency: "USD", Country: "US", EventTime: base.Add(5 * time.Minute)},
		{TxnID: "T6", AccountID: "ACCT-V", Amount: 20, Currency: "USD", Country: "US", EventTime: base.Add(6 * time.Minute)},    // 第 3 笔命中高频 -> REVIEW
		{TxnID: "T2", AccountID: "ACCT-A", Amount: 20000, Currency: "USD", Country: "US", EventTime: base.Add(2 * time.Minute)}, // 重复投递，幂等
		// T7 事件时间早于 T4，却在 T6 之后投递（迟到+乱序）；
		// 其判定只依赖它自己时点之前的历史（为空），应为 ALLOW。
		{TxnID: "T7", AccountID: "ACCT-LATE", Amount: 10, Currency: "USD", Country: "US", EventTime: base.Add(0 * time.Minute)},
		{TxnID: "T8", AccountID: "ACCT-BLACK", Amount: 99, Currency: "USD", Country: "US", EventTime: base.Add(7 * time.Minute)}, // 黑名单拦截
		{TxnID: "T9", AccountID: "ACCT-C", Amount: 0, Currency: "USD", Country: "US", EventTime: base.Add(8 * time.Minute)},      // 字段非法 -> 拦截降级
		{TxnID: "T10", AccountID: "ACCT-A", Amount: 50000, Currency: "USD", Country: "KP", EventTime: base.Add(9 * time.Minute)}, // 大额+地区同时命中，取高优先级（大额）
	}

	fmt.Println("================ 流水摄入（含重复/乱序/迟到/非法） ================")
	first := map[string]*engine.Verdict{}
	for _, txn := range stream {
		v, _ := e.Ingest(ctx, txn)
		if _, seen := first[txn.TxnID]; !seen {
			first[txn.TxnID] = v
		}
	}

	// 重复投递 T2：定论对象指针与结论都必须与首次完全一致。
	vT2again, _ := e.Ingest(ctx, stream[1])
	fmt.Printf("\n一致性校验: T2 首次=%s 重复=%s 同一对象=%t\n",
		first["T2"].Action, vT2again.Action, first["T2"] == vT2again)

	fmt.Println("\n================ 告警去重与计数守恒 ================")
	for _, a := range e.Alerts() {
		fmt.Printf("告警 %s status=%s severity=%s count=%d txnIDs=%d 守恒=%t\n",
			a.Key, a.Status, a.Severity, a.Count, len(a.TxnIDs), a.Count == len(a.TxnIDs))
	}

	fmt.Println("\n================ 告警状态单向流转 ================")
	key := "acct:ACCT-V|day:2026-09-23"
	if a, err := e.TransitionAlert(ctx, key, engine.StatusInReview, now()); err != nil {
		logger.Fatal(err)
	} else {
		fmt.Printf("流转后 %s status=%s version=%d\n", key, a.Status, a.Version)
	}
	if _, err := e.TransitionAlert(ctx, key, engine.StatusInReview, now()); err != nil {
		fmt.Printf("重复流转被拒绝（可区分原因）: %v\n", err)
	}
	if _, err := e.TransitionAlert(ctx, key, engine.StatusOpen, now()); err != nil {
		fmt.Printf("回退被拒绝（乱序/非法命令）: %v\n", err)
	}
	if a, err := e.TransitionAlert(ctx, key, engine.StatusResolved, now()); err != nil {
		logger.Fatal(err)
	} else {
		fmt.Printf("流转后 %s status=%s version=%d\n", key, a.Status, a.Version)
	}
	if _, err := e.TransitionAlert(ctx, "acct:UNKNOWN|day:2026-09-23", engine.StatusResolved, now()); err != nil {
		fmt.Printf("未知告警键被拒绝（迟到命令引用未知簇）: %v\n", err)
	}

	fmt.Println("\n================ 内部环节失败 -> 依赖失败降级 ================")
	reg2 := engine.NewRegistry()
	must(reg2.Register(engine.NewBlacklistRule("RULE_BLACKLIST", 10)))
	must(reg2.Register(engine.NewAmountThresholdRule("RULE_LARGE_AMOUNT", 20, 10000)))
	e2 := engine.NewEngine(reg2, engine.Config{
		Blacklist: &engine.FailBlacklistStore{}, // 名单存储始终失败
		Policy:    engine.ConservativeDegradePolicy(),
		Logger:    logger,
		Now:       now,
	})
	v, _ := e2.Ingest(ctx, engine.Transaction{
		TxnID: "D1", AccountID: "ACCT-D", Amount: 100, Currency: "USD",
		Country: "US", EventTime: base.Add(10 * time.Minute),
	})
	fmt.Printf("依赖失败定论 action=%s degraded=%t reason=%s detail=%s\n",
		v.Action, v.Degraded, v.Degradation.Reason, v.Degradation.Detail)
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
