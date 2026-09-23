package engine

import (
	"context"
	"fmt"
	"time"
)

// ---- 大额交易阈值规则 ----

// AmountThresholdRule 大额交易阈值规则：金额达到阈值即命中（拦截）。
type AmountThresholdRule struct {
	id        string
	priority  int
	threshold float64
}

func NewAmountThresholdRule(id string, priority int, threshold float64) *AmountThresholdRule {
	return &AmountThresholdRule{id: id, priority: priority, threshold: threshold}
}

func (r *AmountThresholdRule) ID() string       { return r.id }
func (r *AmountThresholdRule) Priority() int    { return r.priority }
func (r *AmountThresholdRule) Severity() Action { return ActionBlock }

func (r *AmountThresholdRule) Evaluate(ctx context.Context, txn Transaction, ec EvalContext) RuleResult {
	res := RuleResult{RuleID: r.id, Priority: r.priority}
	if txn.Amount >= r.threshold {
		res.Hit = true
		res.RiskScore = 0.9
		res.Reason = fmt.Sprintf("金额 %.2f %s 达到大额阈值 %.2f", txn.Amount, txn.Currency, r.threshold)
	} else {
		res.Reason = fmt.Sprintf("金额 %.2f %s 低于大额阈值 %.2f", txn.Amount, txn.Currency, r.threshold)
	}
	return res
}

// ---- 高风险地区规则 ----

// CountryRule 高风险地区规则：交易地位于配置名单即命中（拦截）。
// 名单为规则本地静态配置，不依赖外部环节，因此不会产生依赖失败降级。
type CountryRule struct {
	id       string
	priority int
	highRisk map[string]struct{}
}

func NewCountryRule(id string, priority int, countries []string) *CountryRule {
	set := make(map[string]struct{}, len(countries))
	for _, c := range countries {
		set[c] = struct{}{}
	}
	return &CountryRule{id: id, priority: priority, highRisk: set}
}

func (r *CountryRule) ID() string       { return r.id }
func (r *CountryRule) Priority() int    { return r.priority }
func (r *CountryRule) Severity() Action { return ActionBlock }

func (r *CountryRule) Evaluate(ctx context.Context, txn Transaction, ec EvalContext) RuleResult {
	res := RuleResult{RuleID: r.id, Priority: r.priority}
	if _, ok := r.highRisk[txn.Country]; ok {
		res.Hit = true
		res.RiskScore = 0.8
		res.Reason = fmt.Sprintf("交易发生地 %s 属于高风险地区名单", txn.Country)
	} else {
		res.Reason = fmt.Sprintf("交易发生地 %s 不在高风险地区名单", txn.Country)
	}
	return res
}

// ---- 短时高频交易规则 ----

// VelocityRule 短时高频规则：窗口内该主体已有交易数达到上限即命中（转人工复核）。
type VelocityRule struct {
	id       string
	priority int
	window   time.Duration
	limit    int
}

func NewVelocityRule(id string, priority int, window time.Duration, limit int) *VelocityRule {
	return &VelocityRule{id: id, priority: priority, window: window, limit: limit}
}

func (r *VelocityRule) ID() string       { return r.id }
func (r *VelocityRule) Priority() int    { return r.priority }
func (r *VelocityRule) Severity() Action { return ActionReview }

func (r *VelocityRule) Evaluate(ctx context.Context, txn Transaction, ec EvalContext) RuleResult {
	res := RuleResult{RuleID: r.id, Priority: r.priority}
	count, err := ec.CountBefore(txn.AccountID, txn.EventTime, r.window)
	if err != nil {
		res.Degraded = true
		res.Degradation = Degradation{
			Reason: DegradeDependencyFailed,
			Detail: fmt.Sprintf("速度索引查询失败: %v", err),
			RuleID: r.id,
		}
		res.Reason = "速度索引查询失败，无法判定短时高频"
		return res
	}
	// count 为本笔之前、窗口内的已处理交易数（不含本笔自身）；达到上限即命中。
	if count >= r.limit {
		res.Hit = true
		res.RiskScore = 0.6
		res.Reason = fmt.Sprintf("%s 内主体 %s 已有 %d 笔交易（阈值 %d），疑似短时高频",
			r.window, txn.AccountID, count, r.limit)
	} else {
		res.Reason = fmt.Sprintf("%s 内主体 %s 已有 %d 笔交易（阈值 %d），频率正常",
			r.window, txn.AccountID, count, r.limit)
	}
	return res
}

// ---- 黑名单主体规则 ----

// BlacklistRule 黑名单规则：主体命中内部名单即命中（最高优先级拦截）。
// 名单存储环节失败时返回 DependencyFailed 降级，绝不把“查询失败”当作“不在名单”。
type BlacklistRule struct {
	id       string
	priority int
}

func NewBlacklistRule(id string, priority int) *BlacklistRule {
	return &BlacklistRule{id: id, priority: priority}
}

func (r *BlacklistRule) ID() string       { return r.id }
func (r *BlacklistRule) Priority() int    { return r.priority }
func (r *BlacklistRule) Severity() Action { return ActionBlock }

func (r *BlacklistRule) Evaluate(ctx context.Context, txn Transaction, ec EvalContext) RuleResult {
	res := RuleResult{RuleID: r.id, Priority: r.priority}
	listed, err := ec.IsBlacklisted(ctx, txn.AccountID)
	if err != nil {
		res.Degraded = true
		res.Degradation = Degradation{
			Reason: DegradeDependencyFailed,
			Detail: fmt.Sprintf("黑名单存储查询失败: %v", err),
			RuleID: r.id,
		}
		res.Reason = "黑名单存储查询失败，无法确认主体名单状态"
		return res
	}
	if listed {
		res.Hit = true
		res.RiskScore = 1
		res.Reason = fmt.Sprintf("主体 %s 命中黑名单", txn.AccountID)
	} else {
		res.Reason = fmt.Sprintf("主体 %s 不在黑名单", txn.AccountID)
	}
	return res
}
