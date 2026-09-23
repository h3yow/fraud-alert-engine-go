package engine

import (
	"context"
	"time"
)

// DegradePolicy 定义各类失败原因对应的降级定论。
type DegradePolicy struct {
	RuleMissing      Action
	InvalidField     Action
	DependencyFailed Action
}

// ConservativeDegradePolicy 是保守降级策略：
// 字段非法直接拦截；规则缺失（配置异常）与内部环节失败转人工复核，
// 可疑交易绝不静默放行；不同失败原因通过 Degradation.Reason 区分。
func ConservativeDegradePolicy() DegradePolicy {
	return DegradePolicy{
		RuleMissing:      ActionReview,
		InvalidField:     ActionBlock,
		DependencyFailed: ActionReview,
	}
}

// Evaluator 对单笔交易执行多规则组合评估，
// 按稳定优先级从命中规则中给出唯一定论，并处理异常降级。
type Evaluator struct {
	registry *Registry
	policy   DegradePolicy
	now      func() time.Time
}

func NewEvaluator(reg *Registry, policy DegradePolicy) *Evaluator {
	return &Evaluator{registry: reg, policy: policy, now: time.Now}
}

// Evaluate 执行组合评估。判定依据（命中、未命中、降级原因）全部保留在 Verdict 中。
func (e *Evaluator) Evaluate(ctx context.Context, txn Transaction, ec EvalContext) *Verdict {
	v := &Verdict{TxnID: txn.TxnID, Action: ActionAllow, DecidedAt: e.now().UTC()}

	// 1) 输入合法性：字段非法直接按策略降级，不进入规则评估。
	if detail, bad := validateTransaction(txn); bad {
		v.Action = e.policy.InvalidField
		v.Degraded = true
		v.Degradation = Degradation{Reason: DegradeInvalidField, Detail: detail}
		return v
	}

	// 2) 规则配置缺失：没有任何规则意味着无法做出可信监测，按策略降级。
	rules := e.registry.Ordered()
	if len(rules) == 0 {
		v.Action = e.policy.RuleMissing
		v.Degraded = true
		v.Degradation = Degradation{
			Reason: DegradeRuleMissing,
			Detail: "规则注册表为空，无法执行组合评估",
		}
		return v
	}

	// 3) 组合评估全部规则。规则按优先级稳定排序，保证评估顺序确定。
	var hits []RuleHit
	var firstDegraded *RuleResult
	for _, rule := range rules {
		if err := ctx.Err(); err != nil {
			v.Action = e.policy.DependencyFailed
			v.Degraded = true
			v.Degradation = Degradation{
				Reason: DegradeDependencyFailed,
				Detail: "评估被取消: " + err.Error(),
			}
			return v
		}
		res := rule.Evaluate(ctx, txn, ec)
		if res.Hit {
			hits = append(hits, RuleHit{
				RuleID:    res.RuleID,
				Priority:  res.Priority,
				Reason:    res.Reason,
				RiskScore: res.RiskScore,
			})
			continue
		}
		if res.Degraded && firstDegraded == nil {
			rr := res
			firstDegraded = &rr
		}
	}

	// 4) 唯一定论：多规则同时命中时，按最高优先级（Priority 最小；
	//    并列时取规则 ID 字典序）的命中规则的 Severity 作为最终定论。
	//    命中规则之间是“拦截/复核”的强度关系而非真矛盾；稳定次序保证
	//    同一交易重复或乱序投递永远得到同一个结论。
	if len(hits) > 0 {
		winner := hits[0]
		for _, h := range hits[1:] {
			if h.Priority < winner.Priority ||
				(h.Priority == winner.Priority && h.RuleID < winner.RuleID) {
				winner = h
			}
		}
		rule, _ := e.registry.Get(winner.RuleID)
		v.Hits = hits
		v.Action = rule.Severity()
		return v
	}

	// 5) 没有命中但存在规则降级：按策略给出结论，不静默放过。
	if firstDegraded != nil {
		v.Action = e.policy.DependencyFailed
		v.Degraded = true
		v.Degradation = firstDegraded.Degradation
		return v
	}

	// 6) 全部规则正常且未命中：放行。
	return v
}

// validateTransaction 校验交易字段合法性，返回人类可读原因。
func validateTransaction(txn Transaction) (string, bool) {
	switch {
	case txn.TxnID == "":
		return "交易 ID(TxnID) 为空", true
	case txn.AccountID == "":
		return "主体 ID(AccountID) 为空", true
	case txn.Amount <= 0:
		return "交易金额必须为正数，实际为 " + formatAmount(txn.Amount), true
	case txn.Currency == "":
		return "币种(Currency) 为空", true
	case txn.Country == "":
		return "交易发生地(Country) 为空", true
	case txn.EventTime.IsZero():
		return "业务事件时间(EventTime) 为零值", true
	default:
		return "", false
	}
}
