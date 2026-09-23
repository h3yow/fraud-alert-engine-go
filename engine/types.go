// Package engine 实现本地实时交易风控与合规告警组件。
//
// 组件用本地交易流水与规则配置模拟监测过程，保证在重复、迟到或乱序
// 事件下，规则命中、告警去重与人工复核的结论一致且可解释。
package engine

import (
	"context"
	"time"
)

// Action 是一笔交易的最终唯一定论。
type Action int

const (
	// ActionAllow 放行
	ActionAllow Action = iota
	// ActionBlock 拦截
	ActionBlock
	// ActionReview 转人工复核
	ActionReview
)

func (a Action) String() string { return actionNames[a] }

var actionNames = [...]string{"ALLOW", "BLOCK", "REVIEW"}

// DegradeReason 描述发生降级时的失败原因，不同失败原因须可区分。
type DegradeReason int

const (
	DegradeNone DegradeReason = iota
	// DegradeRuleMissing 规则配置缺失或引用了不存在的规则
	DegradeRuleMissing
	// DegradeInvalidField 交易字段非法（金额、主体、时间等）
	DegradeInvalidField
	// DegradeDependencyFailed 判定依赖的内部环节失败（如名单存储异常）
	DegradeDependencyFailed
)

func (r DegradeReason) String() string { return degradeReasonNames[r] }

var degradeReasonNames = [...]string{
	"NONE",
	"RULE_MISSING",
	"INVALID_FIELD",
	"DEPENDENCY_FAILED",
}

// Transaction 是一笔本地交易流水。
type Transaction struct {
	TxnID     string    // 交易唯一标识，用于重复事件去重
	AccountID string    // 主体标识
	Amount    float64   // 交易金额（必须为正）
	Currency  string    // 币种
	Country   string    // 交易发生地
	EventTime time.Time // 业务事件时间（乱序/迟到以此判定）
}

// RuleHit 记录一条命中规则的可解释判定依据。
type RuleHit struct {
	RuleID    string
	Priority  int
	Reason    string  // 人类可读的判定依据
	RiskScore float64 // 规则风险分 [0,1]
}

// Degradation 记录一次降级的可区分原因与上下文。
type Degradation struct {
	Reason DegradeReason
	Detail string
	RuleID string // 触发降级的规则（若有）
}

// Verdict 是一笔交易的最终判定结论。
type Verdict struct {
	TxnID       string
	Action      Action
	Hits        []RuleHit
	Degraded    bool
	Degradation Degradation
	DecidedAt   time.Time // 首次判定时间（结论不可变，重复/乱序投递返回同一结论）
}

// RuleResult 是单条规则的评估结果。
type RuleResult struct {
	RuleID    string
	Priority  int
	Hit       bool
	Reason    string
	RiskScore float64
	// Degraded 为 true 表示该规则评估时发生降级（而非正常未命中）。
	Degraded    bool
	Degradation Degradation
}

// EvalContext 是规则评估所依赖的运行时上下文。
type EvalContext interface {
	// CountBefore 返回账号在 t（含）之前窗口内已处理交易数。
	CountBefore(accountID string, t time.Time, window time.Duration) (int, error)
	// IsBlacklisted 查询主体是否在黑名单中。
	IsBlacklisted(ctx context.Context, accountID string) (bool, error)
}

// Rule 是一条可评估的风控规则。
type Rule interface {
	ID() string
	// Priority 数值越小优先级越高。
	Priority() int
	// Severity 声明该规则命中时隐含的定论（拦截或转人工复核）。
	Severity() Action
	// Evaluate 对交易进行评估。
	Evaluate(ctx context.Context, txn Transaction, ec EvalContext) RuleResult
}

// AlertStatus 告警状态，只能单向流转。
type AlertStatus int

const (
	// StatusOpen 已生成、待处理
	StatusOpen AlertStatus = iota
	// StatusInReview 已进入人工复核
	StatusInReview
	// StatusResolved 已处置终结（确认欺诈/解除）
	StatusResolved
)

func (s AlertStatus) String() string { return alertStatusNames[s] }

var alertStatusNames = [...]string{"OPEN", "IN_REVIEW", "RESOLVED"}

// Alert 是去重聚合后的告警实体。
type Alert struct {
	Key       string
	Status    AlertStatus
	Severity  Action // 触发告警的最终定论（BLOCK 优先于 REVIEW）
	AccountID string
	TxnIDs    []string // 聚合到该告警的全部交易（按首次出现顺序）
	Hits      []RuleHit
	CreatedAt time.Time
	UpdatedAt time.Time
	Count     int // 去重聚合后的命中交易计数，须与 TxnIDs 守恒
	Version   int // 状态版本，每次成功流转自增
}
