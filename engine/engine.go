package engine

import (
	"context"
	"fmt"
	"log"
	"os"
	"sort"
	"sync"
	"time"
)

// defaultLateGrace 是默认的迟到容忍窗口：事件时间晚于
// (watermark - grace) 的乱序/迟到事件仍按 watermark 时点的完整已知历史判定，
// 因此与其“按序投递”的结论一致；更老的迟到事件按首次已知历史做最终定论，
// 定论一经作出不可更改（不做会回退的状态更新）。
const defaultLateGrace = 30 * time.Second

// Logger 是引擎使用的最小日志接口，日志中须打印输入与判定依据。
type Logger interface {
	Printf(format string, args ...any)
}

// nopLogger 丢弃日志，用于显式静默的测试场景。
type nopLogger struct{}

func (nopLogger) Printf(string, ...any) {}

// NopLogger 返回空日志器。
func NopLogger() Logger { return nopLogger{} }

// Engine 串联规则注册表、组合评估器、告警存储与幂等判定缓存，
// 是本组件对外的统一入口。
type Engine struct {
	mu        sync.Mutex
	registry  *Registry
	evaluator *Evaluator
	store     *AlertStore
	velocity  *VelocityIndex
	blacklist BlacklistStore
	log       Logger
	now       func() time.Time
	lateGrace time.Duration

	// watermark 为已观测到的最大业务事件时间。
	watermark time.Time
	// pending 缓存处于迟到容忍窗口内事件的定论；这些事件可能因更老事件
	// 的到达而被重评（结论只会从 ALLOW 变为可疑，且缓存随之原子替换）。
	pending map[string]*pendingEntry
	// verdicts 缓存已最终定论交易；重复 TxnID 永远返回同一对象，
	// 不会重复计数或产生重复告警。
	verdicts map[string]*Verdict
}

type pendingEntry struct {
	verdict *Verdict
	txn     Transaction
}

// Config 为引擎装配参数。
type Config struct {
	Blacklist BlacklistStore
	Policy    DegradePolicy
	Logger    Logger
	// Now 注入时钟，便于测试；为 nil 时使用 time.Now。
	Now func() time.Time
	// LateGrace 自定义迟到容忍窗口；为零值使用 defaultLateGrace。
	LateGrace time.Duration
}

// NewEngine 装配引擎。cfg 中缺省项使用保守默认值。
func NewEngine(reg *Registry, cfg Config) *Engine {
	bl := cfg.Blacklist
	if bl == nil {
		bl = NewMemoryBlacklistStore(nil)
	}
	logger := cfg.Logger
	if logger == nil {
		logger = log.New(os.Stderr, "[fraud-engine] ", log.LstdFlags|log.Lmicroseconds)
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	grace := cfg.LateGrace
	if grace <= 0 {
		grace = defaultLateGrace
	}
	ev := NewEvaluator(reg, cfg.Policy)
	ev.now = now
	return &Engine{
		registry:  reg,
		evaluator: ev,
		store:     NewAlertStore(),
		velocity:  NewVelocityIndex(),
		blacklist: bl,
		log:       logger,
		now:       now,
		lateGrace: grace,
		pending:   map[string]*pendingEntry{},
		verdicts:  map[string]*Verdict{},
	}
}

// Ingest 处理一笔带业务事件时间的交易。
//   - 重复 TxnID 返回已有定论（同一对象指针），幂等不计数；
//   - 乱序/迟到但处于容忍窗口内的事件被缓冲；更老事件到达时，待决事件按
//     各自事件时点重评（历史截止天然排除未来事件），流平息后与按序投递一致；
//   - 暂定论只允许 ALLOW -> REVIEW -> BLOCK 单调增强，不回退；
//   - watermark 推进越过容忍窗口后事件最终定稿，定论不可变；
//   - 超出容忍窗口的迟到事件按其时点已知历史立即最终定稿。
func (e *Engine) Ingest(ctx context.Context, txn Transaction) (*Verdict, *Alert) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if v, ok := e.lookupVerdict(txn.TxnID); ok {
		var alert *Alert
		if txn.AccountID != "" && !txn.EventTime.IsZero() {
			alert, _ = e.store.Get(dedupeKey(txn))
		}
		e.log.Printf("重复事件被幂等忽略 txn=%s account=%s eventTime=%s => 已有定论 %s",
			txn.TxnID, txn.AccountID, fmtTime(txn.EventTime), v.Action)
		return v, alert
	}
	if p, ok := e.pending[txn.TxnID]; ok {
		e.log.Printf("重复缓冲事件被幂等忽略 txn=%s => 暂定论 %s", txn.TxnID, p.verdict.Action)
		return p.verdict, e.alertFor(txn)
	}

	// 字段非法不依赖历史，立即最终降级定论。
	if detail, bad := validateTransaction(txn); bad {
		v := &Verdict{
			TxnID:       txn.TxnID,
			Action:      e.evaluator.policy.InvalidField,
			Degraded:    true,
			Degradation: Degradation{Reason: DegradeInvalidField, Detail: detail},
			DecidedAt:   e.now().UTC(),
		}
		if txn.TxnID != "" {
			e.verdicts[txn.TxnID] = v
		}
		alert, _ := e.recordAlert(v, txn)
		e.logVerdict(txn, v, alert, true)
		return v, alert
	}

	if txn.EventTime.After(e.watermark) {
		e.watermark = txn.EventTime
	}
	e.velocity.Add(txn)

	// 为本事件建立（或复用）稳定的定论对象；后续重评只原地更新其内容，
	// 保证首次拿到的 *Verdict 指针在收敛、定稿后依然有效且内容最新。
	v := &Verdict{TxnID: txn.TxnID, DecidedAt: e.now().UTC()}

	adoptVerdict(v, e.evaluateAt(ctx, txn, txn.EventTime))

	if txn.EventTime.Before(e.watermark.Add(-e.lateGrace)) && txn.EventTime.Before(e.watermark) {
		// 超出容忍窗口的迟到事件：立即最终定稿，定论不再可变；
		// 同时让窗口内待决事件感知这条更老历史并重评。
		e.verdicts[txn.TxnID] = v
		e.reevaluatePending(ctx)
		alert, _ := e.recordAlert(v, txn)
		e.log.Printf("迟到事件超出容忍窗口(%s)，按其时点历史最终定论", e.lateGrace)
		e.logVerdict(txn, v, alert, alert != nil)
		return v, alert
	}

	// 容忍窗口内（含按序到达）：缓冲为待决定论，随更老事件到达而重评收敛。
	e.pending[txn.TxnID] = &pendingEntry{verdict: v, txn: txn}
	e.reevaluatePending(ctx)
	e.finalizeDuePending(ctx)

	alert, created := e.recordAlert(v, txn)
	e.logVerdict(txn, v, alert, created)
	return v, alert
}

// evaluateAt 以 asOf 时点的历史上下文对交易做组合评估。
func (e *Engine) evaluateAt(ctx context.Context, txn Transaction, asOf time.Time) *Verdict {
	ec := newEvalContext(e.velocity, e.blacklist).withAsOf(asOf)
	return e.evaluator.Evaluate(ctx, txn, ec)
}

// adoptVerdict 将 fresh 的定论内容原地拷贝到 dst，保证同一交易在多次
// 重评、定稿过程中对外暴露的 *Verdict 指针恒定，重复投递拿到同一对象。
func adoptVerdict(dst, fresh *Verdict) {
	dst.Action = fresh.Action
	dst.Hits = fresh.Hits
	dst.Degraded = fresh.Degraded
	dst.Degradation = fresh.Degradation
	if dst.DecidedAt.IsZero() {
		dst.DecidedAt = fresh.DecidedAt
	}
}

// reevaluatePending 在一条更老的迟到/乱序事件录入后调用：所有待决事件都按
// 自身事件时点重评。此刻全部已到达的更老事件都在索引中，而评估上下文的
// 历史截止（待决事件自身时点）天然排除更晚事件，因此结果与按序投递一致。
// 重评结果原地写入稳定的 Verdict 对象，不替换指针。
func (e *Engine) reevaluatePending(ctx context.Context) {
	for _, p := range e.pending {
		fresh := e.evaluateAt(ctx, p.txn, p.txn.EventTime)
		adoptVerdict(p.verdict, fresh)
		e.reconcileAlert(p.verdict, p.txn)
	}
}

// finalizeDuePending 将事件时间不晚于 (watermark - grace) 的缓冲事件定稿：
// 以当前已知历史做最后一次原地重评，随后移入最终缓存、结论不可再变；
// 指针保持不变，调用方早期持有的 *Verdict 仍指向最终定论。
func (e *Engine) finalizeDuePending(ctx context.Context) {
	cutoff := e.watermark.Add(-e.lateGrace)
	for id, p := range e.pending {
		if p.txn.EventTime.After(cutoff) {
			continue
		}
		fresh := e.evaluateAt(ctx, p.txn, p.txn.EventTime)
		adoptVerdict(p.verdict, fresh)
		e.reconcileAlert(p.verdict, p.txn)
		e.verdicts[id] = p.verdict
		delete(e.pending, id)
		e.log.Printf("缓冲事件定稿 txn=%s eventTime=%s 定论=%s", id, fmtTime(p.txn.EventTime), p.verdict.Action)
	}
}

// reconcileAlert 用于事件重评后刷新告警：若定论从非告警变为告警或严重度变化，
// 交给 AlertStore 做幂等聚合（同 TxnID 不会重复计数，严重度单调增强）。
func (e *Engine) reconcileAlert(v *Verdict, txn Transaction) (*Alert, bool) {
	return e.recordAlert(v, txn)
}

// recordAlert 记录一次定论对应的告警，返回快照与是否新建。
func (e *Engine) recordAlert(v *Verdict, txn Transaction) (*Alert, bool) {
	return e.store.Record(v, txn, dedupeKey(txn), e.now())
}

func (e *Engine) alertFor(txn Transaction) *Alert {
	a, ok := e.store.Get(dedupeKey(txn))
	if !ok {
		return nil
	}
	return a
}

func (e *Engine) lookupVerdict(id string) (*Verdict, bool) {
	if v, ok := e.verdicts[id]; ok {
		return v, true
	}
	if p, ok := e.pending[id]; ok {
		return p.verdict, true
	}
	return nil, false
}

// IngestBatch 批量处理：先按业务事件时间（并列按 TxnID）稳定排序再顺序摄入，
// 消除批次内部乱序造成的非确定性；返回切片与输入位置一一对应。
func (e *Engine) IngestBatch(ctx context.Context, txns []Transaction) []*Verdict {
	idx := make([]int, len(txns))
	for i := range idx {
		idx[i] = i
	}
	sort.SliceStable(idx, func(a, b int) bool {
		ta, tb := txns[idx[a]].EventTime, txns[idx[b]].EventTime
		if !ta.Equal(tb) {
			return ta.Before(tb)
		}
		return txns[idx[a]].TxnID < txns[idx[b]].TxnID
	})
	out := make([]*Verdict, len(txns))
	for _, i := range idx {
		out[i], _ = e.Ingest(ctx, txns[i])
	}
	return out
}

// TransitionAlert 驱动告警状态单向流转（OPEN -> IN_REVIEW -> RESOLVED）。
func (e *Engine) TransitionAlert(ctx context.Context, key string, to AlertStatus, now time.Time) (*Alert, error) {
	a, err := e.store.Transition(ctx, key, to, now.UTC())
	if err != nil {
		e.log.Printf("状态流转被拒绝 key=%s target=%s err=%v", key, to, err)
		return a, err
	}
	e.log.Printf("告警状态流转 key=%s -> %s version=%d", key, to, a.Version)
	return a, nil
}

// Alerts 列出全部告警快照（稳定排序）。
func (e *Engine) Alerts() []*Alert { return e.store.List() }

// Alert 按键读取告警快照。
func (e *Engine) Alert(key string) (*Alert, bool) { return e.store.Get(key) }

// logVerdict 打印输入与判定依据，保证结论可解释、可审计。
func (e *Engine) logVerdict(txn Transaction, v *Verdict, alert *Alert, created bool) {
	e.log.Printf("输入 txn=%s account=%s amount=%.2f %s country=%s eventTime=%s",
		txn.TxnID, txn.AccountID, txn.Amount, txn.Currency, txn.Country, fmtTime(txn.EventTime))
	if v.Degraded {
		e.log.Printf("降级 txn=%s action=%s reason=%s detail=%s rule=%s",
			v.TxnID, v.Action, v.Degradation.Reason, v.Degradation.Detail, v.Degradation.RuleID)
	}
	for _, h := range v.Hits {
		e.log.Printf("命中 txn=%s rule=%s priority=%d score=%.2f 依据=%q",
			v.TxnID, h.RuleID, h.Priority, h.RiskScore, h.Reason)
	}
	agg := "无告警"
	if alert != nil {
		agg = fmt.Sprintf("告警 key=%s status=%s severity=%s count=%d created=%t",
			alert.Key, alert.Status, alert.Severity, alert.Count, created)
	}
	e.log.Printf("定论 txn=%s action=%s %s", v.TxnID, v.Action, agg)
}

// dedupeKey 计算告警去重键：同一主体同一业务日（UTC）的交易聚为一个交易簇。
func dedupeKey(txn Transaction) string {
	return fmt.Sprintf("acct:%s|day:%s", txn.AccountID, txn.EventTime.UTC().Format("2006-01-02"))
}

func fmtTime(t time.Time) string {
	if t.IsZero() {
		return "<zero>"
	}
	return t.UTC().Format(time.RFC3339)
}
