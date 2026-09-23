package engine

import (
	"context"
	"sort"
	"sync"
	"time"
)

// VelocityIndex 维护按业务事件时间排列的已处理交易，供高频规则查询。
// 计数一律以业务事件时间为准（而非投递到达时间），因此迟到/乱序事件
// 不会让某个时间点之前的计数失真。
type VelocityIndex struct {
	mu     sync.Mutex
	byAcct map[string][]time.Time
}

func NewVelocityIndex() *VelocityIndex {
	return &VelocityIndex{byAcct: map[string][]time.Time{}}
}

// Add 录入一笔交易。重复交易由上层去重，这里仅按事件时间有序插入。
func (v *VelocityIndex) Add(txn Transaction) {
	v.mu.Lock()
	defer v.mu.Unlock()
	ts := v.byAcct[txn.AccountID]
	ts = append(ts, txn.EventTime)
	sort.Slice(ts, func(i, j int) bool { return ts[i].Before(ts[j]) })
	v.byAcct[txn.AccountID] = ts
}

// CountBefore 返回账号在 [t-window, t) 内、事件时间严格早于 t 的交易数，
// 即“本笔之前”的历史笔数（不含本笔自身）。这样无论本笔是否已录入索引，
// 首次评估与乱序重评得到的计数都一致；乱序到达的更晚事件也不会被计入。
func (v *VelocityIndex) CountBefore(accountID string, t time.Time, window time.Duration) (int, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.countLocked(accountID, t, t, window), nil
}

// countLocked 是不加锁的内部实现，调用方必须已持有 v.mu。
// 统计 [t-window, t) 内且事件时间不晚于 asOf 的交易数。
// asOf 为历史截止（待决事件自身时点），天然排除更晚（未来）事件。
func (v *VelocityIndex) countLocked(accountID string, t, asOf time.Time, window time.Duration) int {
	if window <= 0 {
		return 0
	}
	lower := t.Add(-window)
	n := 0
	for _, ts := range v.byAcct[accountID] {
		if !ts.Before(lower) && ts.Before(t) && !ts.After(asOf) {
			n++
		}
	}
	return n
}

// countWithAsOf 在持锁前提下统计 (t-window, t] 内、不晚于 asOf 的交易数。
func (v *VelocityIndex) countWithAsOf(accountID string, t, asOf time.Time, window time.Duration) int {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.countLocked(accountID, t, asOf, window)
}

var _ EvalContext = (*evalContext)(nil)

// evalContext 组合速度索引与名单存储，向规则提供 EvalContext。
// asOf 为零值时退化为“只看事件时点之前”的严格模式。
type evalContext struct {
	velocity  *VelocityIndex
	blacklist BlacklistStore
	asOf      time.Time
}

func newEvalContext(v *VelocityIndex, b BlacklistStore) *evalContext {
	return &evalContext{velocity: v, blacklist: b}
}

// withAsOf 派生一个以 watermark 时点为界的评估上下文。
func (c *evalContext) withAsOf(asOf time.Time) *evalContext {
	cp := *c
	cp.asOf = asOf
	return &cp
}

func (c *evalContext) CountBefore(accountID string, t time.Time, window time.Duration) (int, error) {
	if c.asOf.IsZero() {
		return c.velocity.CountBefore(accountID, t, window)
	}
	return c.velocity.countWithAsOf(accountID, t, c.asOf, window), nil
}

func (c *evalContext) IsBlacklisted(ctx context.Context, accountID string) (bool, error) {
	return c.blacklist.IsBlacklisted(ctx, accountID)
}
