package engine

import (
	"context"
	"sort"
)

// Registry 管理规则配置：按 ID 查找规则、按稳定优先级列出规则。
type Registry struct {
	rules map[string]Rule
	order []Rule
}

// NewRegistry 创建空规则注册表。
func NewRegistry() *Registry {
	return &Registry{rules: map[string]Rule{}}
}

// Register 注册一条规则；重复 ID 返回 ErrDuplicateRule。
func (r *Registry) Register(rule Rule) error {
	if _, exists := r.rules[rule.ID()]; exists {
		return ErrDuplicateRule
	}
	r.rules[rule.ID()] = rule
	r.order = append(r.order, rule)
	return nil
}

// Get 按 ID 查找规则，缺失时第二个返回值为 false。
func (r *Registry) Get(id string) (Rule, bool) {
	rule, ok := r.rules[id]
	return rule, ok
}

// Ordered 返回按优先级（Priority 升序，相同则按规则 ID 字典序）
// 稳定排序后的全部规则，保证多规则组合评估的确定性。
// 排序每次基于注册顺序做稳定排序，规则配置不变时输出永远一致。
func (r *Registry) Ordered() []Rule {
	out := make([]Rule, len(r.order))
	copy(out, r.order)
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Priority() != out[j].Priority() {
			return out[i].Priority() < out[j].Priority()
		}
		return out[i].ID() < out[j].ID()
	})
	return out
}

// MustIDs 校验给定规则 ID 全部存在；返回首个缺失的 ID。
func (r *Registry) MustIDs(ctx context.Context, ids []string) (string, bool) {
	for _, id := range ids {
		if _, ok := r.rules[id]; !ok {
			return id, false
		}
	}
	return "", true
}
