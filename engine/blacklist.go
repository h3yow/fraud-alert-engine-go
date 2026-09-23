package engine

import "context"

// BlacklistStore 是名单存储的抽象，内部环节失败以 error 表达，
// 上层据此走降级策略，而非把“查不到”误判为“不在名单”。
type BlacklistStore interface {
	IsBlacklisted(ctx context.Context, accountID string) (bool, error)
}

// MemoryBlacklistStore 是基于内存 map 的名单存储。
type MemoryBlacklistStore struct {
	entries map[string]struct{}
}

// NewMemoryBlacklistStore 根据账号 ID 列表构造内存名单。
func NewMemoryBlacklistStore(accountIDs []string) *MemoryBlacklistStore {
	m := &MemoryBlacklistStore{entries: make(map[string]struct{}, len(accountIDs))}
	for _, id := range accountIDs {
		m.entries[id] = struct{}{}
	}
	return m
}

func (s *MemoryBlacklistStore) IsBlacklisted(ctx context.Context, accountID string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	_, ok := s.entries[accountID]
	return ok, nil
}

// FailBlacklistStore 始终返回内部环节失败，用于异常降级测试：
// 规则必须把 (false, err) 与 (false, nil) 区分开，前者走 DependencyFailed 降级。
type FailBlacklistStore struct{}

func (s *FailBlacklistStore) IsBlacklisted(ctx context.Context, accountID string) (bool, error) {
	return false, ErrDependencyFailed
}
