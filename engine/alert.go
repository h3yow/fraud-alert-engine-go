package engine

import (
	"context"
	"sort"
	"sync"
	"time"
)

// 状态机合法单向流转：OPEN -> IN_REVIEW -> RESOLVED。
// 任何回退（如 RESOLVED -> OPEN）、越级或重复流转均被拒绝；
// 乱序到达的过期命令不会导致状态回退。
var allowedTransitions = map[AlertStatus][]AlertStatus{
	StatusOpen:     {StatusInReview, StatusResolved},
	StatusInReview: {StatusResolved},
	StatusResolved: {},
}

// CanTransition 判断状态是否允许单向流转。
func CanTransition(from, to AlertStatus) bool {
	for _, t := range allowedTransitions[from] {
		if t == to {
			return true
		}
	}
	return false
}

// AlertStore 负责告警去重、聚合与状态流转。所有操作互斥、确定性执行。
type AlertStore struct {
	mu     sync.Mutex
	alerts map[string]*Alert
	// member 记录每个告警簇内已计入的 TxnID，支撑重评场景下的幂等 upsert。
	member map[string]map[string]struct{}
}

func NewAlertStore() *AlertStore {
	return &AlertStore{
		alerts: map[string]*Alert{},
		member: map[string]map[string]struct{}{},
	}
}

// Record 依据交易定论记录一次命中（重评时以最新定论做幂等 upsert）：
//   - 定论为 ActionAllow 且该簇内无此交易：不产生告警，返回 (nil,false)；
//   - 同一去重键（同一主体或同一交易簇）的命中被聚合进同一告警，
//     重复 TxnID 不会重复计数，Count 与 TxnIDs 严格守恒；
//   - 先前按 ALLOW 评估的交易若在重评中变为可疑，会被补记进既有/新建告警；
//   - 告警严重度只允许单调增强（REVIEW -> BLOCK），不会被更弱结论稀释；
//   - 已终结（RESOLVED）告警的迟到命中仍做计数聚合（流水真相保留），
//     但状态绝不回退。
//
// 返回告警快照及该告警是否为本次新建。
func (s *AlertStore) Record(v *Verdict, txn Transaction, key string, now time.Time) (*Alert, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	members := s.member[key]
	_, alreadyMember := members[txn.TxnID]

	// 未计入过且当前定论放行：不产生告警。
	if !alreadyMember && v.Action == ActionAllow {
		return nil, false
	}

	a, exists := s.alerts[key]
	created := false
	if !exists {
		// 已计入却没有告警属于内部不变量被破坏，防御性处理。
		a = &Alert{
			Key:       key,
			Status:    StatusOpen,
			Severity:  v.Action,
			AccountID: txn.AccountID,
			CreatedAt: now.UTC(),
			Version:   1,
		}
		s.alerts[key] = a
		s.member[key] = map[string]struct{}{}
		members = s.member[key]
		created = true
	}

	if !alreadyMember {
		a.TxnIDs = append(a.TxnIDs, txn.TxnID)
		a.Count++
		if members == nil {
			members = map[string]struct{}{}
			s.member[key] = members
		}
		members[txn.TxnID] = struct{}{}
		if v.Action != ActionAllow {
			mergeHits(&a.Hits, v.Hits)
		}
	} else if v.Action != ActionAllow {
		// 已计入成员重评：依据合并是幂等的，计数不变。
		mergeHits(&a.Hits, v.Hits)
	}

	// 严重度单调增强：BLOCK 优先于 REVIEW。
	if v.Action == ActionBlock && a.Severity != ActionBlock {
		a.Severity = ActionBlock
	}
	a.UpdatedAt = now.UTC()
	return cloneAlert(a), created
}

// mergeHits 把新命中的规则依据并入既有依据（同规则只保留一份，按优先级稳定排序）。
func mergeHits(dst *[]RuleHit, add []RuleHit) {
	seen := map[string]int{} // ruleID -> index in dst
	for i, h := range *dst {
		seen[h.RuleID] = i
	}
	for _, h := range add {
		if _, ok := seen[h.RuleID]; !ok {
			seen[h.RuleID] = len(*dst)
			*dst = append(*dst, h)
		}
	}
	sort.SliceStable(*dst, func(i, j int) bool {
		if (*dst)[i].Priority != (*dst)[j].Priority {
			return (*dst)[i].Priority < (*dst)[j].Priority
		}
		return (*dst)[i].RuleID < (*dst)[j].RuleID
	})
}

// Transition 在状态机允许时单向流转告警状态。
// 重复命令返回 ErrDuplicateTransition，回退/越级返回 ErrIllegalTransition，
// 未知键返回 ErrAlertNotFound，便于调用方区分迟到、乱序与非法命令。
func (s *AlertStore) Transition(ctx context.Context, key string, to AlertStatus, now time.Time) (*Alert, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	a, ok := s.alerts[key]
	if !ok {
		return nil, ErrAlertNotFound
	}
	if a.Status == to {
		return cloneAlert(a), ErrDuplicateTransition
	}
	if !CanTransition(a.Status, to) {
		return cloneAlert(a), ErrIllegalTransition
	}
	a.Status = to
	a.UpdatedAt = now.UTC()
	a.Version++
	return cloneAlert(a), nil
}

// Get 按键读取告警快照。
func (s *AlertStore) Get(key string) (*Alert, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, ok := s.alerts[key]
	if !ok {
		return nil, false
	}
	return cloneAlert(a), true
}

// List 返回全部告警的稳定快照（按 CreatedAt、Key 排序）。
func (s *AlertStore) List() []*Alert {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*Alert, 0, len(s.alerts))
	for _, a := range s.alerts {
		out = append(out, cloneAlert(a))
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].CreatedAt.Before(out[j].CreatedAt)
		}
		return out[i].Key < out[j].Key
	})
	return out
}

// cloneAlert 返回深拷贝快照，避免调用方修改内部状态。
func cloneAlert(a *Alert) *Alert {
	cp := *a
	cp.TxnIDs = append([]string(nil), a.TxnIDs...)
	cp.Hits = append([]RuleHit(nil), a.Hits...)
	return &cp
}
