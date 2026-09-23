package engine

import "errors"

// 告警状态流转相关错误，不同失败原因须可区分。
var (
	// ErrAlertNotFound 告警不存在（迟到或乱序的命令引用了未知键）。
	ErrAlertNotFound = errors.New("alert not found")
	// ErrIllegalTransition 状态机不允许的流转（回退或越级）。
	ErrIllegalTransition = errors.New("illegal alert state transition")
	// ErrDuplicateTransition 当前已处于目标状态，重复命令不产生效果。
	ErrDuplicateTransition = errors.New("alert already in target state")
)

// Rule registration errors.
var (
	ErrDuplicateRule = errors.New("duplicate rule id")
)

// ErrDependencyFailed 表示判定所依赖的内部环节（如名单存储）失败。
var ErrDependencyFailed = errors.New("dependency failed")
