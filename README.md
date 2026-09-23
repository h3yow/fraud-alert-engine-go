# 本地实时交易风控与合规告警组件 (fraud-alert-engine-go)

一个纯内存、无外部依赖的本地组件，用本地交易流水与规则配置模拟实时监测：
多规则组合评估、确定性优先级裁决、告警去重聚合、单向状态机，以及异常数据的
降级与人工复核。目标是在**重复、迟到或乱序事件**下给出一致、可解释的结论。

## 目录结构

| 包 | 职责 |
| --- | --- |
| `internal/domain` | 交易、命中(Hit)、判定(Verdict)、告警(Alert)等核心类型 |
| `internal/rule` | 规则接口、注册表、三条内置规则与默认规则集 |
| `internal/engine` | 输入校验、必需规则检查、panic/error 隔离、多规则优先级裁决 |
| `internal/alert` | 去重键、告警聚合、EventID 集合计数、单向状态机 |
| `internal/processor` | 总装管线：幂等去重、迟到水位线、告警装配、对外入口 |
| `internal/logging` | 标准库 logger 与线程安全内存 logger（供测试断言） |
| `cmd/demo` | 本地演示：正常/冲突/重复/乱序/迟到/降级/状态流转全场景 |

## 规则与优先级

同一笔交易同时评估所有**已启用**的规则；每条命中保留在 `Verdict.Hits` 中
（完整判定依据），并按以下确定性次序选出**唯一**定论 `Verdict.WinningHit`：

1. `Priority` 数值**越小越优先**；
2. 优先级相同则按 `RuleID` 字典序。

内置规则（固定优先级，属于公开契约）：

| RuleID | 优先级 | 触发条件 | 动作 |
| --- | --- | --- | --- |
| `S000_MISSING_REQUIRED_RULE`（合成） | 0 | 必需规则缺失/被禁用 | BLOCK（fail-closed） |
| `S001_INVALID_INPUT`（合成） | 50 | 字段非法 | REVIEW |
| `R200_HIGH_RISK_COUNTRY` | 100 | 交易发起国在高风险名单（默认 ZZ/XX） | BLOCK |
| `R100_LARGE_AMOUNT` | 200 | 金额超过阈值（默认 >10000） | BLOCK |
| `R300_VELOCITY` | 300 | 同一账户 5 分钟窗口内唯一交易数 >3 | REVIEW |

示例：一笔来自高风险国家且金额超大的交易同时命中 R200 与 R100，最终定论
稳定为 **R200 BLOCK**，两条命中都可在日志与 `Hits` 中追溯。

## 告警去重与聚合

去重键 (`alert.DedupKey`)：

- 同一非空 `ClusterID` 的事件聚成**一条簇告警**（跨规则合并）：`cluster:<id>`；
- 否则按 `(规则, 账户, 冷却时间桶)` 聚合：
  `rule:<id>|acct:<acct>|bucket:<unix>`，冷却窗口默认 10 分钟，时间桶按
  OccurredAt 截断到绝对边界。

计数守恒：每条告警维护 **EventID 集合**，`HitCount = len(EventIDs)`。
重复投递（相同 EventID）幂等吸收，绝不重复告警、绝不虚增计数。
`FirstSeen/LastSeen` 由业务时间最小/最大值维护，迟到事件不会使它们回退。

### 告警状态机（单向流转）

```
OPEN ──Ack──▶ ACKED ──Resolve──▶ RESOLVED(终态)
  └──────────Resolve───────────▶ RESOLVED(终态)
```

- 只允许**严格向前**：`ACKED` 再次 Ack、`RESOLVED` 再 Ack/Resolve 一律拒绝
  （返回 `ok=false` 并记录日志），状态不可能回退；
- 迟到或重复事件到达时，告警计数与状态均不受影响（processor 对已见
  EventID 直接回放缓存判定，根本不进入写入路径）。

## 降级与人工复核策略

不同失败原因以 `domain.FailureReason` **明确区分**，且一律产生非 ALLOW
结论与对应告警，可疑交易不会被静默放过：

| 失败原因 | 触发条件 | 策略 |
| --- | --- | --- |
| `INVALID_FIELD` | EventID/TxID/AccountID/Currency 为空、金额 <= 0、OccurredAt 为零值 | **REVIEW** 转人工，日志给出具体非法字段 |
| `RULE_MISSING` | 必需规则未注册或被禁用 | **BLOCK** fail-closed（宁错拦不漏放） |
| `RULE_EVAL_ERROR` | 规则返回 error 或 panic（engine 用 recover 隔离） | 采用该规则配置的 `OnError` 兜底（默认 REVIEW），panic 永不逃逸 |

正常路径 `FailureReason` 为空。判定中 `Degraded=true` 与 `Failure` 字段
让调用方可以区分"真实风控命中"与"降级产生的结论"。

## 重复、迟到与乱序的一致性保证

- **幂等**：`Processor` 以 `EventID` 缓存首次判定；再次投递返回相同
  Verdict（标记 `IsDuplicate=true`），动作、命中规则、告警键完全一致；
  并发重复投递同样安全（内部加锁，已用 `-race` 验证）。
- **迟到**：维护已观测最大业务时间（水位线）；当
  `OccurredAt + lateAfter < 水位线` 时标记 `IsLate=true`。迟到事件仍被
  正常评估并告警，但不会回退任何状态或计数。
- **乱序**：规则与去重全部基于**业务时间 (OccurredAt)** 而非到达顺序；
  对同一批事件，无论投递顺序如何，同一 EventID 的动作、获胜规则、失败
  原因一致（见 `TestOutOfOrderSameSetNoContradiction`）。
- 有状态规则（velocity）按账户维护 `EventID -> OccurredAt` 集合，在
  `(t-Window, t]` 业务时间窗口内计数，重复 EventID 被集合吸收。

## 日志可解释性

每个事件均打印：输入（event/tx/账户/簇/金额/币种/国家/业务时间/摄入时间/
late 标记）→ 每条命中（规则、优先级、动作、降级标记、失败原因、理由与
细节）→ 最终裁决依据（winning rule、verdict、命中条数）→ 告警记录与
去重结果。测试 `TestLoggingContainsInputAndBasis` 对此做断言。

## 验证方法

```bash
go build ./...          # 编译全部包与 demo
go vet ./...            # 静态检查
go test ./...           # 单元 + 端到端测试
go test -race ./...     # 竞态检测（并发重复投递）
go test -cover ./...    # 覆盖率（engine/processor >96%，rule/alert >89%）
go run ./cmd/demo       # 交互式场景演示，末尾打印告警与计数守恒汇总
```

测试覆盖矩阵：

| 场景 | 测试 |
| --- | --- |
| 重复投递 → 判定稳定、计数守恒 | `TestDuplicateDeliveryStableVerdictAndCount`、`TestRecordDedupCountConservation` |
| 并发重复投递 | `TestConcurrentDuplicateDeliveryCountConservation` |
| 乱序同一批事件无矛盾结论 | `TestOutOfOrderSameSetNoContradiction` |
| 迟到事件标记、拦截、只计一次 | `TestLateEventFlaggedAndCountedOnce` |
| 规则同时命中/冲突优先级 | `TestConflictResolvedByPriority` |
| velocity 窗口与重复幂等 | `TestVelocityWindowAndIdempotence` |
| 非法字段降级 REVIEW | `TestInvalidFieldDegradesToReview`、`TestValidate` |
| 必需规则缺失 fail-closed | `TestMissingRequiredRuleFailsClosed` |
| 规则 error/panic 降级且原因可区分 | `TestRuleErrorAndPanicDegradation` |
| 状态机单向、终态拒绝 | `TestStateMachineOneWay`、`TestManualReviewStateFlowEndToEnd` |
| 簇合并 / 冷却桶 | `TestClusterMergesAcrossRules`、`TestCooldownBucketing` |
| 日志含输入与判定依据 | `TestLoggingContainsInputAndBasis` |

## 设计边界（诚实声明）

有状态规则在"流处理进行到一半"时，若一个事件先于同窗口内其他事件被摄入，
其即时结论可能不同（计数依赖已摄入前缀）。组件对外部承诺的一致性是
**EventID 级别的**：同一事件（含其任意重放）结论恒定，且同一批输入在任意
投递顺序下，每个 EventID 的最终结论一致——通过"业务时间窗口 + 首次判定
缓存回放"实现，而不是重写规则内部状态。
