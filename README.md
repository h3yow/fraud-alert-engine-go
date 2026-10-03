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
| `internal/backfill` | 历史流水批量回填：分批落盘、归并重放、断点续跑 |
| `internal/logging` | 标准库 logger 与线程安全内存 logger（供测试断言） |
| `cmd/demo` | 本地演示：正常/冲突/重复/乱序/迟到/降级/状态流转/批量回填全场景 |

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

## 历史流水批量回填（backfill）

`internal/backfill` 在实时管线之上提供可断点续跑的历史批量回填，用于把
跨度大、批内乱序、夹带重复的存量流水补进同一个 `Processor`。

### 使用方式

```go
bf := backfill.New(proc, backfill.Options{
    Dir:         "/var/lib/fraud/backfill", // 会话状态（run 文件 + 断点）持久化目录
    Logger:      logger,
    MaxOpenRuns: 32,                         // 归并路数上限，超出自动压实
})
sess, _ := bf.Begin("2026-09-history")       // 同 id 重复 Begin 即"续跑"
for chunk := range chunkSource {             // 每块任意乱序、可含重复
    sess.Submit(chunk)                       // 块内排序后落盘为 sorted run
}
st, _ := sess.Run(ctx)                       // 全局归并，按业务时间重放；ctx 取消即安全停止
fmt.Println(st.Applied, st.Done)
```

- **内存有界**：`Submit` 只把当前一块排序落盘；`Run` 做 k 路归并，内存占用
  为"每路一条记录"。run 文件数超过 `MaxOpenRuns` 时自动压实（同样是流式
  归并），因此可以处理明显大于内存的批量数据。
- **断点续跑**：每应用一笔，就把"游标 `(OccurredAt, EventID)` + 整个处理器
  状态"在同一个 `checkpoint.json` 里原子落盘（tmp + rename）。处理器状态
  包括：**逐笔首次判定缓存、业务时间水位线、有状态规则（velocity）的窗口
  集合、全部告警（EventID 计数集合、首末发生时间、Reasons，以及
  OPEN/ACKED/RESOLVED 生命周期状态）**。`ctx` 取消、进程停止后，用同一
  session id 重新 `Begin` + `Run` 即可继续：
  - **进程重启、组件从空状态起来**（真实部署形态）：`Begin` 检测到 checkpoint
    有已应用进度且当前处理器为空时，先把处理器完整恢复到游标时刻，再从游标
    之后继续重放；游标之前的记录跳过、但其全部效果（计数、扎堆窗口、水位线、
    告警状态）已经通过快照恢复——已处理的不重计，未处理的不漏。
  - **同进程内中断续跑**（仅取消 ctx、组件还在）：处理器内存中本就保留着
    游标前缀的全部效果，续跑直接跳过前缀；若挂载的非空处理器**不包含**
    游标对应的状态（挂错了处理器），`Begin` 显式报错而不是静默分叉历史。
- **一致性承诺**：回填结束后，每一笔的定论、命中规则（含完整 Hits）、归属
  告警与计数，与"这批数据按 **EventID 去重后**按业务时间从早到晚逐条走正常
  实时路径"完全一致。同一 EventID 在批内出现多次、或跨块与已有记录撞同一
  标识（即便携带不同 OccurredAt/载荷），只保留最早业务时间的那一笔，只算
  一次（`TestBackfillRestartEmptyComponentResume`、
  `TestBackfillEventIDCollisionAcrossChunks` 逐事件、逐告警对比）。

#### 中断后重启续跑：保留什么、不保留什么（兼容边界）

| 状态 | 是否跨重启保留 | 说明 |
| --- | --- | --- |
| 逐笔首次判定（Action/WinningHit/全部 Hits/AlertKey/降级标记） | 保留 | 恢复后重复投递仍回放同一判定（`IsDuplicate=true`） |
| velocity 等有状态规则的窗口集合 | 保留 | 按账户的 `EventID -> OccurredAt` 全表恢复，短时间扎堆判定不因重启丢失 |
| 业务时间水位线 | 保留 | 单调不减；回填与之后实时流水的迟到/乱序判断平滑衔接 |
| 告警计数（EventID 集合 / HitCount） | 保留 | 只增不减，不重计、不丢 |
| 告警 FirstSeen/LastSeen | 保留 | 维持业务时间最小/最大语义 |
| 告警生命周期 OPEN/ACKED/RESOLVED | 保留 | 操作员的 Ack/Resolve 不会因续跑丢失或回退；终态拒绝仍生效 |
| 已提交未落盘 run 文件（Submit 进行中崩溃） | 不保留 | run 文件写完才更新提交计数，半成品 run 不会被读取（以完整 `.jsonl` 为准） |
| 与回填会话无关的实时事件（发生在两次 checkpoint 之间且进程硬崩溃） | 见下方诚实声明 | 快照只捕获最近一次"应用/提交"时刻的全量状态 |

- **不凭空标异常**：历史事件按业务时间重放，永不标记 `IsLate`，恢复后也不会
  把正常历史流水补标成迟到。
- **配置需一致**：恢复会校验快照版本与冷却窗口（bucket 键依赖它）；快照里
  有而当前未注册的必需/有状态规则会显式报错（fail-closed，拒绝用错误配置
  续跑），而不是静默丢状态。
- **只允许恢复进空组件**：`Processor.RestoreState` 拒绝覆盖任何已有判定/水位
  线/告警，避免把两段不同历史静默合并。
- **崩溃窗口（诚实声明）**：
  1. "应用一笔"与"快照落盘"之间极窄窗口内硬崩溃：重启后该笔会被重放一次，
     但规则状态按 EventID 幂等、告警按 EventID 集合去重、判定缓存回放，不
     产生第二次计数，结论不变。
  2. 回填运行期间若同时有实时流水写处理器，而硬崩溃恰好发生在"实时事件已
     入内存、下一次回填快照之前"，该实时事件不在快照内——它与任何崩溃时的
     纯内存系统一样需要由上游重投（处理器对重投幂等，重放结论一致）。
  3. 快照在每次应用与每次 Submit 后落盘，因此上述窗口最多只涉及单笔记录。

### 与实时处理/告警聚合的兼容边界

- **并发安全**：回填走 `Processor.ProcessHistorical`，与实时
  `Process` 共用同一把处理器锁——同一 EventID 无论从哪条路径先到，
  只有第一次评估生效，之后一律回放缓存判定，不会出现两个结论；
  告警侧靠 EventID 集合去重，计数不重复也不丢。
- **不凭空标异常**：历史事件按业务时间重放，永不标记 `IsLate`；水位线
  只单调前进，回填结束后新来实时事件的迟到判断基于最新业务时间平滑
  衔接。
- **不回退**：告警计数只增不减、`FirstSeen/LastSeen` 维持业务时间
  最小/最大语义（回填更早的历史事件可能合法地把 `FirstSeen` 提前，这是
  补齐真实最小值而非回退）、告警状态机不受回填影响，且 Ack/Resolve 的
  生命周期状态随快照跨重启保留。
- **同一笔只算一次**：归并按 **EventID** 全局去重（同标识保留最早业务时间
  记录，跨块、不同时间戳撞车同样只算一次）；与实时路径撞车的 EventID 由
  处理器幂等缓存吸收。
- **提交顺序约束**：会话产生已应用进度后，新 `Submit` 的块必须整体晚于
  进度游标，否则返回错误（宁可显式拒绝也不静默丢数据）。实践中把所有 chunk
  在 Run 之前/之间提交完毕即可，重启续跑不要求重新提交（run 文件已在盘上）。

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
go test -race ./...     # 竞态检测（并发重复投递、回填与实时并发）
go test -v ./internal/backfill/   # 回填场景明细日志（业务覆盖矩阵、用例结果、游标、计数、结论）
go test -cover ./...    # 覆盖率（engine/processor >96%，rule/alert >89%）
go run ./cmd/demo       # 交互式场景演示，末尾含批量回填与"空状态重启续跑"演示
```

专门验证"跑到一半停掉、换成全新空状态重启后继续"这条路径：

```bash
# 主回归：两次空状态重启，逐笔结论/命中规则/告警归属/计数/生命周期对比一次性顺序处理
go test -v -run TestBackfillRestartEmptyComponentResume ./internal/backfill/
# 同 EventID 跨块撞车（不同时间戳/载荷）只算最早那笔，且重启后结论一致
go test -v -run TestBackfillEventIDCollisionAcrossChunks ./internal/backfill/
# 挂错非空处理器续跑必须被显式拒绝；空状态重启仍能补齐
go test -v -run TestBackfillRestartRejectsForeignNonEmptyProcessor ./internal/backfill/
# 组件级快照往返（判定缓存/水位线/velocity/告警含生命周期）
go test -v -run 'TestProcessorSnapshotRestoreRoundTrip|TestProcessorRestoreGuards' ./internal/processor/
go test -v -run TestStoreSnapshotRestoreRoundTrip ./internal/alert/
go test -v -run TestVelocitySnapshotRestore ./internal/rule/
# 既有场景不回归（批内乱序重复、压实、进程内续跑、回填与实时并发）
go test -race -v ./internal/backfill/
# 端到端人工观察：phase1 应用 4 笔后丢弃整个组件，phase2 空状态续跑，逐笔打印 ok/MISMATCH
go run ./cmd/demo | sed -n '/restart-from-empty/,$p'
```

`go test -v ./internal/backfill/` 开头会打印「backfill 业务覆盖矩阵」
（每条业务由哪个用例覆盖），结尾打印每个用例的 `result=PASS/FAIL` 汇总；
用例内部的 `scenario=...` 日志行给出输入规模、断点游标、应用/跳过计数与
断言结论。

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
| 批量回填 ⇔ 按业务时间逐条处理等价 | `TestBackfillMatchesSequentialByBusinessTime` |
| 批内乱序+重复、分块落盘与压实 | `TestBackfillChunkedStreamingAndGuards` |
| 回填中途停止、进程内续跑、计数守恒 | `TestBackfillStopAndResumeCountConservation` |
| **空状态重启续跑**：逐笔结论/命中规则/告警归属/计数/首末时间/生命周期与一次性顺序处理完全一致（多次重启） | `TestBackfillRestartEmptyComponentResume` |
| 同 EventID 跨块/不同时间戳撞车只算一次，重启后结论一致 | `TestBackfillEventIDCollisionAcrossChunks` |
| 挂错非空处理器续跑显式拒绝 | `TestBackfillRestartRejectsForeignNonEmptyProcessor` |
| 处理器快照往返（判定缓存/水位线/velocity/告警）与空状态保护 | `TestProcessorSnapshotRestoreRoundTrip`、`TestProcessorRestoreGuards` |
| 告警快照往返（计数/首末时间/Reasons/生命周期） | `TestStoreSnapshotRestoreRoundTrip` |
| velocity 规则快照往返（窗口集合行级一致） | `TestVelocitySnapshotRestore` |
| 回填与实时并发：定论唯一、计数守恒、无回退 | `TestBackfillConcurrentWithRealtime` |

回填测试运行时通过 `go test -v ./internal/backfill/` 可先看到「业务覆盖
矩阵」（业务 × 用例），再看到每个场景的输入规模、断点游标、应用/跳过计数
与 `result=PASS` 结论（`scenario=...` 日志行），结尾有用例结果汇总。

## 设计边界（诚实声明）

有状态规则在"流处理进行到一半"时，若一个事件先于同窗口内其他事件被摄入，
其即时结论可能不同（计数依赖已摄入前缀）。组件对外部承诺的一致性是
**EventID 级别的**：同一事件（含其任意重放）结论恒定，且同一批输入在任意
投递顺序下，每个 EventID 的最终结论一致——通过"业务时间窗口 + 首次判定
缓存回放"实现，而不是重写规则内部状态。
