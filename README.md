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
| `internal/processor` | 总装管线：幂等去重、迟到水位线、告警装配、对外入口（含历史回填摄入入口） |
| `internal/backfill` | 历史流水批量回填：分块外部排序、k 路归并、微批检查点、暂停/重启续跑 |
| `internal/logging` | 标准库 logger 与线程安全内存 logger（供测试断言） |
| `cmd/demo` | 本地演示：正常/冲突/重复/乱序/迟到/降级/状态流转全场景 |
| `cmd/backfill-demo` | 回填端到端演示：乱序+重复+大跨度 JSONL 批次、暂停续跑、与升序参考流逐笔比对 |

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

## 历史流水批量回填 (`internal/backfill`)

在实时管线之上新增一层回填能力，解决"一大批业务时间跨度很大、批内乱序、还夹着
重复的旧流水"无法逐条喂入、中途失败无法续跑的问题。

### 用法

```go
store, _   := backfill.NewDiskStore("/var/lib/fraud/backfill") // 或 backfill.NewMemStore()
runner     := backfill.NewRunner(store, logger)
result, err := runner.Run(ctx, processor, backfill.Config{
    JobID:          "reconcile-2026Q2", // 续跑时复用同一 JobID
    ChunkSize:      10_000,             // 排序阶段单块内存上限
    MicroBatchSize: 500,                // 归并阶段检查点节奏
    Source:         backfill.NewJSONLFileReader("history.jsonl"),
})
```

输入源只需实现可重复打开的 `InputReader`（内置 JSONL 文件读取器
`NewJSONLFileReader` 与内存切片适配器 `SliceReader`）。每行 JSON：

```json
{"seq": 1, "tx": {"event_id": "e1", "tx_id": "t1", "account_id": "a1",
                  "amount": 50, "currency": "USD", "country": "US",
                  "event_type": "PAY",
                  "occurred_at": "2026-06-01T00:00:00Z",
                  "ingested_at": "2026-06-01T00:00:01Z"}}
```

`seq` 可省略（按行号生成）。`(OccurredAt, EventID, Seq)` 构成全序游标，即使同
EventID 同业务时间也能唯一定位。

暂停/续跑：`ctx` 取消是协作式的——当前块/微批写完、检查点落盘后返回
`Paused=true`；用同一 `JobID` 再次 `Run` 即从上次提交点继续。进程重启后用
`RestoreInto(ctx, 新processor, jobID)` 把块按归并顺序重放一遍重建引擎/告警状态
（processor 的 EventID 幂等保证重放恰好一次），随后再 `Run` 收尾。

### 与"逐条升序正常处理"的等价性是怎么做到的

1. **排序阶段（外部排序）**：读源，内存缓冲到 `ChunkSize` 就排序落盘成一个
   chunk（JSONL），`ChunkSize` 即内存上限——批大小只影响 chunk 数量，不影响
   内存峰值。每个 chunk 落盘都更新清单，排序中途崩溃也能跳过已落盘前缀。
2. **归并阶段（k 路最小堆）**：同时打开所有 chunk，每路只保留堆顶一条，按
   `(OccurredAt, EventID, Seq)` 全局升序吐出记录。因此摄入 processor 的顺序
   严格等价于"这批数据按业务时间从早到晚一条条喂"。
3. **去重不额外造状态**：批内重复（同 EventID 多条）交给 processor 已有的
   EventID 幂等层吸收——升序流里最早出现的那笔成为权威判定，后续重放返回相同
   Verdict、不重复计数。velocity 规则对已知 EventID 只保留最早业务时间，保证
   "同 EventID 更晚时间的重复副本"不污染窗口状态。
4. **微批检查点**：每 `MicroBatchSize` 条提交一次清单（游标、累计 applied/
   duplicate 数、历史高水位、告警操作状态）。续跑时跳过 `<= cursor` 的记录。

`TestBackfillEquivalentToAscendingReference` 对一个乱序、重复、跨两个月的批次
逐字段比对两种路径（动作/获胜规则/全部命中/告警键/每告警 HitCount/FirstSeen/
LastSeen）完全一致。

### 与实时处理、告警聚合的兼容边界

| 主题 | 保证 |
| --- | --- |
| 回填期间实时并发 | 回填走独立入口 `IngestHistorical`，与 `Process` 共用同一把 processor 锁与同一张 EventID 幂等表；同一 EventID 无论从哪条路径先到，只有一个权威结论（另一路径等待并回放），告警计数不重不丢。见 `TestBackfillConcurrentWithLive`、`TestConcurrentHistoricalAndLiveSameEvent`（`-race` 验证）。 |
| 迟到/乱序平滑衔接 | 回填期间**不**因实时水位线远高于历史时间而把历史事件误标 late（否则补旧数据会把所有正常事件标成异常）；回填期间实时水位线照常推进。回填**完成后**才用历史高水位**单调抬升**实时水位线——只升不降，所以纯历史回填不会让既有 late 判断、告警计数、首末时间或状态发生回退。见 `TestHistoricalIngestionNotFlaggedLateAndSeamsWatermark`、`TestWatermarkAdvancedForwardAfterNewerBackfill`。 |
| 告警聚合 | 回填产生的告警与实时告警进入同一个 `alert.Store`、同一套 `(规则,账户,冷却桶)/cluster` 去重键；EventID 集合计数天然吸收跨路径重复。回填把某条已存在告警的 FirstSeen 向更早修正、或把 HitCount 补齐到"完整输入本该有的值"，这是与参考终态对齐，而非状态回退；操作状态（ACKED/RESOLVED）只在单向状态机上向前合并，检查点里持久化的 Ack/Resolve 在重启重放后不丢失（`TestBackfillRestartPreservesAlertState`）。 |
| 重复出现的同一笔 | 批内多次、跨批次、历史与实时重叠，统一只算一次（最早业务时间的副本权威）。 |
| 计数守恒 | 暂停 N 次后续跑、甚至换进程重放重建，最终每条告警 `HitCount = 唯一 EventID 数`，applied + 幂等吸收数守恒（`TestBackfillPauseAndResumeConservesCounts`、`TestBackfillRestartRebuildsProcessor`）。 |

**诚实边界**：回填的等价性是针对"被回填的这批数据 + 已落定状态"按业务时间升序
的终态；回填进行中、业务时间落在**尚未回填区间内**的实时事件，仍沿用实时系统
"基于已摄入前缀"的既有语义（见文末设计边界）。回填完成（水位线衔接）之后到达
的新数据则完全按衔接后的水位线判定。实时运行态本身仍是纯内存组件，进程崩溃后
的实时部分恢复不在回填重建范围内（回填重建的是历史作业对应的引擎窗口与告警
状态）。

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
go run ./cmd/demo                    # 实时场景演示，末尾打印告警与计数守恒汇总

# 回填端到端演示（生成乱序+重复+大跨度 JSONL，逐笔比对升序参考流）：
go run ./cmd/backfill-demo ./bf-work            # 全量回填
go run ./cmd/backfill-demo ./bf-work --pause    # 归并途中取消，再续跑
ls ./bf-work/backfill-state                     # 分块 JSONL 与清单（完成后自动清理）
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
| 回填结论与升序参考流逐笔一致（乱序+重复+大跨度） | `backfill.TestBackfillEquivalentToAscendingReference` |
| 回填内存有界（批远大于单块） | `backfill.TestBackfillBoundedMemoryManyChunks` |
| 磁盘分块/清单落盘与重载 | `backfill.TestBackfillDiskStoreRoundTrip`、`backfill.TestJSONLFileReaderRoundTrip` |
| 历史与实时 EventID 重叠只计一次 | `backfill.TestBackfillIdempotentWithLiveState`、`processor.TestHistoricalAndLiveShareIdempotence` |
| 回填与实时并发一致 | `backfill.TestBackfillConcurrentWithLive`、`processor.TestConcurrentHistoricalAndLiveSameEvent` |
| 历史不误标 late、水位线单调衔接 | `processor.TestHistoricalIngestionNotFlaggedLateAndSeamsWatermark`、`processor.TestWatermarkAdvancedForwardAfterNewerBackfill` |
| 归并途中取消、多次续跑计数守恒 | `backfill.TestBackfillPauseAndResumeConservesCounts` |
| 进程重启重放重建、终态与参考一致 | `backfill.TestBackfillRestartRebuildsProcessor` |
| 崩溃前 ACK 不回退、计数不重 | `backfill.TestBackfillRestartPreservesAlertState`、`alert.TestSnapshotAndRestoreStatesMonotone` |

## 设计边界（诚实声明）

有状态规则在"流处理进行到一半"时，若一个事件先于同窗口内其他事件被摄入，
其即时结论可能不同（计数依赖已摄入前缀）。组件对外部承诺的实时一致性是
**EventID 级别的**：同一事件（含其任意重放）结论恒定，且同一批输入在任意
投递顺序下，每个 EventID 的最终结论一致——通过"业务时间窗口 + 首次判定
缓存回放"实现，而不是重写规则内部状态。

**回填把这个边界对历史数据收紧到了"批次终态"**：回填在摄入前先做分块外部
排序，使有状态规则观察到的历史前缀就是业务时间升序的完整前缀，所以回填批次
的每笔结论都与"升序逐条处理"完全一致，而不仅是 EventID 级别一致。未被回填
覆盖的实时区间仍遵循上面的实时语义；如上文兼容边界所述，回填进行中落在未回填
区间内的实时事件不享受这种批次终态保证。
