# 本地实时交易风控与合规告警组件（Go）

用本地交易流水与规则配置模拟实时交易监测。组件保证在**重复、迟到或乱序事件**
下，规则命中、告警去重与人工复核的结论**一致、可解释、状态不回退、计数守恒**。

仅依赖 Go 标准库。

```
go run ./cmd/fraud-demo      # 运行本地流水模拟（含重复/乱序/迟到/异常）
go test ./... -race          # 全量测试（含并发竞态检测）
go vet ./...
```

## 1. 总体结构

| 文件 | 职责 |
| --- | --- |
| `engine/types.go` | 交易、规则、定论(`Verdict`)、命中依据(`RuleHit`)、告警等核心类型 |
| `engine/registry.go` | 规则注册表：按稳定优先级（Priority 升序，并列按规则 ID 字典序）列出规则 |
| `engine/rules.go` | 四条内置规则：黑名单、大额阈值、高风险地区、短时高频 |
| `engine/evaluator.go` | 多规则组合评估、优先级仲裁、三类异常降级 |
| `engine/velocity.go` | 按业务事件时间维护的速度索引（计数只看“本笔之前”的历史） |
| `engine/blacklist.go` | 名单存储抽象（正常实现 + 始终失败实现），区分“不在名单”与“查询失败” |
| `engine/alert.go` | 告警去重/聚合、单向状态机、计数守恒、深拷贝快照 |
| `engine/engine.go` | 对外入口：幂等缓存、watermark + 迟到窗口、乱序重评、日志、批量摄入 |

## 2. 定论（每笔交易的唯一结论）

定论只有三种，按强度排序：

```
ALLOW(放行, 0)  <  REVIEW(转人工复核, 1)  <  BLOCK(拦截, 2)
```

每笔交易的 `Verdict` 同时保留：最终 `Action`、**全部命中规则及其判定依据**
（规则 ID、优先级、风险分、人类可读原因）、以及（发生降级时）可区分的失败原因。

### 2.1 规则优先级与冲突仲裁

- 每条规则声明 `Priority()`，**数值越小优先级越高**；`Severity()` 声明该规则
  命中时隐含的定论（黑名单/大额/地区 = `BLOCK`，高频 = `REVIEW`）。
- 多条规则同时命中时，取**优先级最高**（Priority 最小；并列取规则 ID 字典序最小）
  的命中规则的 `Severity` 作为最终定论。规则评估顺序由注册表稳定排序确定，
  与注册顺序、投递顺序均无关，因此结论确定、可复现。
- 内置默认优先级：

  | 规则 | ID | Priority | 命中定论 |
  | --- | --- | --- | --- |
  | 黑名单主体 | `RULE_BLACKLIST` | 10 | BLOCK |
  | 大额阈值 | `RULE_LARGE_AMOUNT` | 20 | BLOCK |
  | 高风险地区 | `RULE_HIGH_RISK_COUNTRY` | 30 | BLOCK |
  | 短时高频 | `RULE_VELOCITY` | 40 | REVIEW |

## 3. 重复、迟到与乱序事件下的一致性

- **幂等去重**：以 `TxnID` 为唯一键缓存定论。重复投递返回同一判定对象，
  不会重复计数、不会重复告警。
- **业务事件时间为准**：高频规则的窗口计数只统计**严格早于本笔事件时间**的
  历史（`[t-window, t)`），不含本笔自身，也不看“未来”事件。因此首次评估与
  乱序到达后的重评得到同一计数。
- **watermark + 迟到容忍窗口**（默认 30s，可由 `Config.LateGrace` 配置）：
  - 窗口内的乱序/迟到事件被缓冲为“暂定论”；每当更老事件到达，相关待决事件按
    **各自事件时点**重评。暂定论只允许沿 `ALLOW -> REVIEW -> BLOCK` **单调增强**，
    不会从可疑回退为放行。
  - 待事件时间越过 `watermark - grace`（更老事件不可能再来）后**最终定稿**，
    定论不可变；流平息后，逆序/乱序投递与按序投递对每个 `TxnID` 的最终定论一致。
  - 超出窗口的迟到事件按其时点已知历史立即最终定稿，并明确记录为迟到。
- **批量摄入**：`IngestBatch` 先按事件时间（并列按 TxnID）稳定排序再处理，
  返回切片与输入位置一一对应。
- 定论对象指针在重评/定稿过程中保持稳定（原地更新），调用方早期持有的
  `*Verdict` 始终指向最新定论。

## 4. 告警去重、聚合与状态流转

- **去重键**：`acct:<主体>|day:<UTC业务日>`，即“同一主体同一业务日”为一个交易簇。
- 只有非 `ALLOW` 定论产生/更新告警；同簇多笔命中聚合为一条告警。
- `Count` 与 `TxnIDs` 严格守恒（`Count == len(TxnIDs)`）；同一 `TxnID` 因重评
  多次写入不会重复计数。先前按 ALLOW 评估、重评后变为可疑的交易会被幂等补记。
- 告警严重度单调增强：`REVIEW -> BLOCK`，不会被更弱结论稀释。
- **单向状态机**（乱序/重复命令均被拒绝，状态绝不回退）：

  ```
  OPEN ──► IN_REVIEW ──► RESOLVED
   │           │
   └────────── ► RESOLVED（可越级直接终结）
  ```

  被拒绝的命令返回**可区分**的错误：
  `ErrAlertNotFound`（未知簇，如迟到命令引用不存在的键）、
  `ErrDuplicateTransition`（重复流转同状态）、
  `ErrIllegalTransition`（回退/越级）。每次成功流转 `Version` 自增。

## 5. 异常数据的降级策略

保守降级策略 `ConservativeDegradePolicy()`，**绝不静默放过可疑交易**：

| 失败原因（`DegradeReason`，可区分） | 触发场景 | 默认定论 |
| --- | --- | --- |
| `INVALID_FIELD` | TxnID/主体为空、金额 ≤ 0、币种/地区为空、事件时间为零值 | **BLOCK** |
| `RULE_MISSING` | 规则注册表为空，无法做出可信监测 | **REVIEW** |
| `DEPENDENCY_FAILED` | 判定依赖的内部环节失败（如黑名单存储查询出错） | **REVIEW** |

- 字段非法在进入规则评估前即拦截，且不污染速度索引。
- 名单查询把“(false, error)”与“(false, nil)”严格区分：存储出错走
  `DEPENDENCY_FAILED` 降级，而不是误判为“不在名单”。
- 若已有**优先级更高且正常命中**的规则，其定论优先；否则按降级策略给出
  拦截/复核。`Verdict.Degradation` 记录原因、细节与来源规则，便于区分与审计。

策略本身由 `DegradePolicy` 暴露三个动作字段，可按业务替换。

## 6. 可解释日志

`Config.Logger` 接受最小日志接口（默认输出到 stderr）。每笔交易打印：

1. **输入**：TxnID、主体、金额/币种、地区、业务事件时间；
2. **命中依据**：每条命中规则的 ID、优先级、风险分与原因；
3. **降级信息**（如有）：原因、细节、来源规则；
4. **定论与告警**：最终 Action，以及告警键、状态、严重度、去重后计数。

## 7. 验证方法与覆盖场景

```bash
go test ./... -race -v
```

测试覆盖：

- **重复事件**：同一交易重复/并发（`-race`）投递，定论对象与计数恒定。
- **乱序/逆序事件**：按序与完全逆序喂入，流平息后每个 TxnID 最终定论一致；
  暂定论只能单调增强；`IngestBatch` 排序确定性。
- **迟到事件**：窗口内迟到触发重评并收敛；超窗迟到立即最终定稿且不可变。
- **规则冲突**：多规则命中按优先级与 ID 次序仲裁；黑名单压过其他规则；
  高优先级正常命中覆盖低优先级依赖失败。
- **告警去重/状态流转**：同簇合并、`Count==len(TxnIDs)` 守恒、严重度单调增强、
  状态机三类拒绝原因可区分、深拷贝快照隔离。
- **异常降级**：`INVALID_FIELD / RULE_MISSING / DEPENDENCY_FAILED` 三原因
  各自定论与来源可区分；黑名单失败不等于“不在名单”。

## 8. 最小使用示例

```go
reg := engine.NewRegistry()
reg.Register(engine.NewBlacklistRule("RULE_BLACKLIST", 10))
reg.Register(engine.NewAmountThresholdRule("RULE_LARGE", 20, 10000))
reg.Register(engine.NewVelocityRule("RULE_VELOCITY", 40, 10*time.Minute, 2))

e := engine.NewEngine(reg, engine.Config{
    Blacklist: engine.NewMemoryBlacklistStore([]string{"ACCT-BLACK"}),
    Policy:    engine.ConservativeDegradePolicy(),
})

v, alert := e.Ingest(ctx, engine.Transaction{ /* TxnID / AccountID / Amount / ... */ })
fmt.Println(v.Action, v.Hits)          // 唯一定论 + 可解释命中依据
e.TransitionAlert(ctx, alert.Key, engine.StatusInReview, time.Now())
```
