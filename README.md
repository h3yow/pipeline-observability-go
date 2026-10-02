# pipeline-observability-go

本地流水线作业的**可观测性与告警组件**：消费作业运行事件（run events），输出
新鲜度（freshness）、SLA 判定与告警生命周期结论。针对本地“至少一次投递”的事件流，
在**乱序、重复、并发摄入**下给出**一致且可解释**的结论。

仅使用 Go 标准库，无外部依赖。

## 目录结构

| 包 | 职责 |
| --- | --- |
| `internal/event` | 版本化事件信封、解码与严格字段校验 |
| `internal/model` | 运行聚合状态、判定结论、告警等领域类型 |
| `internal/rule` | 版本化的新鲜度 / SLA 规则注册表（不可变） |
| `internal/ingest` | 并发摄入：去重、乱序缓冲、迟到判定、规范化重建；缺口证据、待补全视图、显式补数 |
| `internal/eval` | 新鲜度与 SLA 判定（三态 PASS/FAIL/UNKNOWN，含阈值、实测值、文字依据） |
| `internal/alert` | 防抖告警生命周期状态机 OK→FIRING→RECOVERED；补数后规范化重算 |
| `internal/engine` | 编排以上模块；规则版本绑定、按事件时间顺序评估 |
| `cmd/demo` | 可运行的脚本化场景演示 |

## 1. 事件模型

事件信封 `event.Envelope`：

| 字段 | 说明 |
| --- | --- |
| `EventID` | 全局唯一事件 ID，用于去重（必填） |
| `SchemaVersion` | 事件结构版本号，选择对应解码器（必填，未知版本直接拒绝） |
| `Type` | `run.started` / `run.finished` |
| `OccurredAt` | **事件时间**（业务发生时间），所有排序 / 水位 / 判定均基于它（必填） |
| `IngestedAt` | 摄入时间（墙钟），仅用于观测 |
| `RunID` / `JobID` | 运行 / 作业标识（必填） |
| `Payload` | 版本相关字段：`attempt`、`result` |

Schema **v1**：

- `run.started`：`attempt` 可选，缺省按 schema 显式规定为 `1`（用 `HasAttempt` 区分“缺失”与“显式 0”）。
- `run.finished`：`result` **必填**，取值 `SUCCESS` / `FAILED`；`attempt` 可选。

**不静默默认**的原则：

- 未知 `SchemaVersion` → `ErrUnknownVersion`；
- 必填字段缺失（如 finished 缺 `result`、缺 `EventID`、缺 `OccurredAt`）→ `ErrMissingField`；
- 字段类型/取值非法 → `ErrBadField`；
- 只有 schema **显式声明**的默认值（v1 的 `attempt=1`）才会被采用，并在解码结果中
  用 `HasAttempt=false` 标记其为默认值。

## 2. 摄入：去重、乱序、迟到与可解释原因

`ingest.Store` 用单把互斥锁串行化写入，并为每个被接受的事件分配单调递增的
**摄入序号 seq**。每个事件返回一张 `Receipt`，其 `Disposition` 互斥地说明处理原因：

| Disposition | 含义 | 是否接受 |
| --- | --- | --- |
| `INVALID` | 信封校验失败（未知版本 / 缺字段 / 非法值），原因见 `Reason` | 否 |
| `DUPLICATE` | 相同 `EventID` 已处理过；返回**首次**的处理结论，绝不重复应用 | 否 |
| `CONFLICT` | 不同 `EventID` 但与同一 run 的同一阶段冲突（或 run 归属不同 job）；**保留先到状态，拒绝后来者，不覆盖** | 否 |
| `OUT_OF_ORDER_BUFFERED` | 该 run 已有更晚的事件先到；事件被缓存，聚合按事件时间重排后**延迟应用** | 是 |
| `LATE` | 事件时间早于水位 `maxObservedOccurredAt − allowLateness`，run 窗口已关闭 | 否 |
| `APPLIED` | 立即应用 | 是 |
| `BACKFILLED` | 通过 `Backfill` 显式补入（归档/人工恢复），**仅豁免迟到窗口**；其余规则照常、聚合规范化重建 | 是 |

**一致性关键——规范化重建（canonical rebuild）**：每次接受事件后，run 聚合都由其
已接受事件集合按 `(OccurredAt, EventID)` 排序**纯函数式重建**。因此聚合状态只取决于
“最终接受了哪些事件”，与到达顺序、goroutine 调度无关；乱序喂入与顺序喂入得到
完全相同的 `RunState`。`AppliedEventIDs` / `AppliedSeq` 记录状态出处，便于解释与审计。

水位是全局事件时间概念：`watermark = max(已接受事件的 OccurredAt) − allowLateness`。
迟到窗口应配置为能覆盖一次运行的最大时间跨度（演示与测试使用 6h / 1h）；超出窗口
的迟到数据被明确丢弃并给出 `LATE` 原因，而不是污染已关闭的 run。

## 3. 新鲜度与 SLA 判定规则

规则 `rule.Set` 为不可变、版本化对象 `(RuleID, Version)`：

- `FreshnessDeadline`：`FinishedAt ≤ StartedAt + FreshnessDeadline` 则 **FRESH**，否则 **STALE**；
- `SLADuration`：`FinishedAt − StartedAt ≤ SLADuration` 则 **WITHIN_SLA**，否则 **SLA_BREACH**。

阈值边界采用 `<=`（**等于阈值即达标**）。每条结论都带阈值、实测值与文字依据，例如：

```
duration=15m0s limit=10m0s => SLA_BREACH (rule job-a v1)
finished=...10:15Z expectedBy=...10:30Z deadline=30m0s => FRESH (rule job-a v1)
```

### 未知结论（时间基准不全）

新鲜度与 SLA 都依赖完整的时间基准（开始 + 结束）。当一次运行**只有结束事件、没有开始事件**
时，系统绝不臆测达标与否：

- `Freshness.Status` / `SLA.Status` 为三态枚举：`PASS` / `FAIL` / `UNKNOWN`
  （旧的布尔字段 `OK` 仍保留，仅在非 UNKNOWN 时有意义；UNKNOWN 时为 `false`）；
- 结论 `Conclusion.Determinable = false`，`Conclusion.IsUnknown()` 为真，`Breaches` 为空；
- **未知运行不进入告警生命周期**：不产生任何告警，也不计入连续触发（debounce）或连续恢复计数。
  因此 3 次未知之后出现 2 次真实 breach，仍需恰好 2 次才 FIRING；
- 基准齐全的运行，判定与告警行为与此前**完全一致**，不受影响。

缺口原因由摄入层从“已接受事件集 + 被拒候选索引”**纯函数式**推导，记录在
`Conclusion.MissingBasis[0]` 与 `RunState.BasisGap` 上：

| `GapCode` | 含义 | 证据 |
| --- | --- | --- |
| `START_NOT_ARRIVED` | 从未观测到开始事件（可能还在路上，或可补数） | 无证据事件 |
| `START_DROPPED_LATE` | 开始候选早于水位、被超窗丢弃 | 被拒事件 ID |
| `START_DROPPED_INVALID` | 开始候选 schema 校验失败（未知版本/缺字段/非法值） | 被拒事件 ID |
| `START_DISPLACED` | 开始候选因 CONFLICT 被顶掉（同阶段已被占用 / run 归属不同 job） | 被拒事件 ID |

多份候选时按 `DISPLACED > DROPPED_LATE > DROPPED_INVALID` 取最强证据，再按
`(OccurredAt, EventID)` 决胜；所以乱序、重复、并发投递下，缺口原因稳定一致。
（注意：损坏的**结束**事件不会污染开始缺口的证据。）

## 3b. 待补全视图（pending）

`Engine.PendingRuns()` / `ingest.Store.PendingRuns()` 列出所有“已结束但基准不全”的运行，
每行含 `RunID / JobID / FinishedAt / AppliedSeq` 和上述 `Gap`（含原因与证据事件 ID），
结果按 RunID 排序。运行攒得多时用游标分页：

```go
cursor := ""
for {
    page := eng.PendingRunsPage(cursor, 100) // RunID > cursor，每页至多 100
    if len(page) == 0 { break }
    for _, p := range page { /* 按 p.Gap.Code 排查：等到达 / 捞归档 / 查投递冲突 */ }
    cursor = page[len(page)-1].RunID
}
```

该视图由规范化聚合 + 被拒候选索引推导，集合与原因在乱序、重复、并发下保持稳定
（见 `TestPendingViewStableUnderChaos`）。

## 3c. 显式补数（backfill）

从归档或人工补数把丢失的开始事件找回来后，用 `Engine.BackfillEvent(ctx, env)`
（或 `Store.Backfill`）显式补入，随后再调一次 `Evaluate()`：

- **唯一豁免是迟到窗口**：补数绕过水位，因此一条此前被判 `LATE` 的开始事件也能补回；
- 其余规则**照常生效**：schema 仍要校验；相同 `EventID` 已成功应用则为 `DUPLICATE` 幂等空操作；
  阶段已被占用则仍 `CONFLICT`、绝不覆盖；
- 补入后聚合按 `(OccurredAt, EventID)` **规范化重建**，因此补全后的运行与
  “该事件从未丢失、按正常顺序到达”**完全等价**——结论、连续计数、FIRE/RECOVERED 序号一致；
- 为做到这一点，补全后引擎会把**该作业全部可判定运行**按事件时间重折一遍告警生命周期，
  并用 `(事件时间, RunID, Kind)` 规范化重排、重排全局告警序号；未知运行始终不参与；
- **重复补数 / 并发补数**：同一 `EventID` 恰好一次被接受，其余全部 `DUPLICATE`，
  不会产生重复状态或重复告警（`TestConcurrentBackfillNoDuplicates`）；
- 补数不改变规则版本绑定：未知期间已钉住的旧版本，补全后仍按旧版本判定，
  历史结论可原样复现（`TestBackfillUnderPinnedOldRuleVersion`）。

### 版本化与可复现

- 规则注册表中同一 `(RuleID, Version)` **不可变**：用不同定义重复注册返回
  `ErrVersionExists`；完全相同的重复注册是幂等的（便于重放）。
- 判定总是绑定**显式版本号**；未知版本返回 `ErrRuleNotFound`，绝不隐式回退到最新版。
- 作业可由 `Engine.BindRule` 预先钉住版本；否则在首次评估时自动绑定到当时的最新版，
  且此后不再被静默更改。
- 因此规则演进（例如把 SLA 从 10m 收紧到 3m）后，同一批历史事件在 **v1 下的结论可原样
  复现**，v2 只影响按 v2 解释的运行。见 `engine` 包 `TestVersionEvolutionDoesNotRewriteHistory`。

## 4. 告警生命周期

每个 `(JobID, Kind)` 独立维护一个防抖状态机，`Kind` 为 `FRESHNESS` 或 `SLA`：

```
        连续 breach >= FireAfterN
  OK ───────────────────────────────▶ FIRING（恰好一条触发告警）
   ▲                                     │
   │ 连续 pass >= RecoverAfterN          │ 重复 breach：无动作（不重复告警）
   └────────────────────────────── RECOVERED（恰好一条恢复告警，随后回到 OK）
```

- **抖动不重复告警**：FIRING 期间重复 breach 不产生新告警；未达到恢复阈值的偶发 pass
  会被吸收（连续计数被后续 breach 清零）。
- **恢复与再次触发明确区分**：恢复产生独立的 `RECOVERED` 告警并回到 OK；之后再次 breach
  会产生一条**新的、独立序号**的 FIRING 告警（Seq 不同，恢复告警中还记录被恢复告警的 Seq）。
- **观测幂等**：`Observe(obsID, ...)` 以 runID+kind 作为观测 ID；同一条观测被重复投递
  （重试风暴）时返回首次结果，状态机不会被二次推进。
- **未知运行零影响**：时间基准不全（`UNKNOWN`）的运行不产生任何观测，因此既不触发告警，
  也不会计入连续 breach / pass 计数；补全后通过 `Rebuild` 把该作业所有**可判定**运行按
  事件时间重折，得到与正常路径一致的生命周期，且不会重复告警。
- 引擎在 `Evaluate` 中按 **FinishedAt 事件时间顺序**喂观测，因此即使原始事件乱序、并发
  到达，告警看到的观测序列也是确定的。

## 5. 本地验证方法

需要 Go 1.25+（仅标准库）。

```bash
# 构建
go build ./...

# 静态检查
go vet ./...

# 全部单测（含竞态检测）
go test ./... -race

# 查看输入事件与判定依据（测试日志会打印 INPUT 事件、DECISION、decision basis）
go test ./... -race -v

# 只跑某一类场景
go test ./internal/ingest/ -race -v   # 并发摄入 / 乱序 / 迟到 / 重复 / 冲突 / 缺口证据 / 补数 / 待补全
go test ./internal/alert/  -race -v   # 抖动防抖 / 恢复与再次触发 / 幂等 / Rebuild 与正常路径等价
go test ./internal/engine/ -race -v   # 端到端乱序+重复+并发、未知不告警、补数等价、版本复现

# 未知结论 / 补数专项
go test ./internal/engine/ -race -v -run 'Unknown|Backfill|Pending'
go test ./internal/ingest/ -race -v -run 'Pending|Backfill'
go test ./internal/alert/  -race -v -run 'Rebuild'

# 可运行的演示（乱序 + 重复 + 并发投递 -> 结论 -> 告警生命周期；及丢开始->未知->补数）
go run ./cmd/demo
```

### 覆盖的关键场景

- **并发摄入一致性**：同一事件集合被打乱、重复后由多个 goroutine 并发投递，
  连续多轮产生字节一致的聚合与结论（`TestIngestConcurrentDeterministic`、
  `TestDeterminismAcrossChaosRuns`）。
- **乱序 / 迟到**：finished 先于 started 到达时缓冲并按事件时间重建；
  超出水位窗口的事件判 `LATE` 丢弃（`TestIngestOutOfOrderRebuild` / `TestIngestLateRejected`）。
- **重复投递**：相同 EventID 判 `DUPLICATE`，聚合只含一次；告警观测重放幂等。
- **阈值抖动**：连续计数防抖，边界反复不产生重复告警；恢复与再次触发各自一条、序号不同。
- **版本化复现**：规则 v1→v2 收紧后，v1 历史结论不变，v2 产生新结论；
  未知版本 / 缺失字段一律显式报错。
- **缺基准不误判、不告警**：只有结束事件的运行新鲜度/SLA 均为 `UNKNOWN`，不产生 breach、
  不进告警计数；待补全视图列出运行与缺口原因（未到达 / 超窗丢弃 / 非法丢弃 / 被顶掉）。
  （`TestUnknownRunNotJudgedAndNeverAlerts`、`TestPendingGap*`、`TestPendingViewStableUnderChaos`）
- **补数与正常路径等价**：补回丢失的开始事件后，结论与完整告警历史（含序号、原因）与
  “事件从未丢失”逐字节一致；重复 / 并发补数恰好一次生效、无重复告警。
  （`TestBackfillParityWithNormalPath`、`TestConcurrentBackfillNoDuplicates`、
  `TestOutOfOrderBackfillSameAsInOrder`、`TestRebuildEquivalentToIncremental`）
- **补数下的版本复现**：未知期间注册了更新的规则版本，补全后仍按已钉住的旧版本判定。
  （`TestBackfillUnderPinnedOldRuleVersion`）

所有单测均使用 `t.Logf` 打印**输入事件**（ID、run、类型、事件时间）与**判定依据**
（Disposition、阈值/实测值、状态迁移与序号），`-v` 运行即可审阅。
