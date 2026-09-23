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
| `internal/ingest` | 并发摄入：去重、乱序缓冲、迟到判定、规范化重建 |
| `internal/eval` | 新鲜度与 SLA 判定（含阈值、实测值、文字依据） |
| `internal/alert` | 防抖告警生命周期状态机 OK→FIRING→RECOVERED |
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
go test ./internal/ingest/ -race -v   # 并发摄入 / 乱序 / 迟到 / 重复 / 冲突
go test ./internal/alert/  -race -v   # 抖动防抖 / 恢复与再次触发 / 幂等
go test ./internal/engine/ -race -v   # 端到端乱序+重复+并发，及规则版本复现

# 可运行的演示（乱序 + 重复 + 并发投递 -> 结论 -> 告警生命周期）
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

所有单测均使用 `t.Logf` 打印**输入事件**（ID、run、类型、事件时间）与**判定依据**
（Disposition、阈值/实测值、状态迁移与序号），`-v` 运行即可审阅。
