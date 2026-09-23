# 本地流水线作业可观测性与告警组件

消费本地流水线作业运行事件，聚合运行状态，输出**新鲜度（freshness）**、
**SLA** 判定，并驱动告警在「正常 → 触发 → 恢复」之间完整流转。设计目标：

- 乱序、重复、并发摄入下结论**确定、一致、可解释**；
- 判定规则与事件结构**版本化**，历史结论可在旧版本下**精确复现**；
- 任何被丢弃或延迟应用的事件都有**可区分的处理原因**，绝不静默。

## 1. 包结构

| 包 | 职责 |
|---|---|
| `event` | 版本化事件模型与严格解码（无默认值兜底） |
| `rule` | 版本化、不可变的新鲜度/SLA 规则与注册中心 |
| `ingest` | 并发安全聚合：去重、乱序、迟到、终态关闭 |
| `eval` | 纯函数判定：输出新鲜度/SLA 结论与逐条判定依据 |
| `alert` | 告警生命周期状态机与阈值抖动抑制 |
| 根包 `pipelineobs` | 串联以上组件的门面（Facade） |

## 2. 事件模型

所有事件都必须带显式 `schema_version`，解码时**不使用默认值填充缺失字段**，
结构不合法直接拒绝：

- `event/v1`：`schema_version, event_id, run_id, job_id, seq, occurred_at, phase, status`
  - `phase ∈ {STARTED, FINISHED}`；`FINISHED` 必须带 `status ∈ {SUCCESS, FAILED, TIMEOUT}`；
  - v1 没有重试概念，`attempt` 由该 schema 明确定义为 `1`（不是"默认值"）。
- `event/v2`：在 v1 基础上**新增必填** `attempt (>=1)` 与 `host`。
  - 缺字段直接拒绝，不会按 attempt=1 或空 host 静默处理。

关键字段：

- `event_id`：单次物理投递的唯一标识，是**投递去重键**；
  同一 `event_id` 内容必须一致，复用为不同内容会被拒绝（`REJECTED`）。
- `seq`：同一次运行内单调递增的序号，用于乱序判定（**first-write-wins**）。
- `occurred_at`：事件发生的事件时间；聚合维护运行级 event-time watermark。

### 摄入处理原因（`ingest.Reason`）

每次 `Ingest` 都返回一个 `Decision{Applied, Reason, Detail}`：

| Reason | 含义 | 是否改变状态 |
|---|---|---|
| `APPLIED` | 作为新的高水位事件应用 | 是 |
| `APPLIED_LATE` | 晚于更高 seq 到达，但补齐了缺失信息（如迟到的 STARTED） | 是 |
| `DUPLICATE` | 同一 `event_id` 同内容重复投递 | 否 |
| `STALE` | 低于高水位且不带来新信息 | 否 |
| `SUPERSEDED` | 同一 `seq` 已被不同内容占用，首写者胜 | 否 |
| `CLOSED` | 运行已被 FINISHED 关闭，更高 seq 事件不能重开 | 否 |
| `TOO_LATE` | 事件时间落后 watermark 超过 `MaxLateness` 视界 | 否 |
| `REJECTED` | 未知版本/缺字段/schema 冲突/EventID 复用 | 否 |

判定优先级是确定性的：`REJECTED > DUPLICATE > SUPERSEDED > CLOSED >
(TOO_LATE | STALE | APPLIED_LATE) > APPLIED`。乱序的更早 `STARTED`
仍可把"真正的开始时间"纠正为最早值，保证任意到达顺序下**规范终态一致**。

## 3. 新鲜度与 SLA 判定规则

判定是 `(RunState, Rules, at)` 的**纯函数**，同一状态 + 同一规则版本 +
同一评估时刻，永远得到相同的 `Verdict` 与相同的人类可读 `BasisItem` 依据。
结论是三态：`OK / BREACH / UNKNOWN`（信息不足时为 UNKNOWN，绝不静默按 OK 处理）。

### `rules/v1`

- 新鲜度：
  - 运行中：`at - StartedAt <= FreshnessLimit`；
  - 已结束：`at - FinishedAt <= FreshnessLimit`（产出陈旧度）；
  - 连 STARTED 都没有：`UNKNOWN`。
- SLA（已结束才有结论，未结束为 `UNKNOWN`）：
  - `status == SUCCESS` 且 `FinishedAt - StartedAt <= MaxDuration` 才 `OK`；
  - `FAILED`、`TIMEOUT`、超时一律 `BREACH`；
  - FINISHED 已到但 STARTED 尚未补齐（迟到缺口）时为 `UNKNOWN`，不拿零值时间乱算。

### `rules/v2`（规则演进示例）

- 新鲜度同 v1。
- SLA：
  - `SUCCESS`：duration `<= MaxDuration`；
  - `TIMEOUT`：duration `<= TimeoutMaxDuration` 时可达标（快速失败被视为可容忍）；
  - `FAILED`：永远 `BREACH`；
  - 新增**重试策略**：`attempt > MaxAttempts` 无论结果如何都 `BREACH`；
  - 因此 v2 规则**要求 event/v2 输入**。把 v2 规则作用在 event/v1 数据上会
    返回显式错误，而不是按 attempt=1 静默比较。

规则文档 `(jobID, version)` 一经注册**不可覆盖**（写一次），保证历史可复现。

## 4. 告警生命周期与抖动抑制

每个作业一个独立状态机，四阶段：

```
NORMAL ──(连续 breach 达 TriggerAfter)──► PENDING ──► FIRING
  ▲                                        │          │
  └────────────(连续 OK 达 RecoverAfter)────┴──────────┘
                          FIRING ──► RESOLVED（同一 IncidentID 关闭）
```

- 阈值附近抖动（BREACH/OK 来回跳）会在达到阈值前清零计数，**不产生告警**；
  这类被抑制的观察以 `Kind=SUPPRESSED` 返回，附带可读原因，便于审计。
- 触发后持续 breach 只返回 SUPPRESSED，**不会重复告警**。
- 恢复需要连续 `RecoverAfter` 次 OK；恢复时用原 `IncidentID` 关闭事件。
- 恢复后再次触发会开启**全新 IncidentID**（`INC-<job>-<n>`），
  因此"恢复"与"再次触发"永远可明确区分。
- `UNKNOWN` 不携带告警信号，不移动任何计数；同一作业事件时间倒流会被显式拒绝。

只有"被真正应用"的事件才会触发重新评估与告警观察，因此重复/过期投递
不可能改变结论或刷出告警。

## 5. 本地验证

要求 Go 1.25+。

```bash
# 编译与静态检查
go build ./...
go vet ./...

# 全部单测（含并发 / 乱序 / 迟到 / 抖动 / 重复投递）
go test ./...

# 竞态检测（并发摄入、并发 run 隔离）
go test -race ./...

# 查看覆盖率
go test -cover ./...

# 打印输入事件与逐条判定依据（测试日志中 INPUT / DECISION / VERDICT / BASIS / ALERT）
go test -v ./ingest/ ./eval/ ./alert/ .
```

也可使用 `make test` / `make race` / `make cover`。

### 测试覆盖场景

- **并发摄入**：20 种随机交叉 + 50 个并发 run；`-race` 下验证规范终态一致、
  每个 seq 恰好一个首写者胜出、重复绝不应用。
- **乱序与迟到**：FINISHED 先于 STARTED（`APPLIED_LATE` 补洞）、低于
  watermark 的冗余事件（`STALE`）、超过 `MaxLateness`（`TOO_LATE`）。
- **阈值抖动**：(2,2) 策略下 BREACH/OK 反复横跳不产生任何告警。
- **重复投递**：同一事件多次投递结果幂等、无重复告警。
- **生命周期**：触发、抑制、恢复、再触发；两次触发 IncidentID 不同。
- **版本化复现**：同一批历史事件在 rules/v1 下复现 BREACH、rules/v2 下复现 OK；
  未知版本、字段缺失、schema 不匹配均显式报错。
