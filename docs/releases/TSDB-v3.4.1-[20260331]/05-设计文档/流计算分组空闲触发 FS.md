# 流计算分组空闲触发 FS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-03-14 | 2026-03-14 | 1.0 | 邝金清 | 初稿 |

## 2. 背景

TDengine 流计算支持在建流时配置 `WINDOW_OPEN` / `WINDOW_CLOSE` 等事件触发计算或通知，用于在窗口生命周期关键时刻输出结果或发送告警。但在实际监控场景中，用户往往更关心“某个分组长时间没有数据”这一类异常：例如某台设备停止上报、某个业务分区数据链路断流等。
现有窗口事件触发依赖“新数据到达”或“窗口自然闭合”。当某个分组长时间不再收到数据时，窗口可能一直处于打开状态，用户既无法触发一次计算来落库“离线快照”，也无法触发通知来告警。并且，当该分组重新开始上报时，用户也希望系统能立即触发“恢复”事件，形成闭环。
本特性新增“分组空闲/恢复触发”能力：在创建流时配置空闲超时阈值，系统运行时按分组跟踪最后一次接收数据的时间，在进入空闲与恢复时触发计算和/或通知，并提供基础日志与指标，满足监控告警与运维可观测性需求。

## 3. 定义

- **流（Stream）**：通过 `CREATE STREAM` 定义的持续计算任务。
- **分组（Group/Partition）**：流任务按 `PARTITION BY` 指定的键（如 `tbname`、tag、列）划分的独立处理单元；若未指定 `PARTITION BY`，则视为单一分组。
- **空闲超时（idle_timeout）**：在连续 `idle_timeout` 时间内未收到该分组的新数据，则该分组进入空闲状态。
- **空闲事件（IDLE）**：分组从“活跃”转为“空闲”的状态迁移事件，仅在迁移发生时触发一次。
- **恢复事件（RESUME）**：分组从“空闲”转为“活跃”的状态迁移事件，仅在迁移发生时触发一次。
- **最后接收时间（last_recv_time）**：分组“最近一次收到并被流任务处理到的数据”的到达时间（processing time）；不以数据行内的 `ts`（event time）为准。
- **单调时钟（monotonic clock）**：用于计算“空闲间隔”的时间来源，避免 NTP 校时等导致的系统时钟跳变影响空闲判定。
- **触发行为**：
  - **触发计算**：执行流的计算逻辑并写入 `INTO` 输出表（受 `stream_options(event_type(...))` 控制）。
  - **触发通知**：向 `notify(...)` 配置的 URL 发送通知（受 `notify(...) on(...)` 控制）。

## 4. 行为说明

### 4.1 创建流语法扩展

本特性在 `STREAM_OPTIONS(...)` 中新增一个可选配置项 `IDLE_TIMEOUT(duration)`，用于开启分组空闲检测。

#### 4.1.1 `IDLE_TIMEOUT(duration)`

- **作用**：为该流任务开启分组空闲检测，按分组维护 `last_recv_time`，并在空闲/恢复时触发事件。
- **参数**：`duration` 为时间长度表达式。
- **有效范围**：`1s` 到 `10d`（包含边界）。
- **支持单位**：`a`（毫秒）、`s`（秒）、`m`（分钟）、`h`（小时）、`d`（天）。
  - 若用户使用 `a`（毫秒），系统仍需保证最终换算后的时长满足 `>= 1s`。
- **默认值**：未配置则不启用空闲检测（行为与现有版本一致）。
示例（仅展示语法形态，完整 SQL 见后续用例）：
```sql
CREATE STREAM s_idle
  STATE_WINDOW(status)
  FROM meters
  PARTITION BY tbname
  STREAM_OPTIONS(IDLE_TIMEOUT(5m) | EVENT_TYPE(WINDOW_CLOSE | IDLE | RESUME))
  INTO meters_agg
AS
  SELECT _tlocaltime AS event_ts, tbname, COUNT(*) AS cnt
  FROM %%trows;
```

### 4.1 新增事件类型：`IDLE` / `RESUME`

本特性新增两个事件类型，可用于：
- `STREAM_OPTIONS(EVENT_TYPE(...))`：控制“触发计算”的事件类型集合。
- `NOTIFY(url ...) ON (...)`：控制“触发通知”的事件类型集合。

#### 4.1.1 事件类型列表

在现有 `WINDOW_OPEN` / `WINDOW_CLOSE` 的基础上新增：

| 事件类型 | 含义 | 触发时机 |
| --- | --- | --- |
| `IDLE` | 分组进入空闲 | 分组连续 `idle_timeout` 未收到新数据时触发一次 |
| `RESUME` | 分组从空闲恢复 | 已空闲分组收到新数据时触发一次 |

#### 4.1.2 与现有事件共存

`IDLE` / `RESUME` 必须与现有 `WINDOW_OPEN` / `WINDOW_CLOSE` 语义独立，且允许在同一条流上同时启用多个事件类型，例如：
```sql
STREAM_OPTIONS(IDLE_TIMEOUT(5m) | EVENT_TYPE(WINDOW_OPEN | WINDOW_CLOSE | IDLE | RESUME))
```

通知端同理：
```sql
NOTIFY('ws://localhost:12345/stream') ON (WINDOW_OPEN | WINDOW_CLOSE | IDLE | RESUME)
```

说明：
- 事件类型可以共存，但占位符语义不能混用。若某条流的计算会被 `IDLE` 或 `RESUME` 触发，则其计算 SQL 不得引用 `_twstart/_twend`。
- 若业务既需要窗口边界（`_twstart/_twend`）又需要空闲区间边界，应拆分为独立的窗口事件流与空闲事件流，避免同一条计算 SQL 同时承载两套时间语义。

### 4.2 分组空闲检测与状态机

#### 4.2.1 分组状态

系统为每个分组维护以下运行时状态：
- `ACTIVE`：分组当前处于活跃状态（近期收到过数据）。
- `IDLE`：分组当前处于空闲状态（已超过 `idle_timeout` 未收到数据）。
初始状态规则：
- 流任务启动时不预置任何分组状态。
- 当某个分组第一次收到数据并被处理时，系统为其创建状态记录并置为 `ACTIVE`，同时记录 `last_recv_time`。

#### 4.2.2 空闲判定

系统使用单调时钟计算空闲间隔，满足以下条件时触发空闲迁移：
1. 分组当前状态为 `ACTIVE`
2. `now_mono - last_recv_mono >= idle_timeout`
迁移行为：
- 将分组状态置为 `IDLE`
- 触发一次 `IDLE` 事件（用于计算和/或通知，取决于用户配置）
- 记录关键日志与指标（见 4.6 与第 12 节）
注意：
- 对处于 `IDLE` 状态的分组，不重复触发 `IDLE` 事件；只有在后续发生 `RESUME` 后，才可能再次触发下一次 `IDLE`。
- 分组空闲检测彼此独立；某分组进入空闲不影响其它分组的数据处理与事件触发。

#### 4.2.3 恢复判定

当处于 `IDLE` 状态的分组收到新数据时：
- 立即将分组状态置回 `ACTIVE`
- 立即触发一次 `RESUME` 事件
- 更新该分组的 `last_recv_time`（重置空闲计时器）
“立即”的目标约束为：
- `RESUME` 事件触发应在该分组第一条恢复数据被处理后的 100ms 内完成（以系统可观测到的事件时间为准）。

#### 4.2.4 边界行为

- **超时边界附近的数据到达**：若分组在 `idle_timeout` 边界之前收到新数据（例如 `5m` 配置，在 `4m59s` 收到数据），系统必须更新 `last_recv_time` 并继续保持 `ACTIVE`，不得触发 `IDLE`。
- **分组从未出现**：对从未收到过数据的分组，不产生 `IDLE` / `RESUME` 事件。
- **任务重启**：任务重启后清空全部分组的空闲状态（不持久化）。重启后只有当分组再次收到数据时才重新建立状态并开始计时；因此可能延迟（或在极端情况下无法）发现重启前已空闲但重启后一直不再上报的分组。

### 4.3 触发计算行为

#### 4.3.1 如何启用

用户在 `STREAM_OPTIONS(EVENT_TYPE(...))` 中包含 `IDLE` 和/或 `RESUME`，即可在对应事件发生时触发一次计算并写入 `INTO` 输出表。
示例：
```sql
CREATE STREAM dev_idle_calc
  STATE_WINDOW(status)
  FROM meters
  PARTITION BY tbname
  STREAM_OPTIONS(IDLE_TIMEOUT(5m) | EVENT_TYPE(IDLE | RESUME))
  INTO dev_event_log
AS
  SELECT
    _tlocaltime    AS event_ts,
    _tidlestart    AS idle_from,
    _tidleend      AS idle_to,
    tbname         AS device,
    _tgrpid        AS gid,
    COUNT(cint)    AS rows_in_window
  FROM %%trows;
```

说明：
- `IDLE` 事件的计算通常发生在“无新数据到达”的场景，用户应避免依赖“恢复数据行”才能成立的计算逻辑；建议以聚合或常量列构造“事件记录型输出”。
- 若同时启用 `WINDOW_CLOSE` 与 `IDLE`，两类事件触发互不替代：`IDLE` 事件不会隐式关闭窗口，`WINDOW_CLOSE` 仍按原有窗口规则触发。用户需自行决定是否在业务上需要同时启用两者。
- `IDLE` / `RESUME` 的计算如需引用空闲区间，必须使用 `_tidlestart/_tidleend`；`_twstart/_twend` 仅保留给窗口事件触发的计算。

#### 4.3.2 `IDLE` / `RESUME` 的 `_tidlestart` / `_tidleend` 语义（计算上下文）

对于由 `IDLE` / `RESUME` 事件触发的计算，系统必须为本次计算上下文提供 `_tidlestart` 与 `_tidleend` 两个专用时间占位符（供用户在 `SELECT` 中引用）。这两个值表示分组的“空闲区间”（processing time），且不得复用 `_twstart/_twend`，以避免与窗口触发语义混淆。
该语义与分组当前是否存在“开启的窗口”无关，且不复用任何窗口的起止边界：

| 事件 | `_tidlestart` | `_tidleend` |
| --- | --- | --- |
| `IDLE` | 分组进入空闲前最后一次收到并处理到数据的时间（processing time） | `IDLE` 事件触发时间（processing time） |
| `RESUME` | 分组进入空闲前最后一次收到并处理到数据的时间（processing time） | `RESUME` 事件触发时间（processing time，收到恢复数据并处理到后触发） |

说明：
- `_tidlestart/_tidleend` 为 timestamp 类型，固定为 ns 精度。
- `_tidleend - _tidlestart` 表示“距离最后一次数据到达已经过去多久”（wall clock 差值可能受时钟跳变影响）；如同时启用了通知，则事件 payload 会提供 `idleDurationMs`（单调时钟计算），接收端如需严格的时间间隔语义应以 `idleDurationMs` 为准。
- 若 `STREAM_OPTIONS(EVENT_TYPE(...))` 中包含 `IDLE` 或 `RESUME`，则该流的计算 SQL 在建流时必须禁止引用 `_twstart` 或 `_twend`；命中时应返回明确的语义错误。
- `_tidlestart/_tidleend` 仅在 `IDLE` / `RESUME` 计算上下文中有效；仅由窗口事件触发的计算不应依赖这两个占位符。

#### 4.3.3 失败与重试

当 `IDLE` / `RESUME` 触发计算失败时：
- 复用现有流计算的失败处理与重试机制（与 `WINDOW_OPEN` / `WINDOW_CLOSE` 计算触发一致）。
- 失败与重试次数、退避策略、任务暂停/继续等行为保持与现有流计算一致，本特性不引入新的失败策略。

### 4.4 触发通知行为

#### 4.4.1 如何启用

用户在 `NOTIFY(url ...) ON (...)` 中包含 `IDLE` 和/或 `RESUME`，即可在对应事件发生时发送通知。
示例：
```sql
CREATE STREAM dev_idle_notify
  STATE_WINDOW(status)
  FROM meters
  PARTITION BY tbname
  STREAM_OPTIONS(IDLE_TIMEOUT(5m) | EVENT_TYPE(WINDOW_CLOSE))
  NOTIFY('ws://localhost:12345/dev_idle') ON (IDLE | RESUME)
  NOTIFY_OPTIONS(NOTIFY_HISTORY | ON_FAILURE_PAUSE)
  INTO dev_status_agg
AS
  SELECT _twstart, _twend, tbname, AVG(voltage) AS vavg
  FROM %%trows;
```

说明：
- 通知与计算可以独立开启：可以只通知不计算、只计算不通知、或同时开启。
- `IDLE` / `RESUME` 通知机制复用现有通知框架，包括历史通知（`NOTIFY_HISTORY`）与失败暂停（`ON_FAILURE_PAUSE`）等选项。

#### 4.4.2 通知内容与字段

通知 payload 的消息级外层结构沿用现有流通知协议，本特性不展开赘述。本文仅明确 `streams[].events[]` 中 event 对象的字段约定，以及 `IDLE` / `RESUME` 事件新增字段。

##### 4.4.2.1 事件结构（Event，通用字段）

`streams[].events[]` 的每个 event 必须包含以下通用字段：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `eventType` | string | 事件类型：`WINDOW_OPEN` / `WINDOW_CLOSE` / `ON_TIME` / `IDLE` / `RESUME` |
| `eventTime` | number | 事件生成时间，毫秒级 Unix epoch |
| `triggerId` | string | 事件 ID，用于标识一次事件实例并进行事件关联；同一分组的一次空闲周期内，`IDLE` 与对应的 `RESUME` 事件必须具有相同的 `triggerId` |
| `triggerType` | string | 流触发类型：`Period` / `Interval` / `Sliding` / `Session` / `Count` / `State` / `Event` |
| `tableName` | string | 输出表名；分组流为输出子表名；无 `PARTITION BY` 时为输出表名 |
| `groupId` | string | 分组 ID（十进制字符串），与输出子表 tag `_tgrpid` 对应 |

说明：
- `IDLE` / `RESUME` 事件不包含 `windowStart/windowEnd`（避免将空闲/恢复事件误解为窗口事件）。
- `IDLE` / `RESUME` 事件包含 `idleStart/idleEnd`（语义解释参考上一节），注意它们使用系统时间（wall clock），在时钟跳变时可能不连续；接收端如需”间隔”语义，应以 `idleDurationMs` 为准。
- 若本次事件携带计算结果，则包含 `result` 对象（典型结构包含 `result.data`、`result.curSize`、`result.curOffset`、`result.finish`）。

##### 4.4.2.2 `IDLE` / `RESUME` 事件的附加字段

为保证接收端在不依赖外部状态的情况下可判定阈值与持续时间，`IDLE` / `RESUME` event 必须额外携带以下字段（作为 event 对象的同级字段出现）：
公共字段：
- `idleStart`: 数字，该分组进入空闲前最后一次“收到并处理到数据”的处理时间（processing time，wall clock），Unix epoch，固定为 ns 精度。
- `idleEnd`: 数字，触发当前事件的系统时间（processing time，wall clock），Unix epoch，固定为 ns 精度。
- `idleDurationMs`: 数字：
  - 对于 `IDLE` 事件来说，代表从最后一次“收到数据”到触发 `IDLE` 的间隔（毫秒）。
  - 对于 `RESUME` 事件来说，代表从之前最后一次数据到达到本次 `RESUME` 的间隔（毫秒）。
  - 该值的计算必须使用单调时钟，允许因检测周期存在少量额外延迟；接收端如需严格的时间间隔语义应以 `idleDurationMs` 为准，而非 `idleEnd - idleStart`（后者使用系统时间，在时钟跳变时可能不连续）。

##### 4.4.2.3 示例（IDLE / RESUME）

`IDLE` 示例：
```json
{
  "messageId": "8a7b0c1e-6a6d-4e02-9f41-0f1a5d2d9f2b",
  "timestamp": 1760000300123,
  "streams": [
    {
      "streamName": "sdb1.dev_offline_alarm",
      "events": [
        {
          "eventType": "IDLE",
          "eventTime": 1760000300123,
          "triggerId": "16234318070948638251",
          "triggerType": "Interval",
          "tableName": "dev_minute_stat_dev001",
          "groupId": "4321",
          "idleStart": 1760000000122001,
          "idleEnd": 1760000300123007,
          "idleDurationMs": 300001
        }
      ]
    }
  ]
}
```

`RESUME` 示例：
```json
{
  "messageId": "f1a2b3c4-1111-2222-3333-abcdefabcdef",
  "timestamp": 1760000600456,
  "streams": [
    {
      "streamName": "sdb1.dev_offline_alarm",
      "events": [
        {
          "eventType": "RESUME",
          "eventTime": 1760000600456,
          "triggerId": "16234318070948638251",
          "triggerType": "Interval",
          "tableName": "dev_minute_stat_dev001",
          "groupId": "4321",
          "idleStart": 1760000000122001,
          "idleEnd": 1760000600456079,
          "idleDurationMs": 600334
        }
      ]
    }
  ]
}
```

#### 4.4.3 失败处理

当 `IDLE` / `RESUME` 通知发送失败时
- 复用现有通知失败处理机制。
- 若配置了 `ON_FAILURE_PAUSE`（或等价配置项 `NOTIFY_ON_FAILURE_PAUSE`），则触发对应的暂停/重试策略。

## 5. 性能

本特性在运行时为每个已出现的分组维护少量状态（最后接收时间、当前空闲状态等），并以固定频率对分组进行空闲检查。性能影响主要来自：
- **内存开销**：与“已出现分组数量”近似线性相关。在 100000 分组规模下，状态存储应处于可控范围。
- **CPU 开销**：空闲检查通常为 O(N) 扫描（N 为分组数）。通过合理的检查周期与轻量的数据结构，实现目标为对正常流计算吞吐影响小于 5%。
当分组数量达到数万级并且 `idle_timeout` 设置较小（例如秒级）时，检查频率与扫描成本会显著上升；此时建议用户合理设置 `idle_timeout`，并结合指标观察 CPU 影响。

## 6. 安全

本特性不引入新的网络接口与鉴权模型，触发的计算与通知权限控制沿用现有流计算权限体系：
- 建流权限、读源表权限、写目标表权限、以及通知相关权限（若存在）保持不变。
- 通知 URL 的安全策略、网络访问控制等沿用现有机制。
本特性新增的日志与指标不应泄露敏感数据内容；日志中建议仅记录必要的流名、分组标识与状态信息。

## 7. 兼容性

- **向后兼容**：未配置 `IDLE_TIMEOUT(...)` 的现有流任务行为不变，不引入空闲/恢复事件。
- **SQL 兼容**：新增关键字/事件类型 `IDLE`、`RESUME` 以及选项 `IDLE_TIMEOUT(...)`。在不支持本特性的旧版本上，包含上述语法的 `CREATE STREAM` 会报语法或参数错误。
- **与现有事件共存**：允许与 `WINDOW_OPEN` / `WINDOW_CLOSE` 同时启用；互不替代，用户需自行评估是否产生额外输出或通知。

## 8. 运维

运维与部署层面主要关注以下点：
- **配置启用**：仅当建流时配置 `STREAM_OPTIONS(IDLE_TIMEOUT(...))` 才启用；建议在生产环境逐步灰度开启并观察指标变化。
- **重启影响**：流任务重启会清空分组空闲状态；重启后将重新学习分组并重新计时，可能导致空闲/恢复事件在重启窗口内缺失或延迟。对强依赖告警的用户，应在运维流程中将该特性纳入重启评估。
- **容量规划**：分组数越多，状态越大；建议结合业务分组基数与 `IDLE_TIMEOUT` 设置进行容量评估。
- **告警降噪**：若 `IDLE_TIMEOUT` 设置过短或数据到达抖动明显，可能产生频繁的 `IDLE/RESUME` 抖动；建议结合业务特征设置合理阈值，并在通知接收端做去抖/聚合。

## 9. 使用场景

### 9.1 场景 1：设备离线告警（通知）

按设备（`tbname`）分组，当某设备 5 分钟无数据时触发 `IDLE` 告警；恢复上报时触发 `RESUME` 通知。
```sql
CREATE STREAM dev_offline_alarm
  INTERVAL(1m) SLIDING(1m)
  FROM meters
  PARTITION BY tbname
  STREAM_OPTIONS(IDLE_TIMEOUT(5m) | EVENT_TYPE(WINDOW_CLOSE))
  NOTIFY('ws://localhost:12345/alarm') ON (IDLE | RESUME)
  NOTIFY_OPTIONS(NOTIFY_HISTORY | ON_FAILURE_PAUSE)
  INTO dev_minute_stat
AS
  SELECT _twstart, tbname, COUNT(*) AS cnt, AVG(current) AS iavg
  FROM %%trows;
```

说明：该流按分钟输出统计结果（`WINDOW_CLOSE`），同时在分组空闲/恢复时发出通知，便于监控系统告警与恢复闭环。

### 9.2 场景 2：空闲事件落库（计算）

用户希望将每次 `IDLE` / `RESUME` 事件落到一张事件表中，便于后续审计与统计。
```sql
CREATE STREAM dev_idle_event_to_table
  STATE_WINDOW(status)
  FROM meters
  PARTITION BY tbname
  STREAM_OPTIONS(IDLE_TIMEOUT(5m) | EVENT_TYPE(IDLE | RESUME))
  INTO dev_idle_events
AS
  SELECT
    _tlocaltime AS event_ts,
    _tidlestart AS idle_from,
    _tidleend AS idle_to,
    tbname AS device,
    _tgrpid AS gid,
    COUNT(cint) AS window_rows
  FROM %%trows;
```

说明：推荐输出表包含 `event_ts`、空闲区间边界、设备标识、分组标识等字段。若用户需要区分 `IDLE` 与 `RESUME`，可结合通知事件类型或在输出表设计中额外添加区分字段（例如分别建两条流，或在接收端写入事件类型）。

## 10. 约束和限制

**约束**：
- `IDLE_TIMEOUT(duration)` 仅对“已出现过数据”的分组生效；从未出现的分组不产生事件。
- `duration` 必须在 `1s` 到 `10d` 范围内；超出范围或格式非法的 `CREATE STREAM` 必须报错。
- 空闲判定基于 processing time（数据到达/被处理时间），不是数据行内的 `ts`；因此乱序/补写历史数据会触发恢复事件（只要它被处理到）。
- 单调时钟用于间隔计算；即使系统时间跳变，也不应影响空闲判定的正确性。
- 若某条流的计算会被 `IDLE` 或 `RESUME` 触发，则该流的计算 SQL 不得引用 `_twstart/_twend`；需要表达空闲区间时必须改用 `_tidlestart/_tidleend`。
**限制**：
- 任务重启会清空分组空闲状态且不持久化；重启前已活跃但重启后长期不再上报的分组，可能无法再触发 `IDLE` 事件（因为该分组不会再次被“观察到”）。
- 在分组基数极大且持续增长的场景（例如分组键空间无限），分组状态会持续积累并增加内存使用；本特性不额外引入分组状态淘汰策略。
- `IDLE` 事件触发的检测延迟受检查周期与调度影响，目标为 <= 1s，但在系统负载较高时可能出现更大抖动。

## 11. 常见错误和排查

| 问题现象 | 可能原因 | 排查建议 |
| --- | --- | --- |
| 建流时报错，提示 `IDLE_TIMEOUT` 非法 | `duration` 格式错误或超出范围 | 检查是否在 `1s` 到 `10d`，单位是否为 `a/s/m/h/d` |
| 建流时报错，提示 `IDLE/RESUME` 计算不能使用 `_twstart/_twend` | `EVENT_TYPE(...)` 包含 `IDLE` 或 `RESUME`，但计算 SQL 仍引用了窗口占位符 | 改用 `_tidlestart/_tidleend`；若还需要窗口边界，拆成独立的 `WINDOW_*` 计算流 |
| 建流时报错，提示 `_tidlestart/_tidleend` 仅支持 `IDLE/RESUME` | 计算 SQL 引用了空闲区间占位符，但该流的计算不会被 `IDLE` / `RESUME` 触发 | 使用 `SHOW CREATE STREAM` 确认 `EVENT_TYPE(...)`；若仅保留窗口事件，则改用 `_twstart/_twend`；若需要空闲区间，则补充 `IDLE/RESUME` 并评估是否拆流 |
| 配置了 `ON(IDLE|RESUME) `但始终不通知 | 未配置 `IDLE_TIMEOUT(...)` 或分组从未出现 | 确认 `STREAM_OPTIONS(IDLE_TIMEOUT(...))` 已配置；确认该分组确实写入过数据 |
| 频繁出现 `IDLE/RESUME` 抖动 | `IDLE_TIMEOUT` 过短或数据本身有抖动 | 调大 `IDLE_TIMEOUT`（如从 5s 调到 1m/5m）；在接收端做去抖 |
| 重启后没有再触发 `IDLE` | 重启清空状态，且分组未再收到数据 | 这是预期限制；如需重启后仍能告警，需要在外部监控系统补充“最后上报时间”逻辑 |
| 系统负载高时空闲触发延迟变大 | 检查周期/调度受影响 | 观察 CPU/IO 指标与流任务日志，必要时降低分组数或调大 `IDLE_TIMEOUT` |

## 12. 可观测性

无。

## 13. 安装和卸载

本特性不引入新的独立组件与安装依赖。

## 14. 文档

本特性需要同步更新官网文档：
- **SQL 参考手册（中文）**：`community/docs/zh/14-reference/03-taos-sql/41-stream.md`
  - 补充 `IDLE_TIMEOUT(...)`
  - 补充 `IDLE` / `RESUME` 事件类型
  - 补充 `_tidlestart/_tidleend` 的定义
  - 明确 `IDLE/RESUME` 计算禁止使用 `_twstart/_twend`
  - 补充通知 payload 新字段与空闲/恢复示例
- **SQL 参考手册（英文）**：`community/docs/en/14-reference/03-taos-sql/41-stream.md`
  - 与中文手册保持同等内容更新
- **高级用法文档（中文）**：`community/docs/zh/06-advanced/03-stream.md`
  - 增加设备离线/恢复场景示例
  - 增加窗口占位符与空闲区间占位符的区别说明
- **高级用法文档（英文）**：`community/docs/en/06-advanced/03-stream.md`
  - 与中文高级文档保持同等内容更新
- **关键字手册（中文/英文）**：`community/docs/zh/14-reference/03-taos-sql/92-keywords.md`、`community/docs/en/14-reference/03-taos-sql/92-keywords.md`
  - 若 `IDLE`、`RESUME` 作为新增关键字对外暴露，需要同步登记版本范围
- **错误码手册（中文/英文）**：`community/docs/zh/14-reference/09-error-code.md`、`community/docs/en/14-reference/09-error-code.md`
  - 若为占位符误用分配独立用户可见错误码或稳定错误文案，需要同步登记

## 15. 参考文档

无。

## 16. 附录

### 16.1 分组状态机（文字版）

```plaintext
         (timeout)
ACTIVE  ----------->  IDLE
  ^                   |
  | (new data)        |
  +-------------------+
        RESUME
```

### 16.2 推荐的告警去抖策略（接收端）

为减少抖动带来的告警噪声，建议通知接收端对同一分组的 `IDLE/RESUME` 做简单去抖：
- 在 `IDLE` 告警发出后，若在很短时间窗口（例如 10s）内收到 `RESUME`，可将其降级为“抖动提示”而非告警恢复。
- 对连续 `RESUME`（不应发生）或异常频率的切换，可记录为链路不稳定指标，辅助排障。
