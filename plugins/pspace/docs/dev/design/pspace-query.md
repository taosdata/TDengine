# pSpace 历史查询模式设计（Query + QuerySync）

## 概述

Query 和 QuerySync 两种运行模式底层都基于 pSpace SDK 的 `histRead` 接口查询历史数据，区别在于 Query 是一次性任务，QuerySync 在历史回填后持续同步。

## 运行模式

### Query — 一次性历史数据迁移

- **功能**：指定时间范围，将 pSpace 中满足条件的历史数据查询出来，写入 TSDB 数据库，完成一次性数据迁移后退出。
- **底层 SDK 方法**：`histRead`
- **执行流程**：
  1. 解析 `start_time`（必填）和 `end_time`（默认当前时间）
  2. 获取点位列表（规则见 [pspace-points.md](pspace-points.md)）
  3. 连接 pSpace 和 taosX（Netty TCP）
  4. 发送表定义（Arrow IPC）
  5. 将 `[start_time, end_time)` 按 `time_window` 划分为多个不重叠的子查询窗口
  6. 逐窗口调用 `histRead` 查询 `[windowStart, windowEnd)`，序列化为 Arrow IPC，通过 Netty 发送到 taosX
  7. 发送 EOS（End of Stream），断开连接，退出

### QuerySync — 历史回填 + 持续同步

- **功能**：先完成从 `start_time` 到当前时刻的历史数据迁移（Phase 1），然后不退出，按 `query_interval` 间隔持续轮询 pSpace 同步新数据（Phase 2）。
- **底层 SDK 方法**：`histRead`（Phase 1 和 Phase 2 都使用）
- **执行流程**：
  - **Phase 1（历史回填）**：与 Query 模式相同，`end_time` 固定为 Phase 1 开始时刻的当前时间，窗口不重叠
  - **Phase 2（持续同步）**：
    1. `syncStart` = Phase 1 结束时的 `endMillis`
    2. 循环：等待 `query_interval` 秒 → 取当前时间为 `syncEnd` → 查询 `[syncStart - excursion, syncEnd)` → 发送数据 → `syncStart = syncEnd`
    3. `time_excursion` 仅在此阶段生效，向前回溯以捕获乱序（迟到）数据
    4. 直到 Netty 连接断开时退出

## 配置参数

TOML `[run]` 节的参数定义在 `RunConfig` 类中：

| 参数     | TOML 字段        | 类型       |           Query           |         QuerySync         | 说明                           |
| -------- | ---------------- | ---------- | :-----------------------: | :-----------------------: | ------------------------------ |
| 模式     | `mode`           | String     |      必填 `"Query"`       |    必填 `"QuerySync"`     | 运行模式                       |
| 开始时间 | `start_time`     | String     |         **必填**          |         **必填**          | 数据查询的起始时间戳           |
| 结束时间 | `end_time`       | String     |   可选（默认当前时间）    | 不使用（固定为当前时间）  | 数据查询的截止时间戳           |
| 查询窗口 | `time_window`    | Long（秒） | 可选（默认 86400 = 1 天） | 可选（默认 86400 = 1 天） | 划分子查询的时间窗口大小       |
| 乱序偏移 | `time_excursion` | Long（秒） |          不使用           | 可选（默认 0，仅 Phase 2）| Phase 2 每次轮询向前回溯的时间 |
| 查询间隔 | `query_interval` | Long（秒） |          不使用           |      可选（默认 10）      | Phase 2 两次轮询之间的时间间隔 |

## 时间窗口划分示意

```
start_time                                                    end_time
    |                                                             |
    |<--- window 1 --->|<--- window 2 --->|<--- window 3 --->|...|
    |                  |                  |                  |
    实际查询范围 = [windowStart, windowEnd)（不重叠）
```

## TOML 配置示例

### Query 模式

```toml
[run]
mode = "Query"
start_time = "2025-01-01T00:00:00Z"
end_time = "2025-06-01T00:00:00Z"    # 可选，默认为当前时间
time_window = 86400                   # 1 天
```

### QuerySync 模式

```toml
[run]
mode = "QuerySync"
start_time = "2025-01-01T00:00:00Z"
time_window = 86400                   # 1 天
time_excursion = 60                   # 仅 Phase 2：向前回溯 60 秒以捕获乱序数据
query_interval = 10                   # 每 10 秒同步一次
```

## 点位获取

两种模式都需要获取查询的点位列表 `List<Long>`，规则见 [pspace-points.md](pspace-points.md)。

## 底层 SDK 接口

历史数据查询涉及两个核心 SDK 方法：

### `client.hisReadRawAsync` — 异步查询原始历史数据

```java
client.hisReadRawAsync(start, end, ids, maxBatch, useInterpolation, callback);
```

| 参数               | 类型                            | 说明                                       |
| ------------------ | ------------------------------- | ------------------------------------------ |
| `start`            | `Long`                          | 查询开始时间（epoch 毫秒）                 |
| `end`              | `Long`                          | 查询结束时间（epoch 毫秒）                 |
| `ids`              | `List<Long>`                    | 查询的点位 ID 列表                         |
| `maxBatch`         | `int`                           | 每个点位最大返回条数，**系统上限为 10000** |
| `useInterpolation` | `boolean`                       | 边界无原始值时是否插值，一般设为 `false`   |
| `callback`         | `Consumer<PsResult<PsHisData>>` | 异步回调                                   |

**关键限制**：单次调用每个点位最多返回 10000 条数据。如果时间范围内数据量超过 10000，结果会被截断，需要通过游标方式补查。

同步版本：`client.hisReadRaw(start, end, ids, maxBatch, useInterpolation)` 返回 `PsResult<PsHisData>`。

### `client.hisReadProcessed` — 查询历史统计数据

```java
PsResult<PsHisData> result = client.hisReadProcessed(start, end, ids, enums, interval);
```

| 参数       | 类型                       | 说明                                                   |
| ---------- | -------------------------- | ------------------------------------------------------ |
| `start`    | `Long`                     | 查询开始时间（epoch 毫秒）                             |
| `end`      | `Long`                     | 查询结束时间（epoch 毫秒）                             |
| `ids`      | `List<Long>`               | 查询的点位 ID 列表                                     |
| `enums`    | `List<PsHisAggregateEnum>` | 统计方法列表，与 `ids` 一一对应（如 `PS_HIS_COUNT`）   |
| `interval` | `Long`                     | 统计的时间间隔（毫秒），按此间隔对历史数据进行分段统计 |

**用途**：用于探测数据分布（获取各时间段的数据量），以便合理划分查询窗口。

### 返回数据结构

```
PsResult<PsHisData>
├── isSuccess() / isFailInBatch()    // 两者之一为 true 即可解析数据
├── getCode()                         // 错误码
└── getData() -> List<PsHisData>     // 每个点位一个 PsHisData
    └── PsHisData
        ├── getTagId() -> Long
        └── getDataList() -> List<PsData>
            └── PsData
                ├── getValue()        // 数据值（类型由 dataType 决定）
                ├── getDataType()     // 数据类型（如 DOUBLE）
                ├── getTimestamp()    // 时间戳（epoch 毫秒）
                └── getQuality()     // 数据质量（如 GOOD）
```

### 完整历史数据查询算法（hisReadRawAll）

由于 `hisReadRawAsync` 单次最多返回 10000 条数据，查询全部历史数据需要分片策略：

**Step 1：探测数据分布**

- 将 `[start, end)` 等分为 N 个细分段（如 100 段）
- 调用 `hisReadProcessed`，使用 `PS_HIS_COUNT` 统计每个细分段中每个点位的数据条数

**Step 2：贪心合并相邻区间**

- 从左到右扫描细分段，将相邻区间合并为查询段
- 合并条件：合并后每个点位的累计数据量 ≤ 10000（`MAX_BATCH`）
- 任一点位超过 10000 时停止合并，当前区间作为一个独立查询段

**Step 3：并发异步查询各段**

- 对每个查询段调用 `hisReadRawAsync(segStart, segEnd, ids, 10000, false, callback)`
- 各段之间间隔 50ms 发送，避免瞬间打满

**Step 4：截断补查**

- 在回调中检查每个点位返回的数据条数是否达到 `MAX_BATCH`（10000）
- 如果达到，说明数据被截断，需要从最后一条数据的 `timestamp + 1` 开始，用同步 `hisReadRaw` 循环补查直到该段结束

**Step 5：合并输出**

- 按点位聚合所有段的数据
- 按时间戳排序

```
[start, end) 时间轴
    |                                                                |
    v                                                                v
    ┌──┬──┬──┬──┬──┬──┬──┬──┬──┬──┐  ← Step 1: hisReadProcessed 探测各段 count
    │10│20│ 5│ 3│ 8│50│80│90│ 2│ 1│
    └──┴──┴──┴──┴──┴──┴──┴──┴──┴──┘
    |←   合并 (46)  →|← 合并(130)→|←(3)→|  ← Step 2: 贪心合并（每段 ≤ 10000）
    |                |            |      |
    ├── 查询段 1 ────┤── 段 2 ───┤─ 段3 ┤  ← Step 3: 并发 hisReadRawAsync
    |                |            |      |
    └── Step 4: 如果返回 10000 条 → 同步游标补查
```

### 示例输出

查询点位 150019 在 2 小时内（1 秒/条）的数据：

```
====> 共划分为 1 个查询段
====> 查询完成，各测点数据统计：
----------------------------------------------------------------------
tagId: 150019, 总条数: 7200
PsData(code=PSRET_OK, value=9.809926, dataType=DOUBLE, timestamp=1772294400874, quality=GOOD)
PsData(code=PSRET_OK, value=9.858832, dataType=DOUBLE, timestamp=1772294401874, quality=GOOD)
...
```

7200 条 < 10000，因此只需 1 个查询段，无需截断补查。

## 相关代码

- 配置：[RunConfig.java](../../../src/main/java/com/taosdata/taosx/pspace/config/RunConfig.java)
- Query 实现：[QueryTask.java](../../../src/main/java/com/taosdata/taosx/pspace/run/QueryTask.java)
- QuerySync 实现：[QuerySyncTask.java](../../../src/main/java/com/taosdata/taosx/pspace/run/QuerySyncTask.java)
- 模式分发：[TaosXpSpaceMain.java](../../../src/main/java/com/taosdata/taosx/TaosXpSpaceMain.java)（`runTask` 方法）
- 查询执行器：[PSpaceQueryExecutor.java](../../../src/main/java/com/taosdata/taosx/pspace/query/PSpaceQueryExecutor.java)
