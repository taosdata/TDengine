# `consume_point_record` 设计文档

> 源码位置：`taosx-core/src/plugins/sink/mod.rs`

## 概述

`consume_point_record` 是 OPC 类数据源（OPC UA、OPC DA、KingHistorian、Pspace）在 IPC 数据流消费阶段的**核心写入函数**。它负责将 Agent 上报的点位数据（`PointMessage`）转换为 SQL 并写入 TDengine，同时处理各种写入异常，如表不存在、列不匹配、值超长、时间戳越界和连接断开等。

## 函数签名

```rust
async fn consume_point_record(
    pool: &TaosPool,
    taos: &mut Option<TaosConnection>,
    record: &PointMessage,
    count: &mut usize,
    config: &PointModelConfig,
    target_precision: taos::Precision,
    metrics: &IpcMetrics,
    cancel: &CancellationToken,
) -> anyhow::Result<usize>
```

### 参数

| 参数 | 类型 | 说明 |
|------|------|------|
| `pool` | `&TaosPool` | TDengine 连接池，用于连接故障时重新获取连接 |
| `taos` | `&mut Option<TaosConnection>` | 当前 TDengine 连接，可被替换（连接断开重连场景） |
| `record` | `&PointMessage` | 一批点位数据消息，包含多条 `RecordMessage` |
| `count` | `&mut usize` | 累计写入行数（由调用方持有） |
| `config` | `&PointModelConfig` | 点位模型配置 |
| `target_precision` | `taos::Precision` | 目标数据库的时间精度 |
| `metrics` | `&IpcMetrics` | IPC 指标收集器 |
| `cancel` | `&CancellationToken` | 取消令牌 |

### 返回值

`Ok(usize)` — 本次调用成功写入的点位数。

## 功能详解

### 整体处理流程

```
PointMessage (一批点位数据)
       │
       ▼
  [DRY_RUN 检查] ── 是 ──> 直接返回 0
       │ 否
       ▼
  [获取时间戳有效范围]
  min_ts = now - KEEP
  max_ts = now + 100 年
       │
       ▼
  遍历 record.records() 中每条 RecordMessage
       │
       ├── 1. 时间戳预过滤 (filter_record_message_by_ts)
       │
       ├── 2. 数据转换 (handle_transform, 如配置了 transform)
       │
       ├── 3. 生成 SQL (point_records_to_sql)
       │
       └── 4. 执行写入 + 异常处理重试循环
```

### 步骤一：时间戳有效范围获取

调用 `get_minimum_timestamp` 查询目标数据库的 `KEEP` 配置：

```sql
SELECT `precision`, `keep` FROM information_schema.ins_databases WHERE name = database()
```

- `min_ts = now - KEEP`（数据库保留时长之前的数据不可写入）
- `max_ts = now + 100 年`（TDengine 的最大时间戳限制）
- 如果查询失败（如网络错误），则跳过预过滤，依赖后续的逐行错误处理

### 步骤二：时间戳预过滤

对每条 `RecordMessage`，使用 `filter_record_message_by_ts` 过滤掉 `ts` 列超出 `(min_ts, max_ts)` 范围的行：

| 过滤结果 | 处理方式 |
|---------|---------|
| `Ok(Some(m))` | 有有效行，继续处理；记录被过滤的行数到 `drained_rows` 指标 |
| `Ok(None)` | 全部行都超出范围，跳过整个 batch |
| `Err(e)` | 过滤失败（如类型不匹配），以原始数据继续处理 |
| `ts_range` 不可用 | 不过滤，透传原始消息 |

支持的时间精度：秒、毫秒、微秒、纳秒。NULL 时间戳始终被过滤。

### 步骤三：数据转换（可选）

当 `config.need_transform()` 为 `true` 时（通过规则配置了 `value_transform`，或使用 CSV 生成点位映射），调用 `handle_transform`：

- 按点位维度对 `ts`、`received`、`value`、`request` 等列应用各自的转换规则
- 转换规则来源于 `PointModelConfig` 中的 `ColumnConfig.transform` 字段
- 转换后的数据替换原始列，生成新的 `RecordMessage`

### 步骤四：生成 SQL

调用 `point_records_to_sql` 将 `RecordMessage` 转换为两个映射：

1. **`stable_insert_map`**：`HashMap<stable_name, Vec<SqlInsertion>>`
   - 每个 `SqlInsertion` 包含一条 INSERT SQL 及其关联的元信息（列配置、tag 配置、value 列类型等）
   - SQL 格式为行式 INSERT，按超级表分组

2. **`child_table_create_sql_map`**：`HashMap<stable_name, HashMap<child_table_name, create_sql>>`
   - 保存每个子表的建表 SQL 片段，在写入失败（表不存在）时用于补建表

`RecordMessage` 的列结构：

| 列名 | 含义 |
|------|------|
| `id` | point_id（点位唯一标识） |
| `name` | point_name（点位名称） |
| `ts` | 原始时间戳（original_ts） |
| `value` | 点位值 |
| `received` | 接收时间戳 |
| `status` | 数据质量（quality） |
| `request` | 请求时间戳（可选） |

### 步骤五：写入执行与异常处理

对每条 `SqlInsertion` 执行写入，内部有一个最多 **5 次重试**的循环（`'outer` loop）。

#### 写入成功

- 累加 `count` 和 `points`
- 更新指标：`inserted_sqls`、`written_rows`、`written_points`

#### 异常处理矩阵

| 错误码 | 含义 | 处理策略 |
|--------|------|---------|
| **0x2603** / **0x0200** | 表不存在 / stmt 绑定异常 | **自动补建超级表 + 子表**，然后重试 |
| **0x2602** / **0x263F** | 列不存在 / 列数不匹配 | **自动 ALTER TABLE ADD COLUMN/TAG**，然后重试 |
| **0x2653** | 值超长 (value too long) | **自动 ALTER TABLE MODIFY COLUMN/TAG** 扩展长度，然后重试 |
| **0xE000–0xE004** / **0x000B** | 连接错误（DSN / 内部 / 断连 / 超时） | **重新获取连接**，然后重试 |
| **0x060B** | 时间戳超出范围 | **跳过该条 SQL**（记录 `failed_sqls`） |
| 其他错误 | 未知错误 | **立即传播错误**，中止任务 |

#### 自动补建超级表（0x2603 处理细节）

当写入时遇到"表不存在"错误，函数会：

1. **创建超级表**：根据 `SqlInsertion` 中保存的列配置和 tag 配置动态构建 `CREATE STABLE` 语句
   - 创建成功：记录 `created_stables` 指标
   - 遇到 `0x0360`（stable 已存在）、`0x032C`（对象正在创建中）等竞态错误：忽略
   - 连接错误：重连后重试

2. **批量创建子表**：从 `child_table_create_sql_map` 中取出对应的子表 SQL，拼装为批量 `CREATE TABLE` 语句（上限 1 MiB）
   - 创建成功：记录 `created_tables` 指标
   - 遇到 `0x032C`（对象正在创建中）：warn 后忽略
   - 连接错误：重连后重试

3. 补建完成后重新进入重试循环执行 INSERT

#### 自动加列/加 Tag（0x2602 处理细节）

1. 执行 `DESCRIBE {stable_name}` 获取当前表结构
2. 对比 `SqlInsertion` 中的列配置，找出缺失的列，执行 `ALTER TABLE ADD COLUMN`
3. 对比 tag 配置，调用 `generate_alter_sql_diff_desc` 找出缺失或需要扩容的 tag，执行 `ALTER TABLE ADD TAG` 或 `MODIFY TAG`
4. 重试写入

#### 自动扩容（0x2653 处理细节）

1. 执行 `DESCRIBE {stable_name}` 获取当前表结构
2. 调用 `generate_alter_sql_diff_desc` 对比 point_id、point_name 等 tag 的长度，生成 `ALTER TABLE MODIFY TAG` 语句扩展 VARCHAR 长度
3. 检查 value 列是否为变长类型且当前长度不足，执行 `ALTER TABLE MODIFY COLUMN` 扩展
4. 重试写入

### 指标收集

函数在各个阶段更新 `IpcMetrics`：

| 指标 | 触发时机 |
|------|---------|
| `drained_rows` | 时间戳预过滤丢弃的行数 |
| `inserted_sqls` | INSERT 执行成功 |
| `written_rows` | 成功写入的行数 |
| `written_points` | 成功写入的点位数 |
| `failed_sqls` | INSERT 失败且不可恢复 |
| `created_stables` | 自动创建超级表成功 |
| `created_tables` | 自动创建子表成功 |
| `processed_rows` | 每条 RecordMessage 处理完毕（无论成功或过滤） |

## 调用时机

### 在数据流中的位置

```
Agent 建立 IPC 连接
       │
       ▼
handle_point_message_init()   ← 一次性建表初始化
       │
       ▼
进入数据流消费循环 ◄─────────────────┐
       │                              │
       ▼                              │
IpcStreamWorker::consume()            │
  └─ StreamType::Point 分支           │
       │                              │
       ▼                              │
  consume_point_record()  ← ★ 本函数  │
       │                              │
       └──────── 下一批数据 ──────────┘
```

### 两个调用入口

1. **IPC 流模式**（`src/serve/rpc/put.rs` → `ipc_stream_writer`）：
   - 通过 `ipc_stream_writer` 中的消费循环调用
   - 每收到一个 `RecordBatch`，由 `IpcStreamWorker::consume()` 判断 `StreamType::Point` 后调用

2. **IpcStreamWorker 内部**（`taosx-core/src/plugins/sink/mod.rs` → `IpcStreamWorker::consume`）：
   - 直接在 `StreamType::Point` 分支中调用
   - 从 `opc_table_config` 中获取 `PointModelConfig` 传入

### 与 `handle_point_message_init` 的关系

| 阶段 | 函数 | 职责 |
|------|------|------|
| 初始化（一次性） | `handle_point_message_init` | 删除禁用表、预创建超级表和子表 |
| 数据消费（持续） | `consume_point_record` | 写入数据、处理异常、按需补建/修改表结构 |

`handle_point_message_init` 做的是"尽力预建表"，而 `consume_point_record` 中的异常处理确保即使预建表遗漏（如动态点位、表达式未解析的点位），在实际写入时也能自动补建。

## 注意事项

1. **不涉及 transform 配置的写入异常处理**：代码中多处注释标注了 `NOTICE 此方法不涉及 transform 配置，所以不进行"写入异常处理"`。这意味着对于 0x2603、0x2602、0x2653 等错误的自动修复逻辑，不考虑用户自定义的 transform 规则对表结构的影响。
2. **DRY_RUN 模式**：当全局 `DRY_RUN` 标志开启时，函数直接返回 0，不执行任何写入。
3. **连接重连**：遇到连接类错误（0xE000–0xE004、0x000B）时，通过 `pool.get()` 获取新连接替换当前连接，属于"就地重连"策略。
4. **SQL 长度限制**：子表批量创建 SQL 受 1 MiB 上限约束，超过时自动拆分为多条 SQL。
5. **重试上限**：单条 SQL 最多重试 5 次。5 次均失败后，如果最后一次是可恢复错误则传播该错误，否则记录 `failed_sqls` 并跳过。
