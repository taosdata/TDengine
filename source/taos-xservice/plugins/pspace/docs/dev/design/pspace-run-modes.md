# pSpace Plugin 运行模式

## 概述

pSpace plugin 在 `-m run` 模式下支持三种任务模式，通过 TOML 配置文件中的 `[run].mode` 字段指定。

| 模式     | `mode` 值     | 用途                | 生命周期       |
| -------- | ------------- | ------------------- | -------------- |
| 历史查询 | `"Query"`     | 一次性历史数据迁移  | 查询完成后退出 |
| 实时订阅 | `"Subscribe"` | 实时数据同步        | 持续运行       |
| 查询同步 | `"QuerySync"` | 历史回填 + 持续同步 | 持续运行       |

## Query — 历史查询模式

通过查询指定时间范围内的历史数据，从 pSpace 中查询出来并写入 TSDB 数据库，完成一次性数据迁移任务。

### 配置参数

| 参数     | TOML 字段        | 类型       |  必填  | 默认值        | 说明                           |
| -------- | ---------------- | ---------- | :----: | ------------- | ------------------------------ |
| 开始时间 | `start_time`     | String     | **是** | —             | 数据查询的起始时间戳           |
| 结束时间 | `end_time`       | String     |   否   | 当前时间      | 数据查询的截止时间戳           |
| 查询窗口 | `time_window`    | Long（秒） |   否   | 86400（1 天） | 划分子查询的时间窗口大小       |
| 乱序偏移 | `time_excursion` | Long（秒） |   否   | 0             | 每个查询窗口向前偏移的时间间隔 |

### 配置示例

```toml
[run]
mode = "Query"
start_time = "2025-01-01T00:00:00Z"
end_time = "2025-06-01T00:00:00Z"
time_window = 86400
time_excursion = 60
```

### 详细设计

见 [pspace-query.md](pspace-query.md)

## Subscribe — 实时订阅模式

通过 pSpace SDK 的实时订阅接口，接收 pSpace 服务端推送的实时数据，写入 TSDB 数据库，完成实时数据同步。

### 配置参数

无额外配置参数，仅需指定 `mode = "Subscribe"`。

### 配置示例

```toml
[run]
mode = "Subscribe"
```

### 详细设计

见 [pspace-subscribe.md](pspace-subscribe.md)

## QuerySync — 查询同步模式

先将从开始时间到当前时刻的历史数据迁移完成（Phase 1），然后不退出，继续按照固定的时间间隔轮询查询，将新产生的数据同步到 TSDB 中（Phase 2）。

### 配置参数

| 参数     | TOML 字段        | 类型       |  必填  | 默认值        | 说明                             |
| -------- | ---------------- | ---------- | :----: | ------------- | -------------------------------- |
| 开始时间 | `start_time`     | String     | **是** | —             | 数据查询的起始时间戳             |
| 查询窗口 | `time_window`    | Long（秒） |   否   | 86400（1 天） | 划分子查询的时间窗口大小         |
| 乱序偏移 | `time_excursion` | Long（秒） |   否   | 0             | 每个查询窗口向前偏移的时间间隔   |
| 查询间隔 | `query_interval` | Long（秒） |   否   | 10            | Phase 2 中两次查询之间的时间间隔 |

### 配置示例

```toml
[run]
mode = "QuerySync"
start_time = "2025-01-01T00:00:00Z"
time_window = 86400
time_excursion = 60
query_interval = 10
```

### 详细设计

见 [pspace-query.md](pspace-query.md)

## 三种模式对比

| 维度                  | Query              | Subscribe                 | QuerySync            |
| --------------------- | ------------------ | ------------------------- | -------------------- |
| 底层 SDK 方法         | `hisReadRawAsync`  | `realNewSubscribeAndRead` | `hisReadRawAsync`    |
| 数据来源              | 历史数据查询       | 实时推送                  | 历史查询 + 轮询查询  |
| 需要 `start_time`     | 是                 | 否                        | 是                   |
| 需要 `end_time`       | 可选               | 否                        | 否（固定为当前时间） |
| 需要 `query_interval` | 否                 | 否                        | 可选                 |
| 退出条件              | 查询完成           | 连接断开                  | 连接断开             |
| 典型场景              | 一次性历史数据迁移 | 实时监控                  | 先补齐历史再持续同步 |

## Local Only — 仅写本地模式（调试用）

提供一个配置项，使 pSpace plugin 只将查询/订阅的数据写入本地 rawdata 文件，**不上报 taosx**（不建立 Netty TCP 连接）。用途：

- 独立测试 taosx-pspace plugin 的查询/订阅功能
- 排查数据源侧的问题（验证从 pSpace 获取的数据是否正确）
- 在没有 taosx 服务的环境中调试

### 配置参数

`local_only` 是 Java 插件独有的参数，不由 taosx Rust 代码生成，仅在手动编写 TOML 调试时使用。

| 参数       | TOML section | TOML 字段    | 类型    | 默认值  | 说明                                     |
| ---------- | ------------ | ------------ | ------- | ------- | ---------------------------------------- |
| 仅本地模式 | `[report]`   | `local_only` | Boolean | `false` | 设为 `true` 时只写本地文件，不连接 taosx |

### 配置示例

```toml
[report]
local_only = true              # 不连接 taosx，仅写本地

[advanced_options]
keep_raw_data = true           # 必须同时开启，否则数据会丢失
keep_raw_data_dir = "./output"
```

### 行为逻辑

```
if local_only == true:
    - 跳过 Netty TCP 连接（不需要 remote 配置）
    - 强制开启 keep_raw_data = true
    - 数据仅写入本地 rawdata 文件
else:
    - 正常连接 taosx（需要 remote 配置）
    - 根据 advanced_options.keep_raw_data 决定是否同时写入本地文件
```

### 完整配置示例

```toml
[connection]
server = "192.168.2.149"
port = 8889
username = "admin"
password = "admin888"
timeout_sec = 30

[nodes]
root = 150016

[points]
include_data_type = true
point_ids = [150019]

[run]
mode = "Query"
start_time = "2026-03-01T00:00:00+08:00"
end_time = "2026-03-01T05:00:00+08:00"
time_window = 3600
time_excursion = 0

[report]
local_only = true

[advanced_options]
log_level = "Debug"
keep_raw_data = true
keep_raw_data_days = 7
keep_raw_data_dir = "./raw_data"
```

## 公共依赖

所有模式共用的设计文档：

- 点位获取规则：[pspace-points.md](pspace-points.md)
- 数据映射与 Arrow Schema：[pspace-data-mapping.md](pspace-data-mapping.md)
- 高级选项（含 raw data 配置）：[pspace-advanced.md](pspace-advanced.md)
