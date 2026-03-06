# source-pspace TOML 配置生成规则

本文档描述 `source-pspace`（Rust 代码）如何从 DSN 参数构建 `PspaceConfig`，并序列化为 TOML 配置文件下发给 `taosx-pspace` Java 插件。

## 整体架构

```
前端 DSN ──► source-pspace (Rust) ──► PspaceConfig ──► TOML 文件 ──► taosx-pspace.jar
```

1. 前端通过 DSN 传递所有参数
2. Rust 端通过 `PspaceConfigBuilder` 按需解析 DSN 中的各项参数，构建 `PspaceConfig`
3. `PspaceConfig` 通过 `toml::to_string()` 序列化为 TOML 字符串
4. TOML 写入临时文件或任务目录下的 `collect.toml`
5. 通过 `java -jar taosx-pspace.jar -m <mode> -c <config_file>` 启动插件

## PspaceConfig 结构

`PspaceConfig` 是顶层配置结构，各字段映射为 TOML 中的 section：

```rust
pub struct PspaceConfig {
    pub connection: PspaceConnection,                // [connection]     — 必选
    pub nodes: Option<PspaceNodesConfig>,             // [nodes]          — 可选
    pub points: Option<PspacePointsConfig>,           // [points]         — 可选
    pub run: Option<PspaceTaskConfig>,                // [run]            — 可选
    pub report: Option<PspaceReportConfig>,           // [report]         — 可选
    pub advanced_options: Option<AdvancedOptions>,     // [advanced_options] — 可选
}
```

所有 `Option` 类型字段标注了 `#[serde(skip_serializing_if = "Option::is_none")]`，值为 `None` 时不会写入 TOML。

## Builder 模式与各业务场景

`PspaceConfigBuilder` 提供按需链式调用：

| 场景                | Builder 调用链                                                                         | 生成的 TOML section                     |
| ------------------- | -------------------------------------------------------------------------------------- | --------------------------------------- |
| 连接测试 (check)    | `.build()`                                                                             | `[connection]`                          |
| 浏览节点 (nodes)    | `.with_nodes().build()`                                                                | `[connection]` + `[nodes]`              |
| 浏览数据点 (points) | `.with_nodes().with_points().build()`                                                  | `[connection]` + `[nodes]` + `[points]` |
| 运行任务 (run)      | `.with_nodes().with_points().with_run().with_report().with_advanced_options().build()` | 全部 section                            |

## 各 Section 详细说明

### `[connection]` — 连接配置（必选）

始终生成。从 DSN 的 host、port、username、password 及 `connect_timeout` 参数解析。

| TOML 字段     | 类型   | DSN 来源          | 默认值     | 说明                                                   |
| ------------- | ------ | ----------------- | ---------- | ------------------------------------------------------ |
| `server`      | String | DSN host          | 无（必填） | pSpace 服务器地址                                      |
| `port`        | u16    | DSN port          | `5678`     | pSpace 服务器端口                                      |
| `username`    | String | DSN username      | 无（必填） | 用户名                                                 |
| `password`    | String | DSN password      | 无（必填） | 密码                                                   |
| `timeout_sec` | u64    | `connect_timeout` | `30`       | 连接超时（秒），DSN 中以 duration 格式传入（如 `10s`） |

示例：

```toml
[connection]
server = "192.168.2.149"
port = 8889
username = "admin"
password = "admin888"
timeout_sec = 30
```

### `[nodes]` — 节点配置（可选）

通过 `.with_nodes()` 解析。仅当 DSN 中包含 `root` 参数时生成。

| TOML 字段 | 类型 | DSN 来源 | 说明      |
| --------- | ---- | -------- | --------- |
| `root`    | u64  | `root`   | 根节点 ID |

示例：

```toml
[nodes]
root = 150016
```

### `[points]` — 数据点配置（可选）

通过 `.with_points()` 解析。当 `point_name_pattern` 或 `include_data_type` 任一参数存在时生成。

| TOML 字段           | 类型                 | DSN 来源             | 说明                                                           |
| ------------------- | -------------------- | -------------------- | -------------------------------------------------------------- |
| `name_filter`       | Option\<String\>     | `point_name_pattern` | 数据点名称过滤模式（支持通配符）                               |
| `include_data_type` | Option\<bool\>       | `include_data_type`  | 是否在返回结果中包含数据类型                                   |
| `point_ids`         | Option\<Vec\<u64\>\> | 运行时动态设置       | 数据点 ID 列表，CSV 模式下由代码从 `PointModelConfig` 提取填充 |

**注意**：`point_ids` 不从 DSN 解析，而是在 `pspace_to_taos` 运行时，当使用 CSV 配置模式时，从 `PointModelConfig.point_config_map` 的 key 中提取并回填。

示例：

```toml
[points]
name_filter = '\\北京\\朝阳\\*气温*'
include_data_type = true
point_ids = [150019, 150021, 150023]
```

### `[run]` — 任务运行配置（可选）

通过 `.with_run()` 解析。仅当 DSN 中包含 `pspace_task_mode` 参数时生成。支持三种运行模式，不同模式下各字段的启用情况不同：

| TOML 字段        | 类型               | DSN 来源           | Query   | Subscribe | QuerySync |
| ---------------- | ------------------ | ------------------ | ------- | --------- | --------- |
| `mode`           | PspaceTaskMode     | `pspace_task_mode` | ✅      | ✅        | ✅        |
| `start_time`     | Option\<DateTime\> | `start_time`       | ✅ 必填 | ✗         | ✅ 必填   |
| `end_time`       | Option\<DateTime\> | `end_time`         | ✅ 可选 | ✗         | ✗         |
| `time_window`    | Option\<i64\>      | `time_window`      | ✅ 可选 | ✗         | ✅ 可选   |
| `time_excursion` | Option\<i64\>      | `time_excursion`   | ✗       | ✗         | ✅ 可选   |
| `query_interval` | Option\<i64\>      | `query_interval`   | ✗       | ✗         | ✅ 可选   |

**值转换规则**：

- `mode` 序列化为枚举字符串：`"Query"` / `"Subscribe"` / `"QuerySync"`
- `start_time`、`end_time` 以 RFC 3339 格式输出（带时区）
- `time_window`、`time_excursion`、`query_interval` 从 DSN 的 duration 格式（如 `1d`、`2h`、`10m`）转换为**秒数**（i64）

Query 模式示例：

```toml
[run]
mode = "Query"
start_time = "2026-02-01T00:00:00+08:00"
end_time = "2026-02-27T00:00:00+08:00"
time_window = 86400
```

Subscribe 模式示例：

```toml
[run]
mode = "Subscribe"
```

QuerySync 模式示例：

```toml
[run]
mode = "QuerySync"
start_time = "2026-03-03T00:00:00+08:00"
time_window = 86400
time_excursion = 0
query_interval = 10
```

### `[report]` — 上报配置（可选）

通过 `.with_report()` 解析。仅当 `[run]` section 存在时才生成。

| TOML 字段 | 类型             | 来源           | 说明                                                          |
| --------- | ---------------- | -------------- | ------------------------------------------------------------- |
| `remote`  | Option\<String\> | 运行时动态设置 | IPC 上报地址（`ip:port`），由 `pspace_to_taos` 分配端口后回填 |

**注意**：`remote` 字段不从 DSN 解析，而是在 `pspace_to_taos` 运行时通过端口池分配 IPC 端口后，以 `"127.0.0.1:{port}"` 格式动态填入。

示例：

```toml
[report]
remote = "127.0.0.1:6051"
```

### `[advanced_options]` — 高级选项（可选）

通过 `.with_advanced_options()` 解析。当以下任一参数在 DSN 中存在时才生成该 section。所有字段均为 `Option` 类型，仅有值时才写入 TOML。

| TOML 字段            | 类型               | DSN 来源             | 说明                                                    |
| -------------------- | ------------------ | -------------------- | ------------------------------------------------------- |
| `log_level`          | Option\<LogLevel\> | `log_level`          | 日志级别：`Error` / `Warn` / `Info` / `Debug` / `Trace` |
| `read_concurrency`   | Option\<usize\>    | `read_concurrency`   | 读取并发数                                              |
| `write_concurrency`  | Option\<usize\>    | `write_concurrency`  | 写入并发数                                              |
| `batch_size`         | Option\<usize\>    | `batch_size`         | 批量写入大小                                            |
| `batch_timeout`      | Option\<usize\>    | `batch_timeout`      | 批量写入超时（毫秒）                                    |
| `keep_raw_data`      | Option\<bool\>     | `keep_raw_data`      | 是否保留原始数据                                        |
| `keep_raw_data_days` | Option\<usize\>    | `keep_raw_data_days` | 原始数据保留天数                                        |
| `keep_raw_data_dir`  | Option\<String\>   | `keep_raw_data_dir`  | 原始数据保存目录                                        |

示例：

```toml
[advanced_options]
log_level = "Debug"
write_concurrency = 2
batch_size = 1000
batch_timeout = 1
keep_raw_data = true
keep_raw_data_days = 7
keep_raw_data_dir = "./raw_data"
```

## 完整 TOML 示例（Query 模式任务）

以下是一个完整的运行任务配置文件示例：

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
name_filter = '\\北京\\朝阳\\*气温*'
include_data_type = true

[run]
mode = "Query"
start_time = "2026-02-01T00:00:00+08:00"
end_time = "2026-02-27T00:00:00+08:00"
time_window = 86400
time_excursion = 0

[report]
remote = "127.0.0.1:6051"

[advanced_options]
log_level = "Info"
batch_size = 1000
batch_timeout = 1
keep_raw_data = true
keep_raw_data_days = 7
keep_raw_data_dir = "./raw_data"
```

## 不同调用场景生成的 TOML

### 连接测试 (check)

调用方：`is_valid_impl()` → `java -jar taosx-pspace.jar -m check -c <file>`

只包含 `[connection]`：

```toml
[connection]
server = "192.168.2.149"
port = 8889
username = "admin"
password = "admin888"
timeout_sec = 30
```

### 浏览节点 (nodes)

调用方：`list_nodes()` → `java -jar taosx-pspace.jar -m nodes -c <file>`

包含 `[connection]` + `[nodes]`。

### 浏览数据点 (points)

调用方：`list_points()` → `java -jar taosx-pspace.jar -m points -c <file>`

包含 `[connection]` + `[nodes]` + `[points]`。

### 运行任务 (run)

调用方：`pspace_to_taos()` → `java -jar taosx-pspace.jar -m run -c <file>`

包含全部 section。配置文件保存在 `$DATA_DIR/tasks/{task_id}/{job_id}/collect.toml`。

运行时会动态修改：

1. `report.remote` → 设置为分配的 IPC 端口地址
2. `points.point_ids` → 当使用 CSV 配置模式时，从 `PointModelConfig` 提取数据点 ID 列表

## 关键实现细节

- **序列化**：使用 `serde` + `toml` crate，所有 Rust 结构体派生 `Serialize` / `Deserialize`
- **跳过空值**：所有 `Option` 字段标注 `skip_serializing_if = "Option::is_none"`，避免在 TOML 中生成无意义的空字段
- **duration 转换**：DSN 中的 duration 字符串（如 `1d`、`2h`、`10m`、`30s`）统一转换为秒数写入 TOML
- **密码脱敏**：日志输出时将 `connection.password` 替换为 `******`
- **配置文件生命周期**：
  - check / nodes / points 场景使用 `NamedTempFile`（临时文件，自动清理）
  - run 场景写入任务目录下的持久化文件 `collect.toml`
