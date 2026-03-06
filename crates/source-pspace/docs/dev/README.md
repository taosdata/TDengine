# source-pspace

pSpace 数据源连接器，用于将 pSpace 实时数据库中的数据采集并写入 TDengine。

## 目录

- [1. 架构](#1-架构)
- [2. 功能](#2-功能)
- [3. DSN 参数](#3-dsn-参数)
- [4. 详细设计文档](#4-详细设计文档)

## 1. 架构

pSpace 数据接入涉及四个组件协作：

```
┌──────────────┐    HTTP API    ┌──────────┐    Rust 调用    ┌────────────────┐    TOML + CLI    ┌─────────────────────┐
│              │ ─────────────► │          │ ──────────────► │                │ ───────────────► │                     │
│ taos-explorer│                │  taosx   │                 │ source-pspace  │                  │ taosx-pspace plugin │
│   (前端 UI)  │ ◄───────────── │ (后端)   │ ◄────────────── │  (Rust crate)  │ ◄─────────────── │    (Java JAR)       │
│              │    JSON 响应   │          │   函数返回值    │                │    stdout/IPC    │                     │
└──────────────┘                └──────────┘                 └────────────────┘                  └─────────┬───────────┘
                                                                                                          │
                                                                                                          │ pSpace SDK
                                                                                                          ▼
                                                                                                ┌─────────────────────┐
                                                                                                │   pSpace Server     │
                                                                                                └─────────────────────┘
```

### 各组件职责

| 组件                    | 语言       | 职责                                                                                                                  |
| ----------------------- | ---------- | --------------------------------------------------------------------------------------------------------------------- |
| **taos-explorer**       | TypeScript | Web 前端 UI，提供 pSpace 数据源的配置交互界面。用户在此选择根节点、过滤数据点、配置采集任务参数                       |
| **taosx**               | Rust       | 后端服务，接收 explorer 的 HTTP 请求，路由到对应的数据源处理逻辑。负责任务调度、生命周期管理                          |
| **source-pspace**       | Rust       | pSpace 数据源 crate，核心业务逻辑层。负责 DSN 解析、配置生成（TOML）、IPC 通道管理、调用 Java 插件                    |
| **taosx-pspace plugin** | Java       | Java 插件（`taosx-pspace.jar`），通过 pSpace SDK 与 pSpace 服务器通信，执行连通性检查、节点查询、数据点查询和数据采集 |

### 调用链路

1. **explorer** 将用户操作封装为 HTTP 请求（包含 `from_json` 连接参数），发送给 **taosx**
2. **taosx** 将请求参数转换为 DSN，调用 **source-pspace** 中的对应函数
3. **source-pspace** 从 DSN 解析参数，构建 `PspaceConfig`，序列化为 TOML 配置文件
4. **source-pspace** 通过 `java -jar taosx-pspace.jar -m <mode> -c <config_file>` 启动 Java 插件
5. Java 插件读取 TOML 配置，通过 pSpace SDK 执行操作，结果通过 stdout（查询类）或 IPC（采集类）返回

## 2. 功能

### 2.1 连通性检查

验证 pSpace 服务器连接是否正常。

- **入口函数**：`is_valid(dsn)`
- **调用链**：taosx 收到验证请求 → `source-pspace::is_valid()` → 生成仅含 `[connection]` 的 TOML → `java -jar taosx-pspace.jar -m check -c <file>`
- **返回结果**：Java 插件通过 stdout 输出 JSON，包含 `valid`（是否连通）、`version`（pSpace 版本）等字段

### 2.2 查询 pSpace 节点

用户在 explorer 中选择根节点时，需要加载 pSpace 的节点树。pSpace 的数据组织为树形 Node 结构，用户需要逐级展开选择。

- **入口函数**：`list_nodes(dsn)`
- **DSN 参数**：`pspace_mode=nodes`、`root=<节点ID>`
- **调用链**：explorer 请求加载节点 → taosx 路由 → `source-pspace::list_datasets()` → 生成含 `[connection]` + `[nodes]` 的 TOML → `java -jar taosx-pspace.jar -m nodes -c <file>`
- **返回结果**：Java 插件通过 stdout 输出 `PspaceNode` 的 JSON 数组（`id`、`name`、`long_name`、`is_leaf`），Rust 端转换为 `DataSet` 列表返回给前端

### 2.3 查询 pSpace 数据点

用户在 explorer 中通过根节点 + 数据点名称表达式，过滤需要的数据点（Tag）。

- **入口函数**：`list_points(dsn)`
- **DSN 参数**：`pspace_mode=points`、`root=<节点ID>`、`point_name_pattern=<表达式>`（支持通配符，如 `\北京\朝阳\*气温*`）、`include_data_type=true/false`
- **调用链**：explorer 请求加载数据点 → taosx 路由 → `source-pspace::list_datasets()` → 生成含 `[connection]` + `[nodes]` + `[points]` 的 TOML → `java -jar taosx-pspace.jar -m points -c <file>`
- **返回结果**：Java 插件通过 stdout 输出 `PspacePoint` 的 JSON 数组（`id`、`name`、`type`、`long_name`、`desc`、`data_type`），Rust 端转换为 `DataSet` 列表返回给前端
- **附加功能**：
  - **预览数据点**（`csv_format=preview`）：将数据点列表输出为 CSV 格式，方便用户确认
  - **生成 CSV 配置文件**（`csv_format=full`）：为每个数据点生成完整的 CSV 配置行，用于 `csv_config_file` 模式

### 2.4 数据采集

支持三种采集模式，将 pSpace 数据点的时序数据写入 TDengine。

- **入口函数**：`pspace_to_taos(task_job_id, from, to, port_pool, cancel, with_agent, notify)`
- **调用链**：taosx 创建任务 → `source-pspace::pspace_to_taos()` → 解析完整配置 → 生成 PointModelConfig → 分配 IPC 端口 → 建立 IPC 通道 → 生成全量 TOML → `java -jar taosx-pspace.jar -m run -c <file>` → Java 插件通过 IPC 上报数据 → Rust 端 IPC handler 写入 TDengine

#### 三种采集模式

| 模式         | DSN 值                        | 说明                                           | 必填参数                                                             |
| ------------ | ----------------------------- | ---------------------------------------------- | -------------------------------------------------------------------- |
| **历史查询** | `pspace_task_mode=query`      | 按时间范围批量查询历史数据，查询完成后任务结束 | `start_time`，可选 `end_time`、`time_window`                         |
| **实时订阅** | `pspace_task_mode=subscribe`  | 订阅数据点的实时变化，持续运行直到取消         | 无额外必填参数                                                       |
| **查询同步** | `pspace_task_mode=query_sync` | 增量同步，以固定间隔轮询新数据，持续运行       | `start_time`，可选 `time_window`、`time_excursion`、`query_interval` |

#### 两种点位配置方式

| 模式         | DSN 值                                        | 说明                                                                                                                 |
| ------------ | --------------------------------------------- | -------------------------------------------------------------------------------------------------------------------- |
| **规则生成** | `point_config_mode=select_all_points`（默认） | 自动选择所有过滤到的数据点，通过 `super_table_expression` 和 `child_table_expression` 规则生成超级表和子表的映射关系 |
| **CSV 配置** | `point_config_mode=csv_config_file`           | 通过 CSV 文件逐点配置表映射、列别名、值转换、自定义 Tag 等。适用于需要精细控制的场景                                 |

## 3. DSN 参数

DSN 格式：

```
pspace://<username>:<password>@<host>:<port>?<params>
```

### 3.1 连接参数

| 参数              | 必填 | 默认值 | 说明                                           |
| ----------------- | ---- | ------ | ---------------------------------------------- |
| `host`            | 是   | —      | pSpace 服务器地址                              |
| `port`            | 否   | `5678` | pSpace 服务器端口                              |
| `username`        | 是   | —      | 用户名                                         |
| `password`        | 是   | —      | 密码                                           |
| `connect_timeout` | 否   | `30s`  | 连接超时，支持 duration 格式（如 `10s`、`1m`） |

### 3.2 节点与数据点参数

| 参数                 | 必填         | 默认值 | 说明                                                       |
| -------------------- | ------------ | ------ | ---------------------------------------------------------- |
| `pspace_mode`        | 是（查询时） | —      | 查询模式：`nodes`（查询节点）或 `points`（查询数据点）     |
| `root`               | 否           | —      | 根节点 ID，指定从哪个节点开始浏览                          |
| `point_name_pattern` | 否           | —      | 数据点名称过滤表达式，支持通配符（如 `\北京\朝阳\*气温*`） |
| `include_data_type`  | 否           | —      | 是否在数据点列表中返回数据类型信息                         |

### 3.3 任务参数

| 参数               | 必填                  | 默认值 | 说明                                                                 |
| ------------------ | --------------------- | ------ | -------------------------------------------------------------------- |
| `pspace_task_mode` | 是（采集时）          | —      | 采集模式：`query`、`subscribe`、`query_sync`                         |
| `start_time`       | query/query_sync 必填 | —      | 起始时间，ISO 8601 格式（如 `2024-01-01T00:00:00Z`）                 |
| `end_time`         | 否                    | —      | 结束时间，ISO 8601 格式                                              |
| `time_window`      | 否                    | —      | 时间窗口大小，duration 格式（如 `1h`、`1d`），写入 TOML 时转换为秒数 |
| `time_excursion`   | 否                    | —      | 时间偏移，仅 `query_sync` 模式有效，duration 格式                    |
| `query_interval`   | 否                    | —      | 查询轮询间隔，仅 `query_sync` 模式有效，duration 格式                |

### 3.4 点位配置参数

| 参数                      | 必填         | 默认值              | 说明                                                             |
| ------------------------- | ------------ | ------------------- | ---------------------------------------------------------------- |
| `point_config_mode`       | 否           | `select_all_points` | 点位配置模式：`select_all_points` 或 `csv_config_file`           |
| `super_table_expression`  | 否           | `pspace_{type}`     | 超级表命名模式，`{type}` 会替换为数据类型（如 `pspace_float`）   |
| `child_table_expression`  | 否           | `t_{point_id}`      | 子表命名模式，`{point_id}` 替换为数据点 ID                       |
| `table_primary_key`       | 否           | `original_ts`       | 主键列，可选 `original_ts`、`request_ts`、`received_ts`          |
| `table_primary_key_alias` | 否           | `ts`                | 主键列在 TDengine 中的别名                                       |
| `value_col`               | 否           | `val`               | 值列在 TDengine 中的别名                                         |
| `value_transform`         | 否           | —                   | 值转换表达式                                                     |
| `quality_col`             | 否           | `quality`           | 质量码列在 TDengine 中的别名                                     |
| `csv_config_file`         | csv 模式必填 | —                   | CSV 配置文件路径，可加 `@` 前缀                                  |
| `csv_format`              | 否           | `full`              | CSV 导出格式：`preview`（仅预览数据点）或 `full`（完整配置文件） |

### 3.5 高级参数

| 参数                 | 必填 | 默认值 | 说明                                                         |
| -------------------- | ---- | ------ | ------------------------------------------------------------ |
| `log_level`          | 否   | —      | Java 插件日志级别：`Error`、`Warn`、`Info`、`Debug`、`Trace` |
| `batch_size`         | 否   | —      | 批量写入大小                                                 |
| `batch_timeout`      | 否   | —      | 批量写入超时（毫秒）                                         |
| `read_concurrency`   | 否   | —      | 读取并发度                                                   |
| `write_concurrency`  | 否   | —      | 写入并发度                                                   |
| `keep_raw_data`      | 否   | —      | 是否保留原始数据                                             |
| `keep_raw_data_days` | 否   | —      | 原始数据保留天数                                             |
| `keep_raw_data_dir`  | 否   | —      | 原始数据保存目录                                             |

### 3.6 DSN 示例

```bash
# 连通性检查
pspace://admin:admin888@192.168.2.149:8889?connect_timeout=10s

# 查询节点
pspace://admin:admin888@192.168.2.149:8889?pspace_mode=nodes&root=150016

# 查询数据点（按名称过滤）
pspace://admin:admin888@192.168.2.149:8889?pspace_mode=points&root=150016&point_name_pattern=\北京\朝阳\*气温*&include_data_type=true

# 历史查询任务（1天时间窗口）
pspace://admin:admin888@192.168.2.149:8889?pspace_task_mode=query&start_time=2024-01-01T00:00:00Z&end_time=2024-01-02T00:00:00Z&time_window=1d&root=150016&point_name_pattern=*气温*

# 实时订阅任务
pspace://admin:admin888@192.168.2.149:8889?pspace_task_mode=subscribe&root=150016

# 查询同步任务（30分钟轮询）
pspace://admin:admin888@192.168.2.149:8889?pspace_task_mode=query_sync&start_time=2024-01-01T00:00:00Z&time_window=2h&time_excursion=20m&query_interval=30m

# 使用 CSV 配置文件
pspace://admin:admin888@192.168.2.149:8889?pspace_task_mode=query&start_time=2024-01-01T00:00:00Z&point_config_mode=csv_config_file&csv_config_file=@/path/to/config.csv
```

## 4. 详细设计文档

| 文档                                                  | 说明                                                                                     |
| ----------------------------------------------------- | ---------------------------------------------------------------------------------------- |
| [TOML 配置生成规则](design/toml-config-generation.md) | `PspaceConfig` 的 Builder 模式设计、各 TOML section 的字段说明、不同场景下的配置生成规则 |
