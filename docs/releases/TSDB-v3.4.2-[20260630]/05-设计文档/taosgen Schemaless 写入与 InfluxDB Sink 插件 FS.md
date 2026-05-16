# 概要设计说明书（Functional Spec）— taosgen Schemaless 写入与 InfluxDB Sink 插件

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-05-09 | - | 1.0 | 裴亚明 | 初稿 |

## 2. 背景

某客户需要测试从 InfluxDB 迁移到 TDengine 的能力，同时也需要验证 TDengine 消费 Telegraf 数据的能力。为确保 PoC 顺利进行，需要在公司内部先完成相关测试。当前 taosgen 存在以下不足：

1. **TDengine 写入格式受限**：`tdengine/insert` 行动的 `format` 仅支持 `sql`（SQL INSERT）和 `stmt`（参数绑定）两种格式，无法以 InfluxDB Line Protocol（行协议）方式写入 TDengine。Telegraf 等工具原生使用 Line Protocol，taosgen 需要具备模拟此类数据写入的能力。
2. **缺少 InfluxDB 写入目标**：taosgen 目前支持 TDengine、MQTT、Kafka 三种写入目标，无法直接将生成的数据写入 InfluxDB。在迁移 PoC 中，需要先向 InfluxDB 写入基准数据，再验证 TDengine 的读取和迁移能力。

为支撑上述场景，需要新增两个特性：

- **特性 A**：`tdengine/insert` 行动的 `format` 新增 `schemaless` 格式，支持通过 TDengine 原生 C 或 WebSocket API 以 Line Protocol 方式写入数据。
- **特性 B**：新增 `influxdb/write` 行动和 InfluxDB v2 Sink 插件，支持将 taosgen 生成的数据以 Line Protocol 格式写入 InfluxDB v2。

## 3. 定义

| 术语 | 定义 |
| --- | --- |
| Line Protocol | InfluxDB 定义的文本行协议格式：`measurement,tag=value field=value timestamp`，广泛用于时序数据交换 |
| Schemaless 写入 | TDengine 的无模式写入方式，接收 Line Protocol 数据后自动创建超级表和子表，无需预先定义 Schema |
| Telegraf | InfluxData 开源的系统指标采集代理，原生产生 Line Protocol 格式数据 |
| InfluxDB v2 | InfluxDB 2.x 版本，使用 Token 认证和 Organization/Bucket 组织方式 |
| Sink 插件 | taosgen 的数据写入插件，负责将生成的数据发送到目标系统（如 TDengine、Kafka、MQTT、InfluxDB） |
| batch_size | 每次 HTTP 请求中包含的最大 Line Protocol 行数，用于将大请求拆分为多个小请求 |
| gzip | HTTP 请求体的 gzip 压缩，可减少网络传输量 |
| precision | 时间戳精度，支持 `ns`（纳秒）、`us`（微秒）、`ms`（毫秒）、`s`（秒） |

## 4. 行为说明

### 4.1 特性 A：Schemaless 行协议写入 TDengine

#### 4.1.1 核心语义

> **`format: schemaless` 表示将 taosgen 生成的列式数据转换为 InfluxDB Line Protocol 文本，通过 TDengine 原生 C 或 WebSocket API（`taos_schemaless_insert_raw_ttl_with_reqid`）写入 TDengine。TDengine 自动根据 Line Protocol 数据创建超级表和子表，无需预先建表。**

#### 4.1.2 适用范围

| 连接方式 | 是否支持 | 备注 |
| --- | --- | --- |
| native（原生连接） | 是 | TDengine C API 直接调用 `taos_schemaless_insert_raw_ttl_with_reqid` |
| WebSocket | 是 | us 精度不支持，taosAdapter 对精度标识存在兼容性问题（`"us"` vs `"u"`） |

#### 4.1.3 配置参数

`tdengine/insert` 行动的 `with` 块中：

| 参数 | 类型 | 值 | 说明 |
| --- | --- | --- | --- |
| `format` | string | `schemaless` | 指定使用行协议格式写入 |
| `concurrency` | int | 正整数 | 并发写入线程数 |

数据库精度在 `tdengine.props` 中指定（如 `precision 'ms'`），格式化器自动将精度映射到 TDengine SML 常量。

#### 4.1.4 Line Protocol 输出格式

```
measurement,tag1=val1,tag2=val2 field1=value1,field2=value2 timestamp
```

**类型映射规则**：

| taosgen 列类型 | Line Protocol 表示 | 示例 |
| --- | --- | --- |
| `int` / `bigint` | 整数后缀 `i` | `42i` |
| `float` / `double` | 浮点数 | `3.14` |
| `bool` | `true` / `false` | `true` |
| `binary` / `nchar` | 双引号包裹字符串 | `"hello"` |
| `timestamp`（第一列） | 裸数字（根据精度换算） | `1609459200000` |

#### 4.1.5 出错处理

| 异常场景 | 行为 |
| --- | --- |
| 使用 WebSocket 或 RESTful 连接 | 抛出异常："Schemaless insert requires native connection" |
| Line Protocol 格式不合法 | TDengine 返回错误码，记录错误日志（截断至 300 字符），返回 `false` |
| 空数据（无行） | TDengine 返回 "line num is invalid" 错误，不崩溃 |
| TDengine 不可达 | `connect()` 失败，`execute()` 返回 `false`，不崩溃 |
| libtaos 版本过低（无 schemaless 函数） | 抛出异常："taos_schemaless_insert_raw_ttl_with_reqid not available" |

#### 4.1.6 使用示例

```yaml
tdengine:
  dsn: taos://root:taosdata@127.0.0.1:6030/tsbench
  drop_if_exists: true
  props: precision 'ns' vgroups 4

schema:
  name: cpu
  tbname:
    prefix: host_
    count: 100
  columns:
    - name: ts
      type: timestamp
      start: now
      precision: ns
      step: 10s
    - name: usage_idle
      type: float
      min: 50
      max: 99
  tags:
    - name: host
      type: binary(64)
      values: [server01, server02, server03]
  generation:
    interlace: 1
    rows_per_table: 1000

jobs:
  insert-data:
    steps:
      - uses: tdengine/create-database
      - uses: tdengine/insert
        with:
          format: schemaless
          concurrency: 10
```

运行：

```bash
taosgen -c tdengine-schemaless.yaml
```

---

### 4.2 特性 B：InfluxDB v2 Sink 插件

#### 4.2.1 核心语义

> **`influxdb/write` 行动将 taosgen 生成的数据转换为 InfluxDB Line Protocol 格式，通过 HTTP POST 请求发送到 InfluxDB v2 的 `/api/v2/write` 端点。支持 Token 认证、时间戳精度控制、gzip 压缩和批量分片。**

#### 4.2.2 连接配置

YAML 顶层 `influxdb` 块定义连接参数：

| 参数 | 类型 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| `url` | string | 否 | `http://localhost:8086` | InfluxDB 服务地址 |
| `token` | string | 是 | - | InfluxDB v2 API Token |
| `org` | string | 否 | `default` | Organization 名称 |
| `bucket` | string | 否 | `default` | Bucket 名称 |

#### 4.2.3 写入参数

`influxdb/write` 行动的 `with` 块：

| 参数 | 类型 | 必填 | 默认值 | 合法值 | 说明 |
| --- | --- | --- | --- | --- | --- |
| `concurrency` | int | 否 | 1 | 正整数 | 并发写入线程数 |
| `precision` | string | 否 | `ns` | `ns`, `us`, `ms`, `s` | 时间戳精度 |
| `batch_size` | int | 否 | 1000 | 正整数 | 每次 HTTP 请求的最大行数 |
| `gzip` | bool | 否 | `false` | `true`, `false` | 是否启用 gzip 压缩 |

#### 4.2.4 认证机制

InfluxDB v2 Write API 仅支持 Token 认证。HTTP 请求头格式：

```
Authorization: Token <token>
```

Token 可通过以下方式提供（优先级从高到低）：

1. `--password` CLI 参数
2. `INFLUXDB_TOKEN` 环境变量
3. YAML 配置文件中的 `influxdb.token` 字段

> **注意**：InfluxDB v2 的 Basic Auth（用户名/密码）仅适用于 `/api/v2/signin`，不适用于 `/api/v2/write` 端点。因此本插件不支持用户名/密码认证方式。

#### 4.2.5 batch_size 分片行为

当待写入的总行数超过 `batch_size` 时，插件将自动拆分为多个 HTTP 请求：

| 场景 | 行为 |
| --- | --- |
| `total_rows ≤ batch_size` | 单次 HTTP POST 发送全部数据 |
| `total_rows > batch_size` | 按行分片，每个分片包含最多 `batch_size` 行，依次发送 |
| 某个分片失败 | 立即返回失败，不继续发送后续分片 |

**示例**：`batch_size=200`，`total_rows=1000` → 拆分为 5 个 HTTP 请求，每个包含 200 行。

#### 4.2.6 gzip 压缩

启用 `gzip: true` 时：

- 请求体使用 zlib 进行 gzip 压缩
- HTTP 请求头添加 `Content-Encoding: gzip`
- 适用于网络带宽受限或数据量大的场景
- 压缩增加少量 CPU 开销，但可显著减少网络传输量

#### 4.2.7 CLI 参数映射

| CLI 参数 | 目标字段 | 说明 |
| --- | --- | --- |
| `--host` | `influxdb.url` | 如包含 `://` 则直接使用，否则拼接为 `http://{host}:8086` |
| `--password` | `influxdb.token` | 将密码作为 InfluxDB Token 使用 |

#### 4.2.8 环境变量

| 环境变量 | 目标字段 | 说明 |
| --- | --- | --- |
| `INFLUXDB_TOKEN` | `influxdb.token` | InfluxDB API Token，适用于 CI/CD 或容器环境 |

#### 4.2.9 出错处理

| 异常场景 | 行为 |
| --- | --- |
| Token 未配置 | 连接时 InfluxDB 返回 401 错误，记录日志 |
| URL 不可达 | `connect()` 失败，记录 libcurl 错误信息 |
| Bucket 不存在 | HTTP 响应 404，`execute()` 返回 `false`，记录错误日志 |
| HTTP 响应码 ≠ 204 | `execute()` 返回 `false`，记录 HTTP 响应码和响应体 |
| precision 不合法 | 配置解析阶段抛出异常（支持 `ns`/`us`/`ms`/`s`） |
| batch_size ≤ 0 | 配置解析阶段抛出异常 |
| 未知配置键 | 配置解析阶段报错 |

#### 4.2.10 使用示例

**模拟 Telegraf CPU 指标写入 InfluxDB**：

```yaml
influxdb:
  url: http://localhost:8086
  token: "your-influxdb-token"
  org: default
  bucket: default

schema:
  name: cpu
  tbname:
    prefix: host_
    count: 10
  columns:
    - name: ts
      type: timestamp
      start: now
      precision: ns
      step: 10s
    - name: usage_idle
      type: float
      min: 50
      max: 99
    - name: usage_system
      type: float
      min: 0
      max: 20
    - name: usage_user
      type: float
      min: 0
      max: 50
  tags:
    - name: host
      type: binary(64)
      values: [server01, server02, server03]
    - name: cpu
      type: binary(16)
      values: [cpu-total, cpu0, cpu1]
  generation:
    interlace: 1
    rows_per_table: 100
    rows_per_batch: 500

jobs:
  write-data:
    steps:
      - uses: influxdb/write
        with:
          concurrency: 2
          precision: ns
          batch_size: 500
          gzip: true
```

运行：

```bash
# 通过配置文件指定 token
taosgen -c influxdb-telegraf-cpu.yaml

# 或通过环境变量传递 token
export INFLUXDB_TOKEN="your-token"
taosgen -c influxdb-telegraf-cpu.yaml

# 或通过命令行参数
taosgen -c influxdb-telegraf-cpu.yaml --password "your-token"
```

**模拟 Telegraf 内存指标写入 InfluxDB**：

```yaml
influxdb:
  url: http://localhost:8086
  token: "your-influxdb-token"
  org: default
  bucket: default

schema:
  name: mem
  tbname:
    prefix: node_
    count: 10
  columns:
    - name: ts
      type: timestamp
      start: now
      precision: ns
      step: 10s
    - name: total
      type: bigint
      min: 8000000000
      max: 8500000000
    - name: used
      type: bigint
      min: 2000000000
      max: 6500000000
    - name: used_percent
      type: float
      min: 20
      max: 85
  tags:
    - name: host
      type: binary(64)
      values: [node01, node02, node03]
  generation:
    interlace: 1
    rows_per_table: 100

jobs:
  write-data:
    steps:
      - uses: influxdb/write
        with:
          concurrency: 2
          precision: ns
          batch_size: 500
          gzip: true
```

## 5. 性能

1. **Schemaless 写入**：通过 TDengine 原生 C 或 WebSocket API 直接写入，性能与 TDengine 原生 或 WebSocket schemaless 接口一致。数据在内存中序列化为 Line Protocol 文本，一次 API 调用写入整个批次。
2. **InfluxDB 写入**：通过 HTTP 发送，性能受网络延迟和 InfluxDB 服务端处理能力影响。`batch_size` 控制每次请求大小，避免单次请求过大。`gzip` 可减少网络传输量但增加 CPU 开销。`concurrency` 控制并发写入线程数。
3. **Line Protocol 序列化**：使用 `fmt::memory_buffer` 避免频繁内存分配，序列化开销较小。

## 6. 安全

1. **Token 保护**：InfluxDB API Token 支持通过环境变量（`INFLUXDB_TOKEN`）和 CLI 参数（`--password`）传递，避免明文写入配置文件。Token 仅在 HTTP 请求的 `Authorization` 头中传输。
2. **TDengine 认证**：Schemaless 写入使用 TDengine DSN 中配置的用户名和密码进行认证，与现有 SQL/STMT 写入方式一致。
3. **网络安全**：InfluxDB URL 支持 `https://` 协议，可通过 TLS 加密传输。

## 7. 兼容性

1. **完全向后兼容**：两个特性均为新增功能，不修改任何已有行为。
2. **`format` 参数向后兼容**：原有的 `sql` 和 `stmt` 格式不受影响，`schemaless` 为新增选项。
3. **插件架构兼容**：InfluxDB Sink 插件遵循已有的插件架构模式（与 Kafka、MQTT 一致），不影响现有插件。
4. **构建系统兼容**：新增 `libcurl` 依赖通过 Conan 管理，不影响未启用 InfluxDB 插件的构建。

## 8. 运维

1. **Schemaless 写入**：无额外运维需求，TDengine 数据库需预先创建（可通过 `tdengine/create-database` 步骤自动创建）。
2. **InfluxDB 写入**：需确保 InfluxDB v2 服务已运行，且 Token 具有目标 Bucket 的写入权限。可通过 `influx auth list` 命令查看 Token 权限。
3. **日志**：两个特性均通过 taosgen 标准日志框架输出信息，包含连接状态、写入结果、错误详情等。

## 9. 使用场景

### 9.1 InfluxDB → TDengine 迁移 PoC

先使用 taosgen 向 InfluxDB 写入基准数据，再测试 TDengine 的数据迁移工具读取和迁移能力：

```bash
# 步骤 1：向 InfluxDB 写入基准数据
taosgen -c influxdb-telegraf-cpu.yaml

# 步骤 2：使用 TDengine 迁移工具从 InfluxDB 读取数据
# （使用 TDengine 相关迁移工具）
```

### 9.2 模拟 Telegraf 数据写入 TDengine

taosgen 模拟 Telegraf 产生的 CPU/内存等系统指标，通过 Line Protocol 直接写入 TDengine，验证 TDengine 消费 Telegraf 格式数据的能力：

```bash
taosgen -c tdengine-schemaless.yaml
```

### 9.3 对比测试 InfluxDB 与 TDengine 写入性能

使用相同 Schema 配置，分别向 InfluxDB 和 TDengine 写入数据，对比写入吞吐量：

```bash
# 向 InfluxDB 写入
taosgen -c influxdb-telegraf-cpu.yaml

# 向 TDengine 写入（schemaless 格式）
taosgen -c tdengine-schemaless.yaml
```

### 9.4 大批量数据分片写入 InfluxDB

通过 `batch_size` 控制每次 HTTP 请求的行数，避免单次请求超时：

```yaml
jobs:
  write-data:
    steps:
      - uses: influxdb/write
        with:
          batch_size: 500    # 每次请求最多 500 行
          gzip: true         # 压缩减少网络传输
          concurrency: 10    # 10 线程并发写入
```

## 10. 约束和限制

**约束：**

- InfluxDB 写入仅支持 InfluxDB v2 API（`/api/v2/write`），不支持 InfluxDB v1。
- InfluxDB 认证仅支持 Token 方式，不支持用户名/密码。
- `precision` 合法值为 `ns`、`us`、`ms`、`s`，其他值在配置解析阶段报错。

**限制：**

- InfluxDB 写入不支持重试策略的自定义（当前使用 taosgen 默认重试机制）。
- InfluxDB 写入不支持 InfluxDB v2 的其他端点（如查询、删除等），仅支持写入。

## 11. 常见错误和排查

| 错误现象 | 可能原因 | 排查方法 |
| --- | --- | --- |
| "taos_schemaless_insert_raw_ttl_with_reqid not available" | libtaos 版本过低 | 升级 TDengine 客户端库 |
| InfluxDB 返回 401 Unauthorized | Token 错误或未配置 | 检查 `influxdb.token` 配置或 `INFLUXDB_TOKEN` 环境变量 |
| InfluxDB 返回 404 Not Found | Bucket 不存在 | 使用 `influx bucket list` 确认 Bucket 存在 |
| "Invalid precision: xxx" | precision 值不合法 | 使用 `ns`、`us`、`ms` 或 `s` |
| "batch_size must be greater than 0" | batch_size 配置为 0 或负数 | 设置为正整数 |
| HTTP 请求超时 | 单次请求数据量过大 | 减小 `batch_size` 值 |
| "SML line invalid data" | Line Protocol 格式不合法 | 检查 Schema 配置，确保列名和类型正确 |

## 12. 可观测性

1. **写入日志**：taosgen 在 INFO 级别输出写入目标信息（如 `Inserting data into: InfluxDB(http://localhost:8086/default)`）。
2. **错误日志**：写入失败时在 ERROR 级别输出完整错误信息，包括 HTTP 响应码和响应体。
3. **性能指标**：taosgen 统计每个写入线程的吞吐量和延迟，在任务结束时输出汇总报告。
4. **TDengine 侧**：Schemaless 写入创建的超级表和子表可通过 `SHOW STABLES` 和 `SHOW TABLES` 查看。
5. **InfluxDB 侧**：写入的数据可通过 InfluxDB UI 或 Flux 查询语言验证。

## 13. 安装和卸载

无特殊要求。两个特性随 taosgen 版本发布。InfluxDB 插件依赖 `libcurl`，通过 Conan 自动管理。Schemaless 写入依赖 `libtaos.so`，需系统安装 TDengine 客户端。

## 14. 文档

- 已更新官网文档：

新增内容包括：
- InfluxDB 连接参数说明
- `influxdb/write` 行动及其参数说明
- `format: schemaless` 选项说明
- InfluxDB 配置示例

## 15. 参考文档

- InfluxDB v2 Write API 文档：https://docs.influxdata.com/influxdb/v2/api/#operation/PostWrite
- InfluxDB Line Protocol 规范：https://docs.influxdata.com/influxdb/v2/reference/syntax/line-protocol/
- Telegraf 官方文档：https://docs.influxdata.com/telegraf/

## 16. 附录

无。
