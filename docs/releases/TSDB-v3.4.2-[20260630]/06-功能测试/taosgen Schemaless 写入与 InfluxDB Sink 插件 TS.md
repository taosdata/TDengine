# taosgen Schemaless 写入与 InfluxDB Sink 插件 TS

# 1 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-05-09 | - | 1.0 | 裴亚明 | 初稿，覆盖 Schemaless 写入和 InfluxDB Sink 插件全部功能测试设计 |

# 2 测试目标

- 覆盖 Schemaless 行协议写入 TDengine 全部行为：Line Protocol 序列化、精度映射、连接写入、异常处理。
- 覆盖 InfluxDB v2 Sink 插件全部行为：连接配置、Token 认证、batch_size 分片、gzip 压缩、HTTP 写入。
- 验证功能实现正确性：`IInfluxDBClient` 接口抽象、`CurlInfluxDBClient` 实现、`SchemalessInsertDataFormatter` 格式化器、`RowSerializer` Line Protocol 序列化。
- 覆盖所有不支持范围：非法 precision/batch_size 值。
- 覆盖 YAML 配置解析：InfluxDB 连接参数、格式选项、未知键检测、CLI 参数映射、环境变量注入。

# 3 参考文档
无。

# 4 测试结论

- 单元测试用例（CI 自动执行）：49 条，全部 Pass。
- 集成测试用例（需外部服务）：16 条，依赖 TDengine / InfluxDB 环境。
- 覆盖目标：
  - 新增功能：全部覆盖。
  - 不支持范围覆盖：非法参数、未知配置键。
  - CI 不可覆盖项：`CurlInfluxDBClient::execute()`、`send_chunk()`、`write_callback()` 需通过集成测试验证。

# 5 测试环境

- OS：Linux x86_64（Ubuntu 22.04+）
- 编译器：GCC 11+（C++17）
- 构建系统：CMake + Conan（Debug 模式）
- 测试框架：CTest
- 依赖服务（集成测试）：
  - TDengine Server（localhost:6030/6041）
  - InfluxDB v2（localhost:8086），Token 认证
- 关键依赖库：libcurl/8.11.1、zlib

# 6 功能测试

## 6.1 InfluxDB 配置解析

### 6.1.1 测试要点

验证 InfluxDB YAML 配置的解析正确性和校验逻辑：
- `influxdb` 顶层节点解析为 `InfluxDBConfig` 结构体
- `precision`、`batch_size`、`gzip` 解析为 `InfluxDBFormatOptions`
- 非法 precision 值（如 `"h"`）抛出异常
- batch_size ≤ 0 抛出异常
- 未知配置键检测并报错
- 默认值正确（url=`http://localhost:8086`、org=`default`、bucket=`default`、precision=`ns`、batch_size=1000、gzip=false）

### 6.1.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| CFG-001 | 完整配置解析 | YAML 包含 url、token、org、bucket、precision、batch_size、gzip 全部字段，解析后各字段值正确 | Pass |
| CFG-002 | 默认值验证 | 仅配置 token，其余使用默认值，验证 url=`http://localhost:8086`、org=`default`、bucket=`default` | Pass |
| CFG-003 | precision 合法值 ns | `precision: ns` 解析成功，值为 `"ns"` | Pass |
| CFG-004 | precision 合法值 us | `precision: us` 解析成功 | Pass |
| CFG-005 | precision 合法值 ms | `precision: ms` 解析成功 | Pass |
| CFG-006 | precision 合法值 s | `precision: s` 解析成功 | Pass |
| CFG-007 | precision 非法值 | `precision: h` 抛出异常，消息包含 "Invalid precision" | Pass |
| CFG-008 | batch_size 正整数 | `batch_size: 500` 解析成功，值为 500 | Pass |
| CFG-009 | batch_size 为零 | `batch_size: 0` 抛出异常，消息包含 "must be greater than 0" | Pass |
| CFG-010 | batch_size 为负数 | `batch_size: -1` 抛出异常 | Pass |
| CFG-011 | gzip 布尔解析 | `gzip: true` 和 `gzip: false` 均正确解析 | Pass |
| CFG-012 | 未知键检测 | 配置中包含 `unknown_key: value`，抛出异常 | Pass |
| CFG-013 | 空配置 | `influxdb` 节点为空，使用全部默认值，不抛出异常 | Pass |

## 6.2 InfluxDB 客户端（Mock）

### 6.2.1 测试要点

验证 `InfluxDBClient` 包装器和 `IInfluxDBClient` 接口的依赖注入机制：
- `set_client()` 注入 Mock 客户端后，所有方法委托到 Mock
- `execute()` 成功/失败路径正确传递返回值
- `connect()` 失败时返回 `false`
- 未连接时 `is_connected()` 返回 `false`
- 空数据写入正确处理

### 6.2.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| CLI-001 | 默认构造 | 构造 `InfluxDBClient`，`is_connected()` 返回 `false` | Pass |
| CLI-002 | Mock 注入 | `set_client()` 注入 MockInfluxDBClient 后，`connect()` 调用到 Mock | Pass |
| CLI-003 | execute 成功 | Mock 返回 `true`，`execute()` 返回 `true` | Pass |
| CLI-004 | execute 失败 | Mock 返回 `false`，`execute()` 返回 `false` | Pass |
| CLI-005 | connect 失败 | Mock `connect()` 返回 `false`，客户端 `connect()` 返回 `false` | Pass |
| CLI-006 | 空数据 execute | 传入空 `InfluxDBInsertData`（lines 为空），Mock 正常调用 | Pass |
| CLI-007 | close 调用 | `close()` 调用到 Mock，`is_connected()` 变为 `false` | Pass |
| CLI-008 | execute 调用计数 | 多次调用 `execute()`，Mock 记录调用次数正确 | Pass |

## 6.3 InfluxDB Sink 插件（Mock）

### 6.3.1 测试要点

验证 `InfluxDBSinkPlugin` 的完整生命周期：
- 工厂方法正确创建插件实例
- 连接/断开/重连生命周期
- `format()` 正确调用 Formatter 生成 Line Protocol 数据
- `write()` 正确委托到 InfluxDBClient
- 重试机制在写入失败时触发
- 不支持的数据类型抛出异常
- 未连接时写入抛出异常

### 6.3.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| PLG-001 | 工厂创建 | 通过配置创建 InfluxDBSinkPlugin 实例，不抛出异常 | Pass |
| PLG-002 | 连接成功 | 注入 Mock 客户端，`connect()` 返回 `true`，`is_connected()` 为 `true` | Pass |
| PLG-003 | 连接失败 | Mock `connect()` 返回 `false`，`is_connected()` 为 `false` | Pass |
| PLG-004 | 关闭连接 | `close()` 后 `is_connected()` 为 `false` | Pass |
| PLG-005 | format 格式化 | 传入 MemoryBlock，`format()` 返回 InfluxDBInsertData，lines 非空 | Pass |
| PLG-006 | write 成功 | Mock `execute()` 返回 `true`，`write()` 返回 `true` | Pass |
| PLG-007 | write 失败重试 | Mock 首次 `execute()` 返回 `false`，验证重试机制触发 | Pass |
| PLG-008 | 不支持的数据类型 | 传入 SqlInsertData 类型，`write()` 抛出异常包含 "Unsupported data type" | Pass |
| PLG-009 | 未连接时写入 | 未调用 `connect()` 直接 `write()`，抛出异常 "not connected" | Pass |
| PLG-010 | set_client/get_client | `set_client()` 注入后 `get_client()` 返回非空指针 | Pass |

## 6.4 Schemaless 格式化器

### 6.4.1 测试要点

验证 `SchemalessInsertDataFormatter` 的 Line Protocol 序列化正确性：
- measurement 名称正确输出
- tag 键值对以逗号分隔紧跟 measurement
- field 键值对正确序列化（整数后缀 `i`、浮点数、字符串引号、布尔值）
- 时间戳根据精度正确换算
- 多行以 `\n` 分隔
- 空批次返回空结果

### 6.4.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| FMT-001 | 基本格式化 | 单行数据格式化为 `measurement,tag=val field=val timestamp` 格式 | Pass |
| FMT-002 | 多行格式化 | 多行数据以 `\n` 分隔，行数与 total_rows 一致 | Pass |
| FMT-003 | 整数类型 | int/bigint 列输出为 `123i` 格式 | Pass |
| FMT-004 | 浮点类型 | float/double 列输出为 `3.14` 格式（无后缀） | Pass |
| FMT-005 | 精度映射 ms | `precision: ms` 映射为 `TSDB_SML_TIMESTAMP_MILLI_SECONDS` | Pass |
| FMT-006 | 精度映射 us | `precision: us` 映射为 `TSDB_SML_TIMESTAMP_MICRO_SECONDS` | Pass |
| FMT-007 | 精度映射 ns | `precision: ns` 映射为 `TSDB_SML_TIMESTAMP_NANO_SECONDS` | Pass |
| FMT-008 | 空批次 | MemoryBlock 行数为 0，返回空 FormatResult | Pass |

## 6.5 TDengine Schemaless 写入（集成测试）

### 6.5.1 测试要点

验证通过 TDengine 原生连接执行 schemaless 写入的正确性：
- 基本 Line Protocol 数据成功写入，TDengine 自动建表
- 多个 measurement 在同一批次写入
- 空数据不崩溃（返回错误但进程稳定）
- 非法 Line Protocol 格式返回失败
- 未连接状态下 execute 返回 `false` 而非崩溃
- 大批量数据（1000 行）写入成功

### 6.5.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| SML-001 | 基本写入 | 写入 2 行 `cpu,host=h1 usage=50.0 ts` 和 `cpu,host=h2 usage=75.5 ts`，execute 返回 `true` | Pass |
| SML-002 | 多 measurement 写入 | 同一批次包含 cpu、mem、disk 三个 measurement，execute 返回 `true` | Pass |
| SML-003 | 空数据 | lines 为空字符串，total_rows=0，execute 返回错误但不崩溃 | Pass |
| SML-004 | 非法格式 | lines 为 "this is not valid line protocol!!!"，execute 返回 `false` | Pass |
| SML-005 | 未连接写入 | 配置不可达地址（192.0.2.1），execute 返回 `false`，不崩溃 | Pass |
| SML-006 | 大批量写入 | 写入 1000 行数据（10 个子表，每子表 100 行），execute 返回 `true` | Pass |
| SML-007 | 全数据类型写入 | 使用 `conf/tdengine-schemaless-all-types.yaml` 配置，schema 包含 15 种数据类型（TIMESTAMP、TINYINT、TINYINT UNSIGNED、SMALLINT、SMALLINT UNSIGNED、INT、INT UNSIGNED、BIGINT、BIGINT UNSIGNED、FLOAT、DOUBLE、BOOL、BINARY、NCHAR、VARCHAR），10 个子表各 10 行共 100 行写入 TDengine，`describe` 验证表结构包含全部列，`select count(*)` 验证行数正确 | Pass |

## 6.6 InfluxDB HTTP 写入（集成测试）

### 6.6.1 测试要点

验证通过 HTTP 向 InfluxDB v2 实际写入数据的正确性（需要运行中的 InfluxDB 服务）：
- Token 认证成功建立连接
- 单次写入成功，HTTP 响应 204
- batch_size 分片正确（如 batch_size=200，1000 行拆为 5 个请求）
- gzip 压缩写入成功
- 错误 Token 返回 401
- 不存在的 Bucket 返回 404
- 写入的数据可通过 InfluxDB 查询验证

### 6.6.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| HTTP-001 | 基本连接 | 使用正确 Token 连接 InfluxDB v2，`connect()` 返回 `true` | Pass |
| HTTP-002 | 单次写入 | 写入 100 行 cpu 指标数据，execute 返回 `true` | Pass |
| HTTP-003 | batch_size 分片 | batch_size=200，写入 1000 行，日志显示 5 次 HTTP 请求 | Pass |
| HTTP-004 | gzip 压缩 | `gzip: true` 时写入成功，请求头包含 `Content-Encoding: gzip` | Pass |
| HTTP-005 | 错误 Token | 使用错误 Token，execute 返回 `false`，日志包含 401 错误 | Pass |
| HTTP-006 | 不存在 Bucket | 配置不存在的 Bucket，execute 返回 `false`，日志包含 404 错误 | Pass |
| HTTP-007 | 数据验证 | 写入 cpu 指标后，通过 InfluxDB Flux 查询验证数据行数和字段值正确 | Pass |
| HTTP-008 | mem 指标写入 | 使用 influxdb-telegraf-mem.yaml 配置写入 mem 指标，查询验证数据正确 | Pass |
| HTTP-009 | 全数据类型写入 | 使用 `conf/influxdb-all-types.yaml` 配置，schema 包含 15 种数据类型（TIMESTAMP、TINYINT、TINYINT UNSIGNED、SMALLINT、SMALLINT UNSIGNED、INT、INT UNSIGNED、BIGINT、BIGINT UNSIGNED、FLOAT、DOUBLE、BOOL、BINARY、NCHAR、VARCHAR），10 个子表各 10 行共 100 行写入 InfluxDB，通过 Flux 查询验证 measurement `all_types` 数据行数和字段存在性正确 | Pass |

## 6.7 E2E 完整流程（集成测试）

### 6.7.1 测试要点

验证 `taosgen -c xxx.yaml` 命令行方式运行的端到端正确性：
- Schemaless 写入 TDengine 全流程（建库→schemaless 写入→查询验证）
- InfluxDB 写入全流程（连接→写入→查询验证）
- 多种 Telegraf 指标类型（cpu、mem）
- 并发写入（concurrency > 1）

### 6.7.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| E2E-001 | Schemaless 写入 TDengine | `taosgen -c tdengine-schemaless.yaml`，10000 子表 × 1000 行，验证写入成功并查询数据 | Pass |
| E2E-002 | InfluxDB CPU 指标 | `taosgen -c influxdb-telegraf-cpu.yaml`，10 子表 × 100 行，InfluxDB 查询验证 | Pass |
| E2E-003 | InfluxDB MEM 指标 | `taosgen -c influxdb-telegraf-mem.yaml`，10 子表 × 100 行，InfluxDB 查询验证 | Pass |
| E2E-004 | 环境变量 Token | `INFLUXDB_TOKEN=xxx taosgen -c xxx.yaml`（YAML 中无 token），写入成功 | Pass |
| E2E-005 | CLI 参数覆盖 | `taosgen -c xxx.yaml --host http://localhost:8086 --password xxx`，写入成功 | Pass |

## 6.8 长期稳定性测试

无。

## 6.9 性能测试

无独立性能测试。写入性能通过 taosgen 自身的吞吐量统计报告观测。

## 6.10 安全性测试

无独立安全性测试。Token 保护机制通过功能测试间接验证。

# 7 兼容性测试

| # | 测试场景 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | 已有 SQL 格式不受影响 | 使用 `format: sql` 配置运行 taosgen，功能正常 | Pass |
| 2 | 已有 STMT 格式不受影响 | 使用 `format: stmt` 配置运行 taosgen，功能正常 | Pass |
| 3 | Kafka 插件不受影响 | Kafka 插件测试用例全部通过（TestKafkaSinkPlugin 等） | Pass |
| 4 | MQTT 插件不受影响 | MQTT 插件测试用例全部通过 | Pass |
| 5 | 全量回归 | 95 个 CTest 用例全部通过，无回归 | Pass |

# 8 已知问题和限制

- **taosAdapter WebSocket 精度兼容性**：taosAdapter WebSocket 模式下，微秒精度标识 `"us"` 无法被 taosAdapter 识别（仅识别 `"u"`）。Schemaless 写入必须使用 native 连接。
- **InfluxDB 认证方式**：InfluxDB v2 Write API（`/api/v2/write`）仅支持 Token 认证，不支持 Basic Auth（用户名/密码）。
- **CI 覆盖率限制**：`CurlInfluxDBClient` 中的 `execute()`、`send_chunk()`、`write_callback()` 三个函数涉及实际 HTTP 通信，CI 环境无 InfluxDB 服务，无法在单元测试中覆盖。这些函数通过集成测试验证。
