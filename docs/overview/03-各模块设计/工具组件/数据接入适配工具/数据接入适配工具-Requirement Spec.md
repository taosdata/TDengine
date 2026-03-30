# 数据接入适配工具-Requirement Spec

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2025-01-01 | 2025-01-01 | 1.0 | 谭雪峰 | 安可送测第一版 |
| 2025-11-24 | 2025-11-24 | 1.1 | 霍琳贺 | 1. 功能需求增项 1. 其他需求增强 |

## 2. 引言

在物联网、工业互联网等数据密集型应用场景中，高效、稳定、易用的数据接入方案是构建可靠数据平台的关键。taosAdapter 作为 TDengine 数据库的组件，旨在为用户提供一种轻量级、高性能、可扩展的数据接入解决方案，简化数据接入流程，提升数据写入效率，满足海量时序数据的实时处理需求。

### 2.1 术语与缩写名词

1. **fqdn： **全限定域名（Fully Qualified Domain Name），用于唯一标识互联网上的主机或服务器。
2. **RESTful 接口： **基于 HTTP 协议的接口，利用常见方法（GET、POST、PUT、DELETE）执行操作。
3. **WebSocket： **一种全双工通信协议，用于在客户端和服务器之间建立持久连接，适合低延迟和实时数据传输的场景。
4. **JSON：** 一种轻量级的数据交换格式，易于人类阅读和编写，同时便于机器解析和生成，广泛用于 RESTful 接口的数据传输。

### 2.2 相关文档资料

1. **InfluxDB V1 写接口：** https://docs.influxdata.com/influxdb/v2.0/reference/api/influxdb-1x/write/
2. **OpenTSDB：**
  - [http://opentsdb.net/docs/build/html/api_http/put.html](http://opentsdb.net/docs/build/html/api_http/put.html)
  - [http://opentsdb.net/docs/build/html/api_telnet/put.html](http://opentsdb.net/docs/build/html/api_telnet/put.html)
1. **Prometheus remote_read 和 remote_write：** https://prometheus.io/blog/2019/10/10/remote-read-meets-streaming/
2. **node_exporter：** https://github.com/prometheus/node_exporter
3. **collectd：** https://www.collectd.org/
4. **StatsD：** https://github.com/statsd/statsd
5. **icinga2 OpenTSDB writer：** https://icinga.com/docs/icinga-2/latest/doc/14-features/#opentsdb-writer
6. **TCollector：** http://opentsdb.net/docs/build/html/user_guide/utilities/tcollector.html

### 2.3 优先级要求

- **重要程度**：高。
- **期望交付时间**：与 TDengine 3.3.0.0 同期发布。

### 2.4 版本要求

- 开源。
- 在社区版和企业版均支持。
- 预计发布版本：TDengine v3.3.0.0。

## 3. 需求目标

taosAdapter 是 TDengine 的适配器工具，旨在实现以下目标：
1. 提供 RESTful 和 WebSocket 接口，简化数据摄取与查询操作。
2. 支持 InfluxDB 和 OpenTSDB 协议，便于应用迁移到 TDengine。
3. 提供与数据采集软件的无缝对接能力：
   - telegraf
   - collectd
   - StatsD
   - icinga2 OpenTSDB writer
   - TCollector
   - node_exporter
   - prometheus

## 4. 功能需求

| 序号 | **功能类别** | **功能名称** | 功能描述 |
| --- | --- | --- | --- |
| 1 | 数据写入和查询 | RESTful SQL 执行接口 | 提供标准的 RESTful 接口，支持通过 HTTP 协议执行 SQL。 |
| 2 | 数据写入和查询 | WebSocket SQL 执行接口 | 支持通过 WebSocket 接口执行 SQL 写入和查询并获取结果。 |
| 3 | 数据写入和查询 | WebSocket schemaless 写入接口 | 支持通过 WebSocket 接口写入 schemeless 数据 |
| 4 | 数据写入和查询 | WebSocket stmt 写入和查询接口 | 支持通过 WebSocket 接口进行 stmt 写入和查询 |
| 5 | 数据订阅 | Websocket 订阅 | 支持通过 WebSocket 接口进行 tmq 订阅 |
| 6 | 数据写入和查询 | Websocket 订阅结果写入 | 支持通过 WebSocket 接口将 tmq 订阅结果写入TDengine |
| 7 | 数据写入和查询 | Websocket 查询结果写入 | 支持通过 WebSocket 接口将查询结果的数据块写入TDengine |
| 8 | 兼容性支持 | InfluxDB 协议支持 | 兼容 InfluxDB 写接口，支持 InfluxDB v1 格式的数据写入。 |
| 9 | 兼容性支持 | OpenTSDB 协议支持 | 支持 OpenTSDB 的 JSON 和 telnet 格式写入接口。 |
| 10 | 数据采集集成 | telegraf 集成 | 支持 telegraf InfluxDB 协议写入 |
| 11 | 数据采集集成 | collectd 集成 | 支持 collectd 协议接入 |
| 12 | 数据采集集成 | StatsD 集成 | 支持通过 StatsD 协议采集监控指标并写入 TDengine。 |
| 13 | 数据采集集成 | icinga2 OpenTSDB writer 集成 | 支持 icinga2 OpenTSDB writer 使用 OpenTSDB 协议写入 |
| 14 | 数据采集集成 | TCollector 集成 | 支持 TCollector 使用 OpenTSDB 写入 |
| 15 | 数据采集集成 | node_exporter 集成 | 支持获取 node_exporter 指标写入 TDengine |
| 16 | 数据采集集成 | Prometheus 集成 | 提供 Prometheus 数据的 remote_write 和 remote_read 接口。 |
| 17 | 身份认证 | RESTful 多种认证机制支持 | 提供 Basic 和自定义认证机制，支持获取授权码以进行认证。 |
| 18 | 系统监控 | 健康检查接口 | 提供 /ping 接口用于健康检查，返回系统运行状态。 |
| 19 | 系统监控 | 指标监控接口 | 提供 /metrics 接口暴露 Prometheus 格式的运行指标，支持监控 taosAdapter 自身的运行状态和性能数据。 |
| 20 | 身份认证 | RESTful 多种认证机制支持 | 提供 Basic 和自定义认证机制，支持获取授权码以进行认证。 |

## 5. 性能需求

- 写入性能：taosBenchmark rest模式写入，并发1000，典型电表表结构，100w子表，每子表写入100，数据随机，qps达到40w/s以上。
- 查询性能：使用jmeter向JDBC rest请求，select last_row(*) from test.${tbname}语句，tbname随机，并发1000，循环50次，qps达到1000/s以上。
- 启动时间：taosAdapter 应在 5 秒内完成初始化并开始接收请求。

## 6. 安全需求

- 认证机制：
  - 支持 Basic 认证和 Token 认证机制。
- 安全连接：
  - 支持 TLS/SSL 加密连接。
- 权限控制：
  - 支持连接白名单和认证鉴权。
  - 支持 CORS 限制。
  - 支持查询限流。
  - 支持并发限制。

## 7. 其他需求

描述功能、性能之外的需求。
- 兼容性需求
  - 兼容 TDengine v3.3.0.0 及以上版本。
  - 支持常见 Linux 发行版（Ubuntu、CentOS、Debian 等）、Windows、macOS。
  - 要求 Go 1.23 或以上版本，支持 CGO。
- 接口需求
  - 提供标准化的 RESTful 和 WebSocket 接口。
  - WebSocket 支持 JSON 和二进制协议的数据传输。
  - 支持兼容 InfluxDB 写接口，支持 InfluxDB v1 格式的数据写入。
  - 支持 OpenTSDB 的 JSON 和 telnet 格式写入接口。
  - 提供健康检查接口（`/ping`）和监控指标接口（`/metrics`）。
- 运维需求
  - 提供详细的日志记录，支持日志级别配置（trace、debug、info、warning、error）。
  - 支持通过配置文件、命令行参数和环境变量进行配置。
  - 支持日志轮转和结构化日志输出。
  - 提供 Prometheus 指标暴露，便于监控 taosAdapter 运行状态。
  - 提供 taosKeeper 指标上报，便于 TSDB 相关组件实现统一监控。
- 易用性需求
  - 错误可读性：在接口返回中包含详细的错误信息，便于用户定位问题。
  - 支持通过命令行参数 `-V` 查看版本信息。
  - 提供配置文件示例和详细的配置说明。
- 可扩展性需求
  - 采用插件化架构，新增数据源协议支持无需修改核心代码。
  - 支持通过配置启用/禁用特定插件功能。
  - 连接池支持动态调整和并发限制配置。
- 测试需求
  - 覆盖所有主要功能的单元测试，代码覆盖率达到 80% 以上。
  - taosBenchmark 和 JMeter 压测验证性能指标。
  - 支持 Address Sanitizer (ASAN) 内存安全测试。
  - 集成 govulncheck 进行依赖安全扫描。
