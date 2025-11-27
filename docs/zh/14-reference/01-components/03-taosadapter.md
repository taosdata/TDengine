---
title: "taosAdapter 参考手册"
sidebar_label: "taosAdapter"
toc_max_heading_level: 4
---

import Prometheus from "./_prometheus.mdx"
import CollectD from "./_collectd.mdx"
import StatsD from "./_statsd.mdx"
import Icinga2 from "./_icinga2.mdx"
import TCollector from "./_tcollector.mdx"

taosAdapter 是一个 TDengine TSDB 的配套工具，是 TDengine TSDB 集群和应用程序之间的桥梁和适配器。它提供了一种易于使用和高效的方式来直接从数据收集代理软件（如 Telegraf、StatsD、collectd 等）摄取数据。它还提供了 InfluxDB/OpenTSDB 兼容的数据摄取接口，允许 InfluxDB/OpenTSDB 应用程序无缝移植到 TDengine TSDB。
TDengine TSDB 的各语言连接器通过 WebSocket 接口与 TDengine TSDB 进行通信，因此必须安装 taosAdapter。

架构图如下：

![TDengine TSDB Database taosAdapter Architecture](taosAdapter-architecture.webp)

## 功能列表

taosAdapter 提供了以下功能：

- WebSocket 接口：
  支持通过 WebSocket 协议执行 SQL、无模式数据写入、参数绑定和数据订阅功能。
- InfluxDB v1 数据写入：
  [https://docs.influxdata.com/influxdb/v2.0/reference/api/influxdb-1x/write/](https://docs.influxdata.com/influxdb/v2.0/reference/api/influxdb-1x/write/)
- 兼容 OpenTSDB JSON 和 telnet 格式写入：
  - [http://opentsdb.net/docs/build/html/api_http/put.html](http://opentsdb.net/docs/build/html/api_http/put.html)
  - [http://opentsdb.net/docs/build/html/api_telnet/put.html](http://opentsdb.net/docs/build/html/api_telnet/put.html)
- collectd 数据写入：
  collectd 是一个系统统计收集守护程序，请访问 [https://collectd.org/](https://collectd.org/) 了解更多信息。
- StatsD 数据写入：
  StatsD 是一个简单而强大的统计信息汇总的守护程序。请访问 [https://github.com/statsd/statsd](https://github.com/statsd/statsd) 了解更多信息。
- icinga2 OpenTSDB writer 数据写入：
  icinga2 是一个收集检查结果指标和性能数据的软件。请访问 [https://icinga.com/docs/icinga-2/latest/doc/14-features/#opentsdb-writer](https://icinga.com/docs/icinga-2/latest/doc/14-features/#opentsdb-writer) 了解更多信息。
- TCollector 数据写入：
  TCollector 是一个客户端进程，从本地收集器收集数据，并将数据推送到 OpenTSDB。请访问 [http://opentsdb.net/docs/build/html/user_guide/utilities/tcollector.html](http://opentsdb.net/docs/build/html/user_guide/utilities/tcollector.html) 了解更多信息。
- OpenMetrics 采集写入：
  OpenMetrics 是云原生监控领域的新兴标准，扩展并规范了 Prometheus 的指标格式，已成为现代监控工具的事实标准。请访问 [https://github.com/prometheus/OpenMetrics/blob/main/specification/OpenMetrics.md](https://github.com/prometheus/OpenMetrics/blob/main/specification/OpenMetrics.md) 了解更多信息。
- Prometheus remote_read 和 remote_write：
  remote_read 和 remote_write 是 Prometheus 数据读写分离的集群方案。请访问 [https://prometheus.io/blog/2019/10/10/remote-read-meets-streaming/#remote-apis](https://prometheus.io/blog/2019/10/10/remote-read-meets-streaming/#remote-apis) 了解更多信息。
- node_exporter 采集写入：
  node_export 是一个机器指标的导出器。请访问 [https://github.com/prometheus/node_exporter](https://github.com/prometheus/node_exporter) 了解更多信息。
- RESTful 接口：
  [RESTful API](../../connector/rest-api)

### WebSocket 接口

各语言连接器通过 taosAdapter 的 WebSocket 接口，能够实现 SQL 执行、无模式写入、参数绑定和数据订阅功能。参考 [开发指南](../../../develop/connect/#websocket-连接)。

### InfluxDB v1 数据写入

您可以使用任何支持 HTTP 协议的客户端访问 Restful 接口地址 `http://<fqdn>:6041/influxdb/v1/write` 来写入 InfluxDB 兼容格式的数据到 TDengine TSDB。

支持 InfluxDB 参数如下：

- `db` 指定 TDengine TSDB 使用的数据库名
- `precision` TDengine TSDB 使用的时间精度
- `u` TDengine TSDB 用户名
- `p` TDengine TSDB 密码
- `ttl` 自动创建的子表生命周期，以子表的第一条数据的 TTL 参数为准，不可更新。更多信息请参考 [创建表文档](../../taos-sql/table/#创建表)的 TTL 参数。
- `table_name_key` 自定义子表名使用的标签名，如果设置了该参数，则子表名将使用该标签对应的值。

注意：目前不支持 InfluxDB 的 token 验证方式，仅支持 Basic 验证和查询参数验证。
示例：

```shell
curl --request POST http://127.0.0.1:6041/influxdb/v1/write?db=test --user "root:taosdata" --data-binary "measurement,host=host1 field1=2i,field2=2.0 1577836800000000000"
```

### OpenTSDB JSON 和 telnet 格式写入

您可以使用任何支持 HTTP 协议的客户端访问 Restful 接口地址 `http://<fqdn>:6041/<APIEndPoint>` 来写入 OpenTSDB 兼容格式的数据到 TDengine TSDB。EndPoint 如下：

```text
/opentsdb/v1/put/json/<db>
/opentsdb/v1/put/telnet/<db>
```

### collectd 数据写入

<CollectD />

### StatsD 数据写入

<StatsD />

### icinga2 OpenTSDB writer 数据写入

<Icinga2 />

### TCollector 数据写入

<TCollector />

### OpenMetrics 采集写入

OpenMetrics 是一种由 CNCF（云原生计算基金会）支持的开放标准，专注于规范指标数据的采集和传输，是云原生生态中监控和可观测性系统的核心规范之一。

从 **3.3.7.0** 版本开始，taosAdapter 支持 OpenMetrics v1.0.0 数据采集与写入，同时兼容 Prometheus 0.0.4 协议，确保与 Prometheus 生态的无缝集成。

启用 OpenMetrics 数据采集写入需要以下步骤：

- 启用 taosAdapter 的配置 `open_metrics.enable`
- 设置 OpenMetrics 的相关配置
- 重新启动 taosAdapter

### Prometheus remote_read 和 remote_write

<Prometheus />

### node_exporter 采集写入

从 **3.3.7.0** 版本开始，可以使用 OpenMetrics 插件替代 node_exporter 进行数据采集和写入。

Prometheus 使用的由 \*NIX 内核暴露的硬件和操作系统指标的输出器。

- 启用 taosAdapter 的配置 node_exporter.enable
- 设置 node_exporter 的相关配置
- 重新启动 taosAdapter

### RESTful 接口

您可以使用任何支持 HTTP 协议的客户端通过访问 RESTful 接口地址 `http://<fqdn>:6041/rest/sql` 来写入数据到 TDengine TSDB 或从 TDengine TSDB 中查询数据。细节请参考 [REST API 文档](../../connector/rest-api/)。

## 安装

taosAdapter 是 TDengine TSDB 服务端软件 的一部分，如果您使用 TDengine TSDB server 您不需要任何额外的步骤来安装 taosAdapter。您可以从 [涛思数据官方网站](https://docs.taosdata.com/releases/tdengine/) 下载 TDengine TSDB server 安装包。如果需要将 taosAdapter 分离部署在 TDengine TSDB server 之外的服务器上，则应该在该服务器上安装完整的 TDengine TSDB 来安装 taosAdapter。如果您需要使用源代码编译生成 taosAdapter，您可以参考 [构建 taosAdapter](https://github.com/taosdata/taosadapter/blob/3.0/BUILD-CN.md) 文档。

安装完成后使用命令 `systemctl start taosadapter` 可以启动 taosAdapter 服务。

## 配置

taosAdapter 支持通过命令行参数、环境变量和配置文件来进行配置。默认配置文件是 /etc/taos/taosadapter.toml，可用过 `-c` 或 `--config` 命令行参数指定配置文件。

命令行参数优先于环境变量优先于配置文件，命令行用法是 arg=val，如 taosadapter -p=30000 --debug=true。

示例配置文件参见 [example/config/taosadapter.toml](https://github.com/taosdata/taosadapter/blob/3.0/example/config/taosadapter.toml)。

### 基础配置

taosAdapter 的基础配置参数如下：

- **`debug`**：是否启用调试模式（pprof）
  - **设置为 `true` 时**：（默认值）启用 go pprof 调试模式，允许使用 `http://<fqdn>:<port>/debug/pprof` 访问调试信息。
  - **设置为 `false` 时**：关闭调试模式，不允许访问调试信息。
- **`instanceId`**：taosAdapter 实例 id，用于区分不同 taosAdapter 的日志，默认值：32。
- **`port`**：taosAdapter 对外提供 HTTP/WebSocket 服务的端口，默认值：6041。
- **`taosConfigDir`**：TDengine TSDB 的配置文件目录，默认值：`/etc/taos`。该目录下的 `taos.cfg` 文件将被加载。

从 **3.3.4.0 版本** 开始，taosAdapter 支持设置调用 C 方法并发调用数：

- **`maxAsyncConcurrentLimit`**

  设置 C 异步方法的最大并发调用数（0 表示使用 CPU 核心数）。

- **`maxSyncConcurrentLimit`**

  设置 C 同步方法的最大并发调用数（0 表示使用 CPU 核心数）。

### 跨域配置

使用浏览器进行接口调用时，请根据实际情况设置如下跨域（CORS）参数：

- **`cors.allowAllOrigins`**：是否允许所有来源访问，默认为 `true`。
- **`cors.allowOrigins`**：允许跨域访问的来源列表，支持多个来源，以逗号分隔。
- **`cors.allowHeaders`**：允许跨域访问的请求头列表，支持多个请求头，以逗号分隔。
- **`cors.exposeHeaders`**：允许跨域访问的响应头列表，支持多个响应头，以逗号分隔。
- **`cors.allowCredentials`**：是否允许跨域请求包含用户凭证，如 cookies、HTTP 认证信息或客户端 SSL 证书。
- **`cors.allowWebSockets`**：是否允许 WebSockets 连接。

如果不通过浏览器进行接口调用无需关心这几项配置。

以上配置对以下接口生效：

- RESTful 接口请求
- WebSocket 接口请求
- InfluxDB v1 写接口
- OpenTSDB HTTP 写入接口

关于 CORS 协议细节请参考：[https://www.w3.org/wiki/CORS_Enabled](https://www.w3.org/wiki/CORS_Enabled) 或 [https://developer.mozilla.org/zh-CN/docs/Web/HTTP/CORS](https://developer.mozilla.org/zh-CN/docs/Web/HTTP/CORS)。

### 连接池配置

taosAdapter 使用连接池管理与 TDengine TSDB 的连接，以提高并发性能和资源利用率。连接池配置对以下接口生效，且以下接口共享一个连接池：

- RESTful 接口请求
- InfluxDB v1 写接口
- OpenTSDB JSON 和 telnet 格式写入
- Telegraf 数据写入
- collectd 数据写入
- StatsD 数据写入
- node_exporter 数据写入
- OpenMetrics 数据写入
- Prometheus remote_read 和 remote_write

连接池的配置参数如下：

- **`pool.maxConnect`**：连接池允许的最大连接数，默认值为 2 倍 CPU 核心数。建议保持默认设置。
- **`pool.maxIdle`**：连接池中允许的最大空闲连接数，默认与 `pool.maxConnect` 相同。建议保持默认设置。
- **`pool.idleTimeout`**：连接空闲超时时间，默认永不超时。建议保持默认设置。
- **`pool.waitTimeout`**：从连接池获取连接的超时时间，默认设置为 60 秒。如果在超时时间内未能获取连接，将返回 HTTP 状态码 503。该参数从版本 3.3.3.0 开始提供。
- **`pool.maxWait`**：连接池中等待获取连接的请求数上限，默认值为 0，表示不限制。当排队请求数超过此值时，新的请求将返回 HTTP 状态码 503。该参数从版本 3.3.3.0 开始提供。

### HTTP 返回码配置

taosAdapter 通过参数 `httpCodeServerError` 来控制当底层 C 接口返回错误时，是否在 RESTful 接口请求中返回非 200 的 HTTP 状态码。当设置为 `true` 时，taosAdapter 会根据 C 接口返回的错误码映射为相应的 HTTP 状态码。具体映射规则请参考 [HTTP 响应码](../../connector/rest-api/#http-响应码)。

该配置只会影响 **RESTful 接口**。

###### 参数说明

- **`httpCodeServerError`**：
  - **设置为 `true` 时**：根据 C 接口返回的错误码映射为相应的 HTTP 状态码。
  - **设置为 `false` 时**：无论 C 接口返回什么错误，始终返回 HTTP 状态码 `200`（默认值）。

### 内存限制配置

taosAdapter 将监测自身运行过程中内存使用率并通过两个阈值进行调节。有效值范围为 1 到 100 的整数，单位为系统物理内存的百分比。

该配置只会影响以下接口：

- RESTful 接口请求
- InfluxDB v1 写接口
- OpenTSDB HTTP 写入接口
- Prometheus remote_read 和 remote_write 接口

###### 参数说明

- **`pauseQueryMemoryThreshold`**：
  - 当内存使用超过此阈值时，taosAdapter 将停止处理查询请求。
  - 默认值：`70`（即 70% 的系统物理内存）。
- **`pauseAllMemoryThreshold`**：
  - 当内存使用超过此阈值时，taosAdapter 将停止处理所有请求（包括写入和查询）。
  - 默认值：`80`（即 80% 的系统物理内存）。

当内存使用回落到阈值以下时，taosAdapter 会自动恢复相应功能。

##### HTTP 返回内容

- **超过 `pauseQueryMemoryThreshold` 时**：
  - HTTP 状态码：`503`
  - 返回内容：`"query memory exceeds threshold"`
- **超过 `pauseAllMemoryThreshold` 时**：
  - HTTP 状态码：`503`
  - 返回内容：`"memory exceeds threshold"`

##### 状态检查接口

可以通过以下接口检查 taosAdapter 的内存状态：

- **正常状态**：`http://<fqdn>:6041/-/ping` 返回 `code 200`。
- **内存超过阈值**：
  - 如果内存超过 `pauseAllMemoryThreshold`，返回 `code 503`。
  - 如果内存超过 `pauseQueryMemoryThreshold`，且请求参数包含 `action=query`，返回 `code 503`。

##### 相关配置参数

- **`monitor.collectDuration`**：内存监控间隔，默认值为 `3s`，环境变量为 `TAOS_MONITOR_COLLECT_DURATION`。
- **`monitor.incgroup`**：是否在容器中运行（容器中运行设置为 `true`），默认值为 `false`，环境变量为 `TAOS_MONITOR_INCGROUP`。
- **`monitor.pauseQueryMemoryThreshold`**：查询请求暂停的内存阈值（百分比），默认值为 `70`，环境变量为 `TAOS_MONITOR_PAUSE_QUERY_MEMORY_THRESHOLD`。
- **`monitor.pauseAllMemoryThreshold`**：查询和写入请求暂停的内存阈值（百分比），默认值为 `80`，环境变量为 `TAOS_MONITOR_PAUSE_ALL_MEMORY_THRESHOLD`。

您可以根据具体项目应用场景和运营策略进行相应调整，并建议使用运营监控软件及时进行系统内存状态监控。负载均衡器也可以通过这个接口检查 taosAdapter 运行状态。

### 无模式写入创建 DB 配置

从 **3.0.4.0 版本** 开始，taosAdapter 提供了参数 `smlAutoCreateDB`，用于控制在 schemaless 协议写入时是否自动创建数据库（DB）。

`smlAutoCreateDB` 参数只会影响以下接口：

- InfluxDB v1 写接口
- OpenTSDB JSON 和 telnet 格式写入
- Telegraf 数据写入
- collectd 数据写入
- StatsD 数据写入
- node_exporter 数据写入
- OpenMetrics 数据写入

###### 参数说明

- **`smlAutoCreateDB`**：
  - **设置为 `true` 时**：在 schemaless 协议写入时，如果目标数据库不存在，taosAdapter 会自动创建该数据库。
  - **设置为 `false` 时**：用户需要手动创建数据库，否则写入会失败（默认值）。

### 结果返回条数配置

taosAdapter 提供了参数 `restfulRowLimit`，用于控制 HTTP 接口返回的结果条数。

`restfulRowLimit` 参数只会影响以下接口的返回结果：

- RESTful 接口
- Prometheus remote_read 接口

###### 参数说明

- **`restfulRowLimit`**：
  - **设置为正整数时**：接口返回的结果条数将不超过该值。
  - **设置为 `-1` 时**：接口返回的结果条数无限制（默认值）。

### 日志配置

配置参数如下：

- **`log.path`**

  指定日志存储路径（默认值：`"/var/log/taos"`）。

- **`log.level`**

  设置日志级别（默认值：`"info"`）。

- **`log.keepDays`**

  日志保留天数（正整数，默认值：`30`）。

- **`log.rotationCount`**

  日志文件轮转数量（默认值：`30`）。

- **`log.rotationSize`**

  单个日志文件最大大小（支持 KB/MB/GB 单位，默认值：`"1GB"`）。

- **`log.compress`**

  是否压缩旧日志文件（默认值：`false`）。

- **`log.rotationTime`**

  日志轮转时间（已弃用，固定 24 小时轮转）。

- **`log.reservedDiskSize`**

  为日志目录保留的磁盘空间（支持 KB/MB/GB 单位，默认值：`"1GB"`）。

- **`log.enableSqlToCsvLogging`**

  是否启用记录 SQL 到 CSV 文件（默认值：`false`）具体内容见 [记录 SQL 到 csv 文件](#记录-sql-到-csv-文件)。

- **`log.enableRecordHttpSql`**

  **不建议继续使用此参数，推荐使用[记录 SQL 到 csv 文件](#记录-sql-到-csv-文件)作为替代方案**

  是否记录 HTTP SQL 请求（默认值：`false`）。

- **`log.sqlRotationCount`**

  **不建议继续使用此参数，推荐使用[记录 SQL 到 csv 文件](#记录-sql-到-csv-文件)作为替代方案**

  SQL 日志轮转数量（默认值：`2`）。

- **`log.sqlRotationSize`**

  **不建议继续使用此参数，推荐使用[记录 SQL 到 csv 文件](#记录-sql-到-csv-文件)作为替代方案**

  单个 SQL 日志文件最大大小（支持 KB/MB/GB 单位，默认值：`"1GB"`）。

- **`log.sqlRotationTime`**

  **不建议继续使用此参数，推荐使用[记录 SQL 到 csv 文件](#记录-sql-到-csv-文件)作为替代方案**

  SQL 日志轮转时间（默认值：`24h`）。

可以通过设置 --log.level 参数或者环境变量 TAOS_ADAPTER_LOG_LEVEL 来设置 taosAdapter 日志输出详细程度。有效值包括：panic、fatal、error、warn、warning、info、debug 以及 trace。

### 第三方数据源配置

#### Collectd 配置参数

- **`collectd.enable`**

  启用或禁用 collectd 协议支持（默认值：`false`）。

- **`collectd.port`**

  指定 collectd 服务监听端口（默认值：`6045`）。

- **`collectd.db`**

  设置 collectd 数据写入的目标数据库名称（默认值：`"collectd"`）。

- **`collectd.user`**

  配置连接数据库使用的用户名（默认值：`"root"`）。

- **`collectd.password`**

  设置连接数据库使用的密码（默认值：`"taosdata"`）。

- **`collectd.ttl`**

  定义 collectd 数据的生存时间（默认值：`0`，表示无超时）。

- **`collectd.worker`**

  配置 collectd 写入工作线程数量（默认值：`10`）。

#### InfluxDB 配置参数

- **`influxdb.enable`**

  启用或禁用 InfluxDB 协议支持（布尔值，默认值：`true`）。

#### OpenTSDB 配置参数

- **`opentsdb.enable`**

  是否启用 OpenTSDB HTTP 协议支持（默认值：`true`）。

- **`opentsdb_telnet.enable`**

  是否启用 OpenTSDB Telnet 协议支持（警告：无认证机制，默认值：`false`）。

- **`opentsdb_telnet.ports`**

  配置 OpenTSDB Telnet 监听端口（默认值：`[6046,6047,6048,6049]`）。

- **`opentsdb_telnet.dbs`**

  指定 OpenTSDB Telnet 数据写入的数据库（默认值：`["opentsdb_telnet","collectd_tsdb","icinga2_tsdb","tcollector_tsdb"]`）。

- **`opentsdb_telnet.user`**

  设置数据库连接用户名（默认值：`"root"`）。

- **`opentsdb_telnet.password`**

  设置数据库连接密码（默认值：`"taosdata"`）。

- **`opentsdb_telnet.ttl`**

  配置数据生存时间（默认值：`0`，表示无超时）。

- **`opentsdb_telnet.batchSize`**

  设置批量写入大小（默认值：`1`）。

- **`opentsdb_telnet.flushInterval`**

  配置刷新间隔时间（默认值：`0s`）。

- **`opentsdb_telnet.maxTCPConnections`**

  设置最大 TCP 连接数（默认值：`250`）。

- **`opentsdb_telnet.tcpKeepAlive`**

  是否启用 TCP KeepAlive（默认值：`false`）。

#### StatsD 配置参数

- **`statsd.enable`**

  是否启用 StatsD 协议支持（默认值：`false`）。

- **`statsd.port`**

  设置 StatsD 服务监听端口（默认值：`6044`）。

- **`statsd.protocol`**

  指定 StatsD 传输协议（可选：tcp/udp/tcp4/udp4，默认值：`"udp4"`）。

- **`statsd.db`**

  配置 StatsD 数据写入的目标数据库（默认值：`"statsd"`）。

- **`statsd.user`**

  设置数据库连接用户名（默认值：`"root"`）。

- **`statsd.password`**

  设置数据库连接密码（默认值：`"taosdata"`）。

- **`statsd.ttl`**

  配置数据生存时间（默认值：`0`，表示无超时）。

- **`statsd.gatherInterval`**

  设置数据采集间隔（默认值：`5s`）。

- **`statsd.worker`**

  配置写入工作线程数（默认值：`10`）。

- **`statsd.allowPendingMessages`**

  设置允许挂起的消息数量（默认值：`50000`）。

- **`statsd.maxTCPConnections`**

  配置最大 TCP 连接数（默认值：`250`）。

- **`statsd.tcpKeepAlive`**

  是否启用 TCP KeepAlive（默认值：`false`）。

- **`statsd.deleteCounters`**

  是否在采集后删除计数器缓存（默认值：`true`）。

- **`statsd.deleteGauges`**

  是否在采集后删除测量值缓存（默认值：`true`）。

- **`statsd.deleteSets`**

  是否在采集后删除集合缓存（默认值：`true`）。

- **`statsd.deleteTimings`**

  是否在采集后删除计时器缓存（默认值：`true`）。

#### Prometheus 配置参数

- **`prometheus.enable`**

  是否启用 Prometheus 协议支持（默认值：`true`）。

#### OpenMetrics 配置参数

- **`open_metrics.enable`**

  启用或禁用 OpenMetrics 数据采集功能（默认值：`false`）。

- **`open_metrics.user`**

  配置连接 TDengine TSDB 的用户名（默认值：`"root"`）。

- **`open_metrics.password`**

  设置连接 TDengine TSDB 的密码（默认值：`"taosdata"`）。

- **`open_metrics.urls`**

  指定 OpenMetrics 数据采集地址列表（默认值：`["http://localhost:9100"]`，未指定路由时会自动追加 `/metrics`）。

- **`open_metrics.dbs`**

  设置数据写入的目标数据库列表（默认值：`["open_metrics"]`，需与采集地址数量相同）。

- **`open_metrics.responseTimeoutSeconds`**

  配置采集超时时间（秒）（默认值：`[5]`，需与采集地址数量相同）。

- **`open_metrics.httpUsernames`**

  设置 Basic 认证用户名列表（若启用需与采集地址数量相同，默认值：空）。

- **`open_metrics.httpPasswords`**

  设置 Basic 认证密码列表（若启用需与采集地址数量相同，默认值：空）。

- **`open_metrics.httpBearerTokenStrings`**

  配置 Bearer Token 认证列表（若启用需与采集地址数量相同，默认值：空）。

- **`open_metrics.caCertFiles`**

  指定根证书文件路径列表（若启用需与采集地址数量相同，默认值：空）。

- **`open_metrics.certFiles`**

  设置客户端证书文件路径列表（若启用需与采集地址数量相同，默认值：空）。

- **`open_metrics.keyFiles`**

  配置客户端证书密钥文件路径列表（若启用需与采集地址数量相同，默认值：空）。

- **`open_metrics.insecureSkipVerify`**

  是否跳过 HTTPS 证书验证（默认值：`true`）。

- **`open_metrics.gatherDurationSeconds`**

  设置采集间隔时间（秒）（默认值：`[5]`，需与采集地址数量相同）。

- **`open_metrics.ttl`**

  定义数据表的生存时间（秒）（`0` 表示无超时，若启用需与采集地址数量相同，默认值：空）。

- **`open_metrics.ignoreTimestamp`**

  是否忽略采集数据中的时间戳（若忽略则使用采集时刻时间戳，默认值：`false`）。

#### node_exporter 配置参数

- **`node_exporter.enable`**

  是否启用 node_exporter 数据采集（默认值：`false`）。

- **`node_exporter.db`**

  指定 node_exporter 数据写入的数据库名称（默认值：`"node_exporter"`）。

- **`node_exporter.urls`**

  配置 node_exporter 服务地址（默认值：`["http://localhost:9100"]`）。

- **`node_exporter.gatherDuration`**

  设置数据采集间隔时间（默认值：`5s`）。

- **`node_exporter.responseTimeout`**

  配置请求超时时间（默认值：`5s`）。

- **`node_exporter.user`**

  设置数据库连接用户名（默认值：`"root"`）。

- **`node_exporter.password`**

  设置数据库连接密码（默认值：`"taosdata"`）。

- **`node_exporter.ttl`**

  配置采集数据的生存时间（默认值：`0`，表示无超时）。

- **`node_exporter.httpUsername`**

  配置 HTTP 基本认证用户名（可选）。

- **`node_exporter.httpPassword`**

  配置 HTTP 基本认证密码（可选）。

- **`node_exporter.httpBearerTokenString`**

  配置 HTTP Bearer Token 认证（可选）。

- **`node_exporter.insecureSkipVerify`**

  是否跳过 SSL 证书验证（默认值：`true`）。

- **`node_exporter.certFile`**

  指定客户端证书文件路径（可选）。

- **`node_exporter.keyFile`**

  指定客户端证书密钥文件路径（可选）。

- **`node_exporter.caCertFile`**

  指定 CA 证书文件路径（可选）。

### 上报指标配置

taosAdapter 将指标上报到 taosKeeper 进行统一管理，参数如下：

- **`uploadKeeper.enable`**

  是否启用向 taoKeeper 上报监控指标（默认值：`true`）。

- **`uploadKeeper.url`**

  配置 taosKeeper 服务地址（默认值：`http://127.0.0.1:6043/adapter_report` ）。

- **`uploadKeeper.interval`**

  设置上报间隔时间（默认值：`15s`）。

- **`uploadKeeper.timeout`**

  配置上报超时时间（默认值：`5s`）。

- **`uploadKeeper.retryTimes`**

  设置失败重试次数（默认值：`3`）。

- **`uploadKeeper.retryInterval`**

  配置重试间隔时间（默认值：`5s`）。

### 查询请求并发限制配置

从 **3.3.6.29**/**3.3.8.3** 版本开始，taosAdapter 支持配置查询请求的并发限制，防止因过多的并发查询请求导致系统资源耗尽。
当启用该功能后，taosAdapter 会根据配置的并发限制数来控制同时处理的查询请求数量，超过限制的请求将进入等待，直到有可用的处理资源为止。

当等待时间超过配置的超时时间或等待数超过配置的最大等待请求数时，taosAdapter 将直接返回错误响应，提示用户请求过多。
RESTful 请求将返回 HTTP 状态码 `503`，WebSocket 请求将返回错误码 `0xFFFE`。

该配置会影响以下接口：

- **RESTful 接口**
- **WebSocket SQL 执行接口**

###### 参数说明

- **`request.queryLimitEnable`**
  - **设置为 `true` 时**：启用查询请求并发限制功能。
  - **设置为 `false` 时**：禁用查询请求并发限制功能（默认值）。
- **`request.default.queryLimit`**
  - 设置默认的查询请求并发限制数（默认值：`0`，表示无限制）。
- **`request.default.queryWaitTimeout`**
  - 限制并发请求超过限制后的等待时间（单位：秒），请求等待执行超时后将直接返回错误，默认值：`900`。
- **`request.default.queryMaxWait`**
  - 限制并发请求超过限制后的最大等待请求数，超过该数量的请求将直接返回错误，默认值：`0`，表示不限制。
- **`request.excludeQueryLimitSql`**
  - 配置不受并发限制影响的 SQL 列表，必须以 `select` 开头，SQL 忽略大小写。
- **`request.excludeQueryLimitSqlRegex`**
  - 配置不受并发限制影响的 SQL 正则表达式列表。

###### 针对每个用户可单独设置

仅支持配置文件进行设置：

- **`request.users.<username>.queryLimit`**
  - 设置指定用户的查询请求并发限制数，优先级高于默认设置。
- **`request.users.<username>.queryWaitTimeout`**
  - 限制并发请求超过限制后的等待时间（单位：秒），请求等待执行超时后将直接返回错误，优先级高于默认设置。
- **`request.users.<username>.queryMaxWait`**
  - 限制并发请求超过限制后的最大等待请求数，超过该数量的请求将直接返回错误，优先级高于默认设置。

###### 示例说明

```toml
[request]
queryLimitEnable = true
excludeQueryLimitSql = ["select 1","select server_version()"]
excludeQueryLimitSqlRegex = ['(?i)^select\s+.*from\s+information_schema.*']

[request.default]
queryLimit = 200
queryWaitTimeout = 900
queryMaxWait = 0

[request.users.root]
queryLimit = 100
queryWaitTimeout = 200
queryMaxWait = 10
```

- `queryLimitEnable = true` 启用了查询请求并发限制功能。
- `excludeQueryLimitSql = ["select 1","select server_version()"]` 排除了两个常用做 ping 的查询 SQL。
- `excludeQueryLimitSqlRegex = ['(?i)^select\s+.*from\s+information_schema.*']` 排除了所有查询 information_schema 这个数据库的 SQL。
- `request.default` 配置了默认的查询请求并发限制数为 200，等待超时为 900 秒，最大等待请求数为 0（无限制）。
- `request.users.root` 配置了用户 root 的查询请求并发限制数为 100，等待超时为 200 秒，最大等待请求数为 10。

当用户 root 发起查询请求时，taosAdapter 会根据上述配置进行并发限制处理。当查询请求超过 100 个时，后续请求将进入等待，直到有可用的资源为止。如果等待时间超过 200 秒或等待请求数超过 10 个，taosAdapter 将直接返回错误响应。

当其他用户发起查询请求时，将使用默认的并发限制配置进行处理。每个用户配置独立，不共享 `request.default` 的并发限制。比如用户 user1 发起 200 个并发查询请求时，用户 user2 也可以同时发起 200 个并发查询请求而不会阻塞。

### 禁用查询 SQL 配置

从 **3.3.6.34**/**3.4.0.0** 版本开始，taosAdapter 支持通过配置禁用特定的查询 SQL，防止执行某些不安全或资源消耗过大的查询操作。
当启用该功能后，taosAdapter 会检查每个非 insert（忽略大小写）开头的 SQL，如果匹配到配置的禁用正则表达式，则直接返回错误响应，提示用户该查询被禁用。
当被禁用的查询 SQL 被匹配到时，RESTful 接口将返回 HTTP 状态码 `403`，WebSocket 接口将返回错误码 `0xFFFD`，
taosAdapter 将打印警告日志，包含此 SQL 的来源等内容，例如：

```text
reject sql, client_ip:192.168.1.98, port:59912, user:root, app:test_app, reject_regex:(?i)^drop\s+table\s+.*, sql:DROP taBle testdb.stb
```

该配置会影响以下接口：

- **RESTful 接口**
- **WebSocket SQL 执行接口**

###### 参数说明

- **`rejectQuerySqlRegex`**
  - 配置禁用查询 SQL 的正则表达式列表。支持 [Goole RE2 语法](https://github.com/google/re2/wiki/Syntax)。
  - 默认值：空列表，表示不禁用任何查询 SQL。

###### 示例说明

```toml
rejectQuerySqlRegex = ['(?i)^drop\s+database\s+.*','(?i)^drop\s+table\s+.*','(?i)^alter\s+table\s+.*']
```

`rejectQuerySqlRegex = ['(?i)^drop\s+database\s+.*','(?i)^drop\s+table\s+.*','(?i)^alter\s+table\s+.*']` 禁用了所有 drop database、drop table 和 alter table 的查询操作，忽略大小写。

### 环境变量

配置项与环境变量对应如下表：

<details>
<summary>详细信息</summary>

| 参数                                    | 环境变量                                                  |
|:--------------------------------------|:------------------------------------------------------|
| `collectd.db`                         | `TAOS_ADAPTER_COLLECTD_DB`                            |
| `collectd.enable`                     | `TAOS_ADAPTER_COLLECTD_ENABLE`                        |
| `collectd.password`                   | `TAOS_ADAPTER_COLLECTD_PASSWORD`                      |
| `collectd.port`                       | `TAOS_ADAPTER_COLLECTD_PORT`                          |
| `collectd.ttl`                        | `TAOS_ADAPTER_COLLECTD_TTL`                           |
| `collectd.user`                       | `TAOS_ADAPTER_COLLECTD_USER`                          |
| `collectd.worker`                     | `TAOS_ADAPTER_COLLECTD_WORKER`                        |
| `cors.allowAllOrigins`                | `TAOS_ADAPTER_CORS_ALLOW_ALL_ORIGINS`                 |
| `cors.allowCredentials`               | `TAOS_ADAPTER_CORS_ALLOW_Credentials`                 |
| `cors.allowHeaders`                   | `TAOS_ADAPTER_ALLOW_HEADERS`                          |
| `cors.allowOrigins`                   | `TAOS_ADAPTER_ALLOW_ORIGINS`                          |
| `cors.allowWebSockets`                | `TAOS_ADAPTER_CORS_ALLOW_WebSockets`                  |
| `cors.exposeHeaders`                  | `TAOS_ADAPTER_Expose_Headers`                         |
| `debug`                               | `TAOS_ADAPTER_DEBUG`                                  |
| `httpCodeServerError`                 | `TAOS_ADAPTER_HTTP_CODE_SERVER_ERROR`                 |
| `influxdb.enable`                     | `TAOS_ADAPTER_INFLUXDB_ENABLE`                        |
| `instanceId`                          | `TAOS_ADAPTER_INSTANCE_ID`                            |
| `log.compress`                        | `TAOS_ADAPTER_LOG_COMPRESS`                           |
| `log.enableRecordHttpSql`             | `TAOS_ADAPTER_LOG_ENABLE_RECORD_HTTP_SQL`             |
| `log.enableSqlToCsvLogging`           | `TAOS_ADAPTER_LOG_ENABLE_SQL_TO_CSV_LOGGING`          |
| `log.keepDays`                        | `TAOS_ADAPTER_LOG_KEEP_DAYS`                          |
| `log.level`                           | `TAOS_ADAPTER_LOG_LEVEL`                              |
| `log.path`                            | `TAOS_ADAPTER_LOG_PATH`                               |
| `log.reservedDiskSize`                | `TAOS_ADAPTER_LOG_RESERVED_DISK_SIZE`                 |
| `log.rotationCount`                   | `TAOS_ADAPTER_LOG_ROTATION_COUNT`                     |
| `log.rotationSize`                    | `TAOS_ADAPTER_LOG_ROTATION_SIZE`                      |
| `log.rotationTime`                    | `TAOS_ADAPTER_LOG_ROTATION_TIME`                      |
| `log.sqlRotationCount`                | `TAOS_ADAPTER_LOG_SQL_ROTATION_COUNT`                 |
| `log.sqlRotationSize`                 | `TAOS_ADAPTER_LOG_SQL_ROTATION_SIZE`                  |
| `log.sqlRotationTime`                 | `TAOS_ADAPTER_LOG_SQL_ROTATION_TIME`                  |
| `logLevel`                            | `TAOS_ADAPTER_LOG_LEVEL`                              |
| `maxAsyncConcurrentLimit`             | `TAOS_ADAPTER_MAX_ASYNC_CONCURRENT_LIMIT`             |
| `maxSyncConcurrentLimit`              | `TAOS_ADAPTER_MAX_SYNC_CONCURRENT_LIMIT`              |
| `monitor.collectDuration`             | `TAOS_ADAPTER_MONITOR_COLLECT_DURATION`               |
| `monitor.disable`                     | `TAOS_ADAPTER_MONITOR_DISABLE`                        |
| `monitor.identity`                    | `TAOS_ADAPTER_MONITOR_IDENTITY`                       |
| `monitor.incgroup`                    | `TAOS_ADAPTER_MONITOR_INCGROUP`                       |
| `monitor.pauseAllMemoryThreshold`     | `TAOS_ADAPTER_MONITOR_PAUSE_ALL_MEMORY_THRESHOLD`     |
| `monitor.pauseQueryMemoryThreshold`   | `TAOS_ADAPTER_MONITOR_PAUSE_QUERY_MEMORY_THRESHOLD`   |
| `node_exporter.caCertFile`            | `TAOS_ADAPTER_NODE_EXPORTER_CA_CERT_FILE`             |
| `node_exporter.certFile`              | `TAOS_ADAPTER_NODE_EXPORTER_CERT_FILE`                |
| `node_exporter.db`                    | `TAOS_ADAPTER_NODE_EXPORTER_DB`                       |
| `node_exporter.enable`                | `TAOS_ADAPTER_NODE_EXPORTER_ENABLE`                   |
| `node_exporter.gatherDuration`        | `TAOS_ADAPTER_NODE_EXPORTER_GATHER_DURATION`          |
| `node_exporter.httpBearerTokenString` | `TAOS_ADAPTER_NODE_EXPORTER_HTTP_BEARER_TOKEN_STRING` |
| `node_exporter.httpPassword`          | `TAOS_ADAPTER_NODE_EXPORTER_HTTP_PASSWORD`            |
| `node_exporter.httpUsername`          | `TAOS_ADAPTER_NODE_EXPORTER_HTTP_USERNAME`            |
| `node_exporter.insecureSkipVerify`    | `TAOS_ADAPTER_NODE_EXPORTER_INSECURE_SKIP_VERIFY`     |
| `node_exporter.keyFile`               | `TAOS_ADAPTER_NODE_EXPORTER_KEY_FILE`                 |
| `node_exporter.password`              | `TAOS_ADAPTER_NODE_EXPORTER_PASSWORD`                 |
| `node_exporter.responseTimeout`       | `TAOS_ADAPTER_NODE_EXPORTER_RESPONSE_TIMEOUT`         |
| `node_exporter.ttl`                   | `TAOS_ADAPTER_NODE_EXPORTER_TTL`                      |
| `node_exporter.urls`                  | `TAOS_ADAPTER_NODE_EXPORTER_URLS`                     |
| `node_exporter.user`                  | `TAOS_ADAPTER_NODE_EXPORTER_USER`                     |
| `open_metrics.enable`                 | `TAOS_ADAPTER_OPEN_METRICS_ENABLE`                    |
| `open_metrics.user`                   | `TAOS_ADAPTER_OPEN_METRICS_USER`                      |
| `open_metrics.password`               | `TAOS_ADAPTER_OPEN_METRICS_PASSWORD`                  |
| `open_metrics.urls`                   | `TAOS_ADAPTER_OPEN_METRICS_URLS`                      |
| `open_metrics.dbs`                    | `TAOS_ADAPTER_OPEN_METRICS_DBS`                       |
| `open_metrics.responseTimeoutSeconds` | `TAOS_ADAPTER_OPEN_METRICS_RESPONSE_TIMEOUT_SECONDS`  |
| `open_metrics.httpUsernames`          | `TAOS_ADAPTER_OPEN_METRICS_HTTP_USERNAMES`            |
| `open_metrics.httpPasswords`          | `TAOS_ADAPTER_OPEN_METRICS_HTTP_PASSWORDS`            |
| `open_metrics.httpBearerTokenStrings` | `TAOS_ADAPTER_OPEN_METRICS_HTTP_BEARER_TOKEN_STRINGS` |
| `open_metrics.caCertFiles`            | `TAOS_ADAPTER_OPEN_METRICS_CA_CERT_FILES`             |
| `open_metrics.certFiles`              | `TAOS_ADAPTER_OPEN_METRICS_CERT_FILES`                |
| `open_metrics.keyFiles`               | `TAOS_ADAPTER_OPEN_METRICS_KEY_FILES`                 |
| `open_metrics.insecureSkipVerify`     | `TAOS_ADAPTER_OPEN_METRICS_INSECURE_SKIP_VERIFY`      |
| `open_metrics.gatherDurationSeconds`  | `TAOS_ADAPTER_OPEN_METRICS_GATHER_DURATION_SECONDS`   |
| `open_metrics.ignoreTimestamp`        | `TAOS_ADAPTER_OPEN_METRICS_IGNORE_TIMESTAMP`          |
| `open_metrics.ttl`                    | `TAOS_ADAPTER_OPEN_METRICS_TTL`                       |
| `opentsdb.enable`                     | `TAOS_ADAPTER_OPENTSDB_ENABLE`                        |
| `opentsdb_telnet.batchSize`           | `TAOS_ADAPTER_OPENTSDB_TELNET_BATCH_SIZE`             |
| `opentsdb_telnet.dbs`                 | `TAOS_ADAPTER_OPENTSDB_TELNET_DBS`                    |
| `opentsdb_telnet.enable`              | `TAOS_ADAPTER_OPENTSDB_TELNET_ENABLE`                 |
| `opentsdb_telnet.flushInterval`       | `TAOS_ADAPTER_OPENTSDB_TELNET_FLUSH_INTERVAL`         |
| `opentsdb_telnet.maxTCPConnections`   | `TAOS_ADAPTER_OPENTSDB_TELNET_MAX_TCP_CONNECTIONS`    |
| `opentsdb_telnet.password`            | `TAOS_ADAPTER_OPENTSDB_TELNET_PASSWORD`               |
| `opentsdb_telnet.ports`               | `TAOS_ADAPTER_OPENTSDB_TELNET_PORTS`                  |
| `opentsdb_telnet.tcpKeepAlive`        | `TAOS_ADAPTER_OPENTSDB_TELNET_TCP_KEEP_ALIVE`         |
| `opentsdb_telnet.ttl`                 | `TAOS_ADAPTER_OPENTSDB_TELNET_TTL`                    |
| `opentsdb_telnet.user`                | `TAOS_ADAPTER_OPENTSDB_TELNET_USER`                   |
| `pool.idleTimeout`                    | `TAOS_ADAPTER_POOL_IDLE_TIMEOUT`                      |
| `pool.maxConnect`                     | `TAOS_ADAPTER_POOL_MAX_CONNECT`                       |
| `pool.maxIdle`                        | `TAOS_ADAPTER_POOL_MAX_IDLE`                          |
| `pool.maxWait`                        | `TAOS_ADAPTER_POOL_MAX_WAIT`                          |
| `pool.waitTimeout`                    | `TAOS_ADAPTER_POOL_WAIT_TIMEOUT`                      |
| `P`, `port`                           | `TAOS_ADAPTER_PORT`                                   |
| `prometheus.enable`                   | `TAOS_ADAPTER_PROMETHEUS_ENABLE`                      |
| `request.default.queryLimit`          | `TAOS_ADAPTER_REQUEST_DEFAULT_QUERY_LIMIT`            |
| `request.default.queryMaxWait`        | `TAOS_ADAPTER_REQUEST_DEFAULT_QUERY_MAX_WAIT`         |
| `request.default.queryWaitTimeout`    | `TAOS_ADAPTER_REQUEST_DEFAULT_QUERY_WAIT_TIMEOUT`     |
| `request.excludeQueryLimitSql`        | `TAOS_ADAPTER_REQUEST_EXCLUDE_QUERY_LIMIT_SQL`        |
| `request.excludeQueryLimitSqlRegex`   | `TAOS_ADAPTER_REQUEST_EXCLUDE_QUERY_LIMIT_SQL_REGEX`  |
| `request.queryLimitEnable`            | `TAOS_ADAPTER_REQUEST_QUERY_LIMIT_ENABLE`             |
| `restfulRowLimit`                     | `TAOS_ADAPTER_RESTFUL_ROW_LIMIT`                      |
| `smlAutoCreateDB`                     | `TAOS_ADAPTER_SML_AUTO_CREATE_DB`                     |
| `statsd.allowPendingMessages`         | `TAOS_ADAPTER_STATSD_ALLOW_PENDING_MESSAGES`          |
| `statsd.db`                           | `TAOS_ADAPTER_STATSD_DB`                              |
| `statsd.deleteCounters`               | `TAOS_ADAPTER_STATSD_DELETE_COUNTERS`                 |
| `statsd.deleteGauges`                 | `TAOS_ADAPTER_STATSD_DELETE_GAUGES`                   |
| `statsd.deleteSets`                   | `TAOS_ADAPTER_STATSD_DELETE_SETS`                     |
| `statsd.deleteTimings`                | `TAOS_ADAPTER_STATSD_DELETE_TIMINGS`                  |
| `statsd.enable`                       | `TAOS_ADAPTER_STATSD_ENABLE`                          |
| `statsd.gatherInterval`               | `TAOS_ADAPTER_STATSD_GATHER_INTERVAL`                 |
| `statsd.maxTCPConnections`            | `TAOS_ADAPTER_STATSD_MAX_TCP_CONNECTIONS`             |
| `statsd.password`                     | `TAOS_ADAPTER_STATSD_PASSWORD`                        |
| `statsd.port`                         | `TAOS_ADAPTER_STATSD_PORT`                            |
| `statsd.protocol`                     | `TAOS_ADAPTER_STATSD_PROTOCOL`                        |
| `statsd.tcpKeepAlive`                 | `TAOS_ADAPTER_STATSD_TCP_KEEP_ALIVE`                  |
| `statsd.ttl`                          | `TAOS_ADAPTER_STATSD_TTL`                             |
| `statsd.user`                         | `TAOS_ADAPTER_STATSD_USER`                            |
| `statsd.worker`                       | `TAOS_ADAPTER_STATSD_WORKER`                          |
| `taosConfigDir`                       | `TAOS_ADAPTER_TAOS_CONFIG_FILE`                       |
| `uploadKeeper.enable`                 | `TAOS_ADAPTER_UPLOAD_KEEPER_ENABLE`                   |
| `uploadKeeper.interval`               | `TAOS_ADAPTER_UPLOAD_KEEPER_INTERVAL`                 |
| `uploadKeeper.retryInterval`          | `TAOS_ADAPTER_UPLOAD_KEEPER_RETRY_INTERVAL`           |
| `uploadKeeper.retryTimes`             | `TAOS_ADAPTER_UPLOAD_KEEPER_RETRY_TIMES`              |
| `uploadKeeper.timeout`                | `TAOS_ADAPTER_UPLOAD_KEEPER_TIMEOUT`                  |
| `uploadKeeper.url`                    | `TAOS_ADAPTER_UPLOAD_KEEPER_URL`                      |

</details>

## 服务管理

### 启动/停止 taosAdapter

在 Linux 系统上 taosAdapter 服务默认由 systemd 管理。使用命令 `systemctl start taosadapter` 可以启动 taosAdapter 服务。使用命令 `systemctl stop taosadapter` 可以停止 taosAdapter 服务。使用命令 `systemctl status taosadapter` 来检查 taosAdapter 运行状态。

### 升级 taosAdapter

taosAdapter 和 TDengine TSDB server 需要使用相同版本。请通过升级 TDengine TSDB server 来升级 taosAdapter。
与 taosd 分离部署的 taosAdapter 必须通过升级其所在服务器的 TDengine TSDB server 才能得到升级。

### 移除 taosAdapter

使用命令 rmtaos 可以移除包括 taosAdapter 在内的 TDengine TSDB server 软件。

## 动态配置

### 动态修改日志级别

从 **3.3.5.0 版本** 开始，taosAdapter 支持通过 HTTP 接口动态修改日志级别。用户可以通过发送 HTTP PUT 请求到 /config 接口，动态调整日志级别。该接口的验证方式与 /rest/sql 接口相同，请求体中需传入 JSON 格式的配置项键值对。

以下是通过 curl 命令将日志级别设置为 debug 的示例：

```shell
curl --location --request PUT 'http://127.0.0.1:6041/config' \
-u root:taosdata \
--data '{"log.level": "debug"}'
```

### 监听配置文件变更

从 **3.3.6.34**/**3.4.0.0** 版本开始，taosAdapter 支持监听配置文件的变更，支持以下配置自动更新

- `log.level` 日志级别参数
- `rejectQuerySqlRegex` 拒绝查询 SQL 列表配置参数

## IPv6 支持

taosAdapter 自 **3.3.6.13** 版本起支持 IPv6，用户无需进行任何额外配置。
taosAdapter 将自动检测系统的 IPv6 支持情况，并在系统支持时自动启用 IPv6，且同时监听 IPv4 和 IPv6 地址。

## 记录 SQL 到 CSV 文件

taosAdapter 支持将 SQL 请求记录到 CSV 文件中。用户可以通过配置参数 `log.enableSqlToCsvLogging` 来启用此功能，或使用 HTTP 请求动态开启和关闭。

### 配置参数

1. 新增配置项 `log.enableSqlToCsvLogging` 布尔值，默认为 false，表示是否开启 sql 记录到 csv 文件。设置为 true 将开启 sql 记录到 csv 文件任务，开始记录时间为启动时间，结束时间为 `2300-01-01 00:00:00`。
2. 文件命名与日志相同规则：`taosadapterSql_{instanceId}_{yyyyMMdd}.csv[.index]`
   - `instanceId`：taosAdapter 实例 ID，可通过 `instanceId` 参数设置。
   - `yyyyMMdd`：日期，格式为年月日。
   - `index`：如果存在多个文件，则会在文件名后添加数字后缀。
3. 保留空间、文件切割、保存路径等使用 log 已存在参数：
   - `log.path`：保存路径。
   - `log.keepDays` ：保留天数。
   - `log.rotationCount`：最多保留份数。
   - `log.rotationSize`：单个文件最大大小。
   - `log.compress`：是否启用压缩。
   - `log.reservedDiskSize`：保留硬盘空间大小。

### 动态开启

通过发送 HTTP POST 请求到 `/record_sql` 接口来动态开启记录，使用与 `/rest/sql` 相同的鉴权方式，样例如下：

```bash
curl --location --request POST 'http://127.0.0.1:6041/record_sql' \
-u root:taosdata \
--data '{"start_time":"2025-07-15 17:00:00","end_time":"2025-07-15 18:00:00","location":"Asia/Shanghai"}'
```

支持的参数项如下：

- start_time：[可选参数] 开始采集的时间，格式为 `yyyy-MM-dd HH:mm:ss`，如果不设置则使用当前时间。
- end_time：[可选参数] 结束采集的时间，格式为 `yyyy-MM-dd HH:mm:ss`，如果不设置则使用 `2300-01-01 00:00:00`。
- location：[可选参数] 解析采集开始和结束时间使用的时区信息，如果不设置则使用 taosAdapter 所在服务器时区。时区使用 IANA 格式，例如：`Asia/Shanghai`。

如果所有参数都使用默认值则可以不传 data，样例如下：

```bash
curl --location --request POST 'http://127.0.0.1:6041/record_sql' \
-u root:taosdata
```

成功返回 HTTP code 200，返回结构如下

```json
{"code":0,"desc":""}
```

失败返回 HTTP code 非 200，返回 json 结构如下，code 非 0，desc 描述错误内容

```json
{"code":65535,"desc":"unmarshal json error"}
```

### 动态关闭

通过发送 HTTP DELETE 请求到 `/record_sql` 接口来关闭，使用与 `/rest/sql` 相同的鉴权方式，样例如下：

```bash
curl --location --request DELETE 'http://127.0.0.1:6041/record_sql' \
-u root:taosdata
```

成功返回 HTTP code 200

1. 任务存在时返回如下

```json
{
        "code": 0,
        "message": "",
        "start_time": "2025-07-23 17:00:00",
        "end_time": "2025-07-23 18:00:00"
}
```

- start_time 为取消任务配置的启动时间，时区为 taosAdapter 所在服务器时区。
- end_time 为取消任务配置的结束时间，时区为 taosAdapter 所在服务器时区。

2. 任务不存在时返回如下

```json
{
        "code": 0,
        "message": ""
}
```

### 查询状态

通过发送 HTTP GET 请求到 `/record_sql` 接口来查询任务，使用与 `/rest/sql` 相同的鉴权方式，样例如下：

```bash
curl --location 'http://127.0.0.1:6041/record_sql' \
-u root:taosdata
```

成功返回 HTTP code 200，返回样例如下

```json
{
        "code": 0,
        "desc": "",
        "exists": true,
        "running": true,
        "start_time": "2025-07-16 17:00:00",
        "end_time": "2025-07-16 18:00:00",
        "current_concurrent": 100
}
```

- code：错误码，0 为成功。
- desc：错误信息，成功为空字符串。
- exists：任务是否存在。
- running：任务是否在运行期。
- start_time：开始时间，时区为 taosAdapter 所在服务器时区。
- end_time：结束时间，时区为 taosAdapter 所在服务器时区。
- current_concurrent：当前 SQL 记录并发度。

### 记录格式

在 `taos_free_result` 执行之前和任务结束（到达结束时间或主动关闭）时写入记录。
记录以 CSV 格式存储，无表头，每行记录包含以下字段：

1. TS：打印日志时间，格式为 `yyyy-MM-dd HH:mm:ss.SSSSSS`，时区为 taosAdapter 所在服务器时区。
2. SQL：执行的 SQL，按照 CSV 标准不处理 SQL 中的换行符，当存在特殊字符（\n、\r、"）时使用双引号包裹，包含特殊字符时无法直接复制 SQL 使用，例如：

  原始 sql 为：

  ```sql
   select * from t1
   where c1 = "ab"
   ```

   csv 文件中的记录为：

   ```csv
   "select * from t1
   where c1 = ""ab"""
   ```

3. IP：客户端 IP。
4. User：执行此 SQL 的用户名。
5. ConnType：连接类型（HTTP、WS）。
6. QID：请求 ID，保存为 16 进制。
7. ReceiveTime：接收到 SQL 的时间，格式为 `yyyy-MM-dd HH:mm:ss.SSSSSS`，时区为 taosAdapter 所在服务器时区。
8. FreeTime：SQL 释放的时间，格式为 `yyyy-MM-dd HH:mm:ss.SSSSSS`，时区为 taosAdapter 所在服务器时区。
9. QueryDuration(us)：执行 `taos_query_a` 到回调完成时间消耗，单位微秒。
10. FetchDuration(us)：多次执行 `taos_fetch_raw_block_a` 到回调完成的时间消耗累加值，单位微秒。
11. GetConnDuration(us)：HTTP 请求从连接池获取连接的时间消耗，单位微秒。
12. TotalDuration(us)：SQL 请求完成总时间，单位微秒，当 SQL 正常完成时为（SQL 释放的时间 - 接收到 SQL 的时间），当任务结束未完成时为（当前时间 - 接收到 SQL 的时间）。
13. SourcePort：客户端端口。（3.3.6.26 及以上 / 3.3.8.0 及以上版本添加）
14. AppName：客户应用 AppName。（3.3.6.26 及以上 / 3.3.8.0 及以上版本添加）

样例如下：

```csv
2025-07-23 17:10:08.724775,show databases,127.0.0.1,root,http,0x2000000000000008,2025-07-23 17:10:08.707741,2025-07-23 17:10:08.724775,14191,965,1706,17034,53600,jdbc_test_app
```

## taosAdapter 监控指标

taosAdapter 目前仅采集 RESTful/WebSocket 相关请求的监控指标，其他接口暂无监控指标。

taosAdapter 将监控指标上报给 taosKeeper，这些监控指标会被 taosKeeper 写入监控数据库，默认是 `log` 库，可以在 taosKeeper 配置文件中修改。以下是这些监控指标的详细介绍。

`adapter_requests` 表记录 taosAdapter 监控数据：

<details>
<summary>详细信息</summary>

| field              | type             | is\_tag | comment                     |
|:-------------------|:-----------------|:--------|:----------------------------|
| ts                 | TIMESTAMP        |         | 数据采集时间戳                     |
| total              | INT UNSIGNED     |         | 总请求数                        |
| query              | INT UNSIGNED     |         | 查询请求数                       |
| write              | INT UNSIGNED     |         | 写入请求数                       |
| other              | INT UNSIGNED     |         | 其他请求数                       |
| in\_process        | INT UNSIGNED     |         | 正在处理请求数                     |
| success            | INT UNSIGNED     |         | 成功请求数                       |
| fail               | INT UNSIGNED     |         | 失败请求数                       |
| query\_success     | INT UNSIGNED     |         | 查询成功请求数                     |
| query\_fail        | INT UNSIGNED     |         | 查询失败请求数                     |
| write\_success     | INT UNSIGNED     |         | 写入成功请求数                     |
| write\_fail        | INT UNSIGNED     |         | 写入失败请求数                     |
| other\_success     | INT UNSIGNED     |         | 其他成功请求数                     |
| other\_fail        | INT UNSIGNED     |         | 其他失败请求数                     |
| query\_in\_process | INT UNSIGNED     |         | 正在处理查询请求数                   |
| write\_in\_process | INT UNSIGNED     |         | 正在处理写入请求数                   |
| endpoint           | VARCHAR          | TAG     | 请求端点                        |
| req\_type          | TINYINT UNSIGNED | TAG     | 请求类型：0 为 REST，1 为 WebSocket |

</details>

`adapter_status` 表记录 taosAdapter 状态数据：

<details>
<summary>详细信息</summary>

| field                     | type      | is\_tag | comment                                             |
|:--------------------------|:----------|:--------|:----------------------------------------------------|
| _ts                       | TIMESTAMP |         | 数据采集时间戳                                             |
| go_heap_sys               | DOUBLE    |         | Go 运行时系统分配的堆内存大小（字节）                                |
| go_heap_inuse             | DOUBLE    |         | Go 运行时正在使用的堆内存大小（字节）                                |
| go_stack_sys              | DOUBLE    |         | Go 运行时系统分配的栈内存大小（字节）                                |
| go_stack_inuse            | DOUBLE    |         | Go 运行时正在使用的栈内存大小（字节）                                |
| rss                       | DOUBLE    |         | 进程实际占用的物理内存大小（字节）                                   |
| ws_query_conn             | DOUBLE    |         | `/rest/ws` 接口当前 WebSocket 连接数                       |
| ws_stmt_conn              | DOUBLE    |         | `/rest/stmt` 接口当前 WebSocket 连接数                     |
| ws_sml_conn               | DOUBLE    |         | `/rest/schemaless` 接口当前 WebSocket 连接数               |
| ws_ws_conn                | DOUBLE    |         | `/ws` 接口当前 WebSocket 连接数                            |
| ws_tmq_conn               | DOUBLE    |         | `/rest/tmq` 接口当前 WebSocket 连接数                      |
| async_c_limit             | DOUBLE    |         | C 同步接口并发限制总数                                        |
| async_c_inflight          | DOUBLE    |         | C 同步接口当前并发数                                         |
| sync_c_limit              | DOUBLE    |         | C 异步接口并发限制总数                                        |
| sync_c_inflight           | DOUBLE    |         | C 异步接口当前并发数                                         |
| ws_query_conn_inc         | DOUBLE    |         | /rest/ws 接口新增连接（3.3.6.10 及以上）                       |
| ws_query_conn_dec         | DOUBLE    |         | /rest/ws 接口减少连接（3.3.6.10 及以上）                       |
| ws_stmt_conn_inc          | DOUBLE    |         | /rest/stmt 接口新增连接（3.3.6.10 及以上）                     |
| ws_stmt_conn_dec          | DOUBLE    |         | /rest/stmt 接口减少连接（3.3.6.10 及以上）                     |
| ws_sml_conn_inc           | DOUBLE    |         | /rest/schemaless 接口新增连接（3.3.6.10 及以上）               |
| ws_sml_conn_dec           | DOUBLE    |         | /rest/schemaless 接口减少连接（3.3.6.10 及以上）               |
| ws_ws_conn_inc            | DOUBLE    |         | /ws 接口新增连接（3.3.6.10 及以上）                            |
| ws_ws_conn_dec            | DOUBLE    |         | /ws 接口减少连接（3.3.6.10 及以上）                            |
| ws_tmq_conn_inc           | DOUBLE    |         | /rest/tmq 接口新增连接（3.3.6.10 及以上）                      |
| ws_tmq_conn_dec           | DOUBLE    |         | /rest/tmq 接口减少连接（3.3.6.10 及以上）                      |
| ws_query_sql_result_count | DOUBLE    |         | /rest/ws 接口当前持有 SQL 查询结果数量（3.3.6.10 及以上）            |
| ws_stmt_stmt_count        | DOUBLE    |         | /rest/stmt 接口当前持有 stmt 数量（3.3.6.10 及以上）             |
| ws_ws_sql_result_count    | DOUBLE    |         | /ws 接口当前持有 SQL 查询结果数量（3.3.6.10 及以上）                 |
| ws_ws_stmt_count          | DOUBLE    |         | /ws 接口当前持有 stmt 数量（3.3.6.10 及以上）                    |
| ws_ws_stmt2_count         | DOUBLE    |         | /ws 接口当前持有 stmt2 数量（3.3.6.10 及以上）                   |
| cpu_percent               | DOUBLE    |         | taosAdapter 的 CPU 占用百分比（v3.3.6.24 及以上 /v3.3.7.7 及以上） |
| endpoint                  | NCHAR     | TAG     | 请求端点                                                |

</details>

`adapter_conn_pool` 表记录 taosAdapter 连接池监控数据：

<details>
<summary>详细信息</summary>

| field            | type      | is\_tag | comment       |
|:-----------------|:----------|:--------|:--------------|
| _ts              | TIMESTAMP |         | 数据采集时间戳       |
| conn_pool_total  | DOUBLE    |         | 连接池的最大连接数限制   |
| conn_pool_in_use | DOUBLE    |         | 连接池当前正在使用的连接数 |
| endpoint         | NCHAR     | TAG     | 请求端点          |
| user             | NCHAR     | TAG     | 连接池所属的用户名     |

</details>

从 **3.3.6.10** 版本开始新增 `adapter_c_interface` 表记录 taosAdapter 调用 C 接口次数数据：

<details>
<summary>详细信息</summary>

| field                                               | type      | is\_tag | comment                |
|:----------------------------------------------------|:----------|:--------|:-----------------------|
| _ts                                                 | TIMESTAMP |         | 数据采集时间戳                |
| taos_connect_total                                  | DOUBLE    |         | 尝试建立连接的总次数             |
| taos_connect_success                                | DOUBLE    |         | 成功建立连接的次数              |
| taos_connect_fail                                   | DOUBLE    |         | 建立连接失败的次数              |
| taos_close_total                                    | DOUBLE    |         | 尝试关闭连接的总次数             |
| taos_close_success                                  | DOUBLE    |         | 成功关闭连接的次数              |
| taos_schemaless_insert_total                        | DOUBLE    |         | schemaless 插入操作的总次数    |
| taos_schemaless_insert_success                      | DOUBLE    |         | schemaless 插入成功的次数     |
| taos_schemaless_insert_fail                         | DOUBLE    |         | schemaless 插入失败的次数     |
| taos_schemaless_free_result_total                   | DOUBLE    |         | schemaless 释放结果集的总次数   |
| taos_schemaless_free_result_success                 | DOUBLE    |         | schemaless 成功释放结果集的次数  |
| taos_query_total                                    | DOUBLE    |         | 执行同步 SQL 的总次数          |
| taos_query_success                                  | DOUBLE    |         | 执行同步 SQL 成功的次数         |
| taos_query_fail                                     | DOUBLE    |         | 执行同步 SQL 失败的次数         |
| taos_query_free_result_total                        | DOUBLE    |         | 释放同步 SQL 结果集的总次数       |
| taos_query_free_result_success                      | DOUBLE    |         | 成功释放同步 SQL 结果集的次数      |
| taos_query_a_with_reqid_total                       | DOUBLE    |         | 带请求 ID 的异步 SQL 总次数     |
| taos_query_a_with_reqid_success                     | DOUBLE    |         | 带请求 ID 的异步 SQL 成功次数    |
| taos_query_a_with_reqid_callback_total              | DOUBLE    |         | 带请求 ID 的异步 SQL 回调总次数   |
| taos_query_a_with_reqid_callback_success            | DOUBLE    |         | 带请求 ID 的异步 SQL 回调成功次数  |
| taos_query_a_with_reqid_callback_fail               | DOUBLE    |         | 带请求 ID 的异步 SQL 回调失败次数  |
| taos_query_a_free_result_total                      | DOUBLE    |         | 异步 SQL 释放结果集的总次数       |
| taos_query_a_free_result_success                    | DOUBLE    |         | 异步 SQL 成功释放结果集的次数      |
| tmq_consumer_poll_result_total                      | DOUBLE    |         | 消费者 poll 有数据的总次数       |
| tmq_free_result_total                               | DOUBLE    |         | 释放 TMQ 数据的总次数          |
| tmq_free_result_success                             | DOUBLE    |         | 成功释放 TMQ 数据的次数         |
| taos_stmt2_init_total                               | DOUBLE    |         | stmt2 初始化的总次数          |
| taos_stmt2_init_success                             | DOUBLE    |         | stmt2 初始化成功的次数         |
| taos_stmt2_init_fail                                | DOUBLE    |         | stmt2 初始化失败的次数         |
| taos_stmt2_close_total                              | DOUBLE    |         | stmt2 关闭的总次数           |
| taos_stmt2_close_success                            | DOUBLE    |         | stmt2 关闭成功的次数          |
| taos_stmt2_close_fail                               | DOUBLE    |         | stmt2 关闭失败的次数          |
| taos_stmt2_get_fields_total                         | DOUBLE    |         | stmt2 获取字段的总次数         |
| taos_stmt2_get_fields_success                       | DOUBLE    |         | stmt2 成功获取字段的次数        |
| taos_stmt2_get_fields_fail                          | DOUBLE    |         | stmt2 获取字段失败的次数        |
| taos_stmt2_free_fields_total                        | DOUBLE    |         | stmt2 释放字段的总次数         |
| taos_stmt2_free_fields_success                      | DOUBLE    |         | stmt2 成功释放字段的次数        |
| taos_stmt_init_with_reqid_total                     | DOUBLE    |         | 带请求 ID 的 stmt 初始化总次数   |
| taos_stmt_init_with_reqid_success                   | DOUBLE    |         | 带请求 ID 的 stmt 初始化成功次数  |
| taos_stmt_init_with_reqid_fail                      | DOUBLE    |         | 带请求 ID 的 stmt 初始化失败次数  |
| taos_stmt_close_total                               | DOUBLE    |         | stmt 关闭的总次数            |
| taos_stmt_close_success                             | DOUBLE    |         | stmt 关闭成功的次数           |
| taos_stmt_close_fail                                | DOUBLE    |         | stmt 关闭失败的次数           |
| taos_stmt_get_tag_fields_total                      | DOUBLE    |         | stmt 获取 tag 字段的总次数     |
| taos_stmt_get_tag_fields_success                    | DOUBLE    |         | stmt 成功获取 tag 字段的次数    |
| taos_stmt_get_tag_fields_fail                       | DOUBLE    |         | stmt 获取 tag 字段失败的次数    |
| taos_stmt_get_col_fields_total                      | DOUBLE    |         | stmt 获取列字段的总次数         |
| taos_stmt_get_col_fields_success                    | DOUBLE    |         | stmt 成功获取列字段的次数        |
| taos_stmt_get_col_fields_fail                       | DOUBLE    |         | stmt 获取列字段失败的次数        |
| taos_stmt_reclaim_fields_total                      | DOUBLE    |         | stmt 释放字段的总次数          |
| taos_stmt_reclaim_fields_success                    | DOUBLE    |         | stmt 成功释放字段的次数         |
| tmq_get_json_meta_total                             | DOUBLE    |         | tmq 获取 JSON 元数据的总次数    |
| tmq_get_json_meta_success                           | DOUBLE    |         | tmq 成功获取 JSON 元数据的次数   |
| tmq_free_json_meta_total                            | DOUBLE    |         | tmq 释放 JSON 元数据的总次数    |
| tmq_free_json_meta_success                          | DOUBLE    |         | tmq 成功释放 JSON 元数据的次数   |
| taos_fetch_whitelist_a_total                        | DOUBLE    |         | 异步获取白名单的总次数            |
| taos_fetch_whitelist_a_success                      | DOUBLE    |         | 异步成功获取白名单的次数           |
| taos_fetch_whitelist_a_callback_total               | DOUBLE    |         | 异步获取白名单回调总次数           |
| taos_fetch_whitelist_a_callback_success             | DOUBLE    |         | 异步成功获取白名单回调次数          |
| taos_fetch_whitelist_a_callback_fail                | DOUBLE    |         | 异步获取白名单回调失败次数          |
| taos_fetch_rows_a_total                             | DOUBLE    |         | 异步获取行的总次数              |
| taos_fetch_rows_a_success                           | DOUBLE    |         | 异步成功获取行的次数             |
| taos_fetch_rows_a_callback_total                    | DOUBLE    |         | 异步获取行回调总次数             |
| taos_fetch_rows_a_callback_success                  | DOUBLE    |         | 异步成功获取行回调次数            |
| taos_fetch_rows_a_callback_fail                     | DOUBLE    |         | 异步获取行回调失败次数            |
| taos_fetch_raw_block_a_total                        | DOUBLE    |         | 异步获取原始块的总次数            |
| taos_fetch_raw_block_a_success                      | DOUBLE    |         | 异步成功获取原始块的次数           |
| taos_fetch_raw_block_a_callback_total               | DOUBLE    |         | 异步获取原始块回调总次数           |
| taos_fetch_raw_block_a_callback_success             | DOUBLE    |         | 异步成功获取原始块回调次数          |
| taos_fetch_raw_block_a_callback_fail                | DOUBLE    |         | 异步获取原始块回调失败次数          |
| tmq_get_raw_total                                   | DOUBLE    |         | 获取原始数据的总次数             |
| tmq_get_raw_success                                 | DOUBLE    |         | 成功获取原始数据的次数            |
| tmq_get_raw_fail                                    | DOUBLE    |         | 获取原始数据失败的次数            |
| tmq_free_raw_total                                  | DOUBLE    |         | 释放原始数据的总次数             |
| tmq_free_raw_success                                | DOUBLE    |         | 成功释放原始数据的次数            |
| tmq_consumer_new_total                              | DOUBLE    |         | 创建新消费者的总次数             |
| tmq_consumer_new_success                            | DOUBLE    |         | 成功创建新消费者的次数            |
| tmq_consumer_new_fail                               | DOUBLE    |         | 创建新消费者失败的次数            |
| tmq_consumer_close_total                            | DOUBLE    |         | 关闭消费者的总次数              |
| tmq_consumer_close_success                          | DOUBLE    |         | 成功关闭消费者的次数             |
| tmq_consumer_close_fail                             | DOUBLE    |         | 关闭消费者失败的次数             |
| tmq_subscribe_total                                 | DOUBLE    |         | 订阅主题的总次数               |
| tmq_subscribe_success                               | DOUBLE    |         | 成功订阅主题的次数              |
| tmq_subscribe_fail                                  | DOUBLE    |         | 订阅主题失败的次数              |
| tmq_unsubscribe_total                               | DOUBLE    |         | 取消订阅的总次数               |
| tmq_unsubscribe_success                             | DOUBLE    |         | 成功取消订阅的次数              |
| tmq_unsubscribe_fail                                | DOUBLE    |         | 取消订阅失败的次数              |
| tmq_list_new_total                                  | DOUBLE    |         | 创建新主题列表的总次数            |
| tmq_list_new_success                                | DOUBLE    |         | 成功创建新主题列表的次数           |
| tmq_list_new_fail                                   | DOUBLE    |         | 创建新主题列表失败的次数           |
| tmq_list_destroy_total                              | DOUBLE    |         | 销毁主题列表的总次数             |
| tmq_list_destroy_success                            | DOUBLE    |         | 成功销毁主题列表的次数            |
| tmq_conf_new_total                                  | DOUBLE    |         | tmq 创建新配置的总次数          |
| tmq_conf_new_success                                | DOUBLE    |         | tmq 成功创建新配置的次数         |
| tmq_conf_new_fail                                   | DOUBLE    |         | tmq 创建新配置失败的次数         |
| tmq_conf_destroy_total                              | DOUBLE    |         | tmq 销毁配置的总次数           |
| tmq_conf_destroy_success                            | DOUBLE    |         | tmq 成功销毁配置的次数          |
| taos_stmt2_prepare_total                            | DOUBLE    |         | stmt2 准备的总次数           |
| taos_stmt2_prepare_success                          | DOUBLE    |         | stmt2 准备成功的次数          |
| taos_stmt2_prepare_fail                             | DOUBLE    |         | stmt2 准备失败的次数          |
| taos_stmt2_is_insert_total                          | DOUBLE    |         | 检查是否为插入的总次数            |
| taos_stmt2_is_insert_success                        | DOUBLE    |         | 成功检查是否为插入的次数           |
| taos_stmt2_is_insert_fail                           | DOUBLE    |         | 检查是否为插入失败的次数           |
| taos_stmt2_bind_param_total                         | DOUBLE    |         | stmt2 绑定参数的总次数         |
| taos_stmt2_bind_param_success                       | DOUBLE    |         | stmt2 成功绑定参数的次数        |
| taos_stmt2_bind_param_fail                          | DOUBLE    |         | stmt2 绑定参数失败的次数        |
| taos_stmt2_exec_total                               | DOUBLE    |         | stmt2 执行的总次数           |
| taos_stmt2_exec_success                             | DOUBLE    |         | stmt2 执行成功的次数          |
| taos_stmt2_exec_fail                                | DOUBLE    |         | stmt2 执行失败的次数          |
| taos_stmt2_error_total                              | DOUBLE    |         | stmt2 错误检查的总次数         |
| taos_stmt2_error_success                            | DOUBLE    |         | stmt2 成功检查错误的次数        |
| taos_fetch_row_total                                | DOUBLE    |         | 同步获取行的总次数              |
| taos_fetch_row_success                              | DOUBLE    |         | 成功同步获取行的次数             |
| taos_is_update_query_total                          | DOUBLE    |         | 检查是否为更新语句的总次数          |
| taos_is_update_query_success                        | DOUBLE    |         | 成功检查是否为更新语句的次数         |
| taos_affected_rows_total                            | DOUBLE    |         | SQL 获取影响行数的总次数         |
| taos_affected_rows_success                          | DOUBLE    |         | SQL 成功获取影响行数的次数        |
| taos_num_fields_total                               | DOUBLE    |         | 获取字段数量的总次数             |
| taos_num_fields_success                             | DOUBLE    |         | 成功获取字段数量的次数            |
| taos_fetch_fields_e_total                           | DOUBLE    |         | 获取字段信息的扩展总次数           |
| taos_fetch_fields_e_success                         | DOUBLE    |         | 成功获取字段信息的扩展次数          |
| taos_fetch_fields_e_fail                            | DOUBLE    |         | 获取字段信息的扩展失败次数          |
| taos_result_precision_total                         | DOUBLE    |         | 获取结果精度的总次数             |
| taos_result_precision_success                       | DOUBLE    |         | 成功获取结果精度的次数            |
| taos_get_raw_block_total                            | DOUBLE    |         | 获取原始块的总次数              |
| taos_get_raw_block_success                          | DOUBLE    |         | 成功获取原始块的次数             |
| taos_fetch_raw_block_total                          | DOUBLE    |         | 拉取原始块的总次数              |
| taos_fetch_raw_block_success                        | DOUBLE    |         | 成功拉取原始块的次数             |
| taos_fetch_raw_block_fail                           | DOUBLE    |         | 拉取原始块失败的次数             |
| taos_fetch_lengths_total                            | DOUBLE    |         | 获取字段长度的总次数             |
| taos_fetch_lengths_success                          | DOUBLE    |         | 成功获取字段长度的次数            |
| taos_write_raw_block_with_reqid_total               | DOUBLE    |         | 带请求 ID 写入原始块的总次数       |
| taos_write_raw_block_with_reqid_success             | DOUBLE    |         | 带请求 ID 成功写入原始块的次数      |
| taos_write_raw_block_with_reqid_fail                | DOUBLE    |         | 带请求 ID 写入原始块失败的次数      |
| taos_write_raw_block_with_fields_with_reqid_total   | DOUBLE    |         | 带请求 ID 和字段写入原始块的总次数    |
| taos_write_raw_block_with_fields_with_reqid_success | DOUBLE    |         | 带请求 ID 和字段成功写入原始块的次数   |
| taos_write_raw_block_with_fields_with_reqid_fail    | DOUBLE    |         | 带请求 ID 和字段写入原始块失败的次数   |
| tmq_write_raw_total                                 | DOUBLE    |         | 写入原始数据的 TMQ 总次数        |
| tmq_write_raw_success                               | DOUBLE    |         | 成功写入原始数据的 TMQ 次数       |
| tmq_write_raw_fail                                  | DOUBLE    |         | 写入原始数据的 TMQ 失败次数       |
| taos_stmt_prepare_total                             | DOUBLE    |         | stmt 准备的总次数            |
| taos_stmt_prepare_success                           | DOUBLE    |         | stmt 准备成功的次数           |
| taos_stmt_prepare_fail                              | DOUBLE    |         | stmt 准备失败的次数           |
| taos_stmt_is_insert_total                           | DOUBLE    |         | 检查 stmt 是否为插入的总次数      |
| taos_stmt_is_insert_success                         | DOUBLE    |         | 成功检查 stmt 是否为插入的次数     |
| taos_stmt_is_insert_fail                            | DOUBLE    |         | 检查 stmt 是否为插入失败的次数     |
| taos_stmt_set_tbname_total                          | DOUBLE    |         | stmt 设置表名的总次数          |
| taos_stmt_set_tbname_success                        | DOUBLE    |         | stmt 成功设置表名的次数         |
| taos_stmt_set_tbname_fail                           | DOUBLE    |         | stmt 设置表名失败的次数         |
| taos_stmt_set_tags_total                            | DOUBLE    |         | stmt 设置 tag 的总次数       |
| taos_stmt_set_tags_success                          | DOUBLE    |         | stmt 成功设置 tag 的次数      |
| taos_stmt_set_tags_fail                             | DOUBLE    |         | stmt 设置 tag 失败的次数      |
| taos_stmt_bind_param_batch_total                    | DOUBLE    |         | stmt 批量绑定参数的总次数        |
| taos_stmt_bind_param_batch_success                  | DOUBLE    |         | stmt 成功批量绑定参数的次数       |
| taos_stmt_bind_param_batch_fail                     | DOUBLE    |         | stmt 批量绑定参数失败的次数       |
| taos_stmt_add_batch_total                           | DOUBLE    |         | stmt 添加批处理的总次数         |
| taos_stmt_add_batch_success                         | DOUBLE    |         | stmt 成功添加批处理的次数        |
| taos_stmt_add_batch_fail                            | DOUBLE    |         | stmt 添加批处理失败的次数        |
| taos_stmt_execute_total                             | DOUBLE    |         | stmt 执行的总次数            |
| taos_stmt_execute_success                           | DOUBLE    |         | stmt 执行成功的次数           |
| taos_stmt_execute_fail                              | DOUBLE    |         | stmt 执行失败的次数           |
| taos_stmt_num_params_total                          | DOUBLE    |         | stmt 获取参数数量的总次数        |
| taos_stmt_num_params_success                        | DOUBLE    |         | stmt 成功获取参数数量的次数       |
| taos_stmt_num_params_fail                           | DOUBLE    |         | stmt 获取参数数量失败的次数       |
| taos_stmt_get_param_total                           | DOUBLE    |         | stmt 获取参数的总次数          |
| taos_stmt_get_param_success                         | DOUBLE    |         | stmt 成功获取参数的次数         |
| taos_stmt_get_param_fail                            | DOUBLE    |         | stmt 获取参数失败的次数         |
| taos_stmt_errstr_total                              | DOUBLE    |         | stmt 获取 stmt 错误信息的总次数  |
| taos_stmt_errstr_success                            | DOUBLE    |         | stmt 成功获取 stmt 错误信息的次数 |
| taos_stmt_affected_rows_once_total                  | DOUBLE    |         | stmt 获取单次影响行数的总次数      |
| taos_stmt_affected_rows_once_success                | DOUBLE    |         | stmt 成功获取单次影响行数的次数     |
| taos_stmt_use_result_total                          | DOUBLE    |         | stmt 使用结果集的总次数         |
| taos_stmt_use_result_success                        | DOUBLE    |         | stmt 成功使用结果集的次数        |
| taos_stmt_use_result_fail                           | DOUBLE    |         | stmt 使用结果集失败的次数        |
| taos_select_db_total                                | DOUBLE    |         | 选择数据库的总次数              |
| taos_select_db_success                              | DOUBLE    |         | 成功选择数据库的次数             |
| taos_select_db_fail                                 | DOUBLE    |         | 选择数据库失败的次数             |
| taos_get_tables_vgId_total                          | DOUBLE    |         | 获取表 vgroup ID 的总次数     |
| taos_get_tables_vgId_success                        | DOUBLE    |         | 成功获取表 vgroup ID 的次数    |
| taos_get_tables_vgId_fail                           | DOUBLE    |         | 获取表 vgroup ID 失败的次数    |
| taos_options_connection_total                       | DOUBLE    |         | 设置连接选项的总次数             |
| taos_options_connection_success                     | DOUBLE    |         | 成功设置连接选项的次数            |
| taos_options_connection_fail                        | DOUBLE    |         | 设置连接选项失败的次数            |
| taos_validate_sql_total                             | DOUBLE    |         | 验证 SQL 的总次数            |
| taos_validate_sql_success                           | DOUBLE    |         | 成功验证 SQL 的次数           |
| taos_validate_sql_fail                              | DOUBLE    |         | 验证 SQL 失败的次数           |
| taos_check_server_status_total                      | DOUBLE    |         | 检查服务器状态的总次数            |
| taos_check_server_status_success                    | DOUBLE    |         | 成功检查服务器状态的次数           |
| taos_get_current_db_total                           | DOUBLE    |         | 获取当前数据库的总次数            |
| taos_get_current_db_success                         | DOUBLE    |         | 成功获取当前数据库的次数           |
| taos_get_current_db_fail                            | DOUBLE    |         | 获取当前数据库失败的次数           |
| taos_get_server_info_total                          | DOUBLE    |         | 获取服务器信息的总次数            |
| taos_get_server_info_success                        | DOUBLE    |         | 成功获取服务器信息的次数           |
| taos_options_total                                  | DOUBLE    |         | 设置选项的总次数               |
| taos_options_success                                | DOUBLE    |         | 成功设置选项的次数              |
| taos_options_fail                                   | DOUBLE    |         | 设置选项失败的次数              |
| taos_set_conn_mode_total                            | DOUBLE    |         | 设置连接模式的总次数             |
| taos_set_conn_mode_success                          | DOUBLE    |         | 成功设置连接模式的次数            |
| taos_set_conn_mode_fail                             | DOUBLE    |         | 设置连接模式失败的次数            |
| taos_reset_current_db_total                         | DOUBLE    |         | 重置当前数据库的总次数            |
| taos_reset_current_db_success                       | DOUBLE    |         | 成功重置当前数据库的次数           |
| taos_set_notify_cb_total                            | DOUBLE    |         | 设置通知回调的总次数             |
| taos_set_notify_cb_success                          | DOUBLE    |         | 成功设置通知回调的次数            |
| taos_set_notify_cb_fail                             | DOUBLE    |         | 设置通知回调失败的次数            |
| taos_errno_total                                    | DOUBLE    |         | 获取错误码的总次数              |
| taos_errno_success                                  | DOUBLE    |         | 成功获取错误码的次数             |
| taos_errstr_total                                   | DOUBLE    |         | 获取错误信息的总次数             |
| taos_errstr_success                                 | DOUBLE    |         | 成功获取错误信息的次数            |
| tmq_consumer_poll_total                             | DOUBLE    |         | tmq 消费者 poll 的总次数      |
| tmq_consumer_poll_success                           | DOUBLE    |         | tmq 消费者 poll 成功的次数     |
| tmq_consumer_poll_fail                              | DOUBLE    |         | tmq 消费者 poll 失败的次数     |
| tmq_subscription_total                              | DOUBLE    |         | tmq 获取订阅信息的总次数         |
| tmq_subscription_success                            | DOUBLE    |         | tmq 成功获取订阅信息的次数        |
| tmq_subscription_fail                               | DOUBLE    |         | tmq 获取订阅信息失败的次数        |
| tmq_list_append_total                               | DOUBLE    |         | tmq 列表追加的总次数           |
| tmq_list_append_success                             | DOUBLE    |         | tmq 成功列表追加的次数          |
| tmq_list_append_fail                                | DOUBLE    |         | tmq 列表追加失败的次数          |
| tmq_list_get_size_total                             | DOUBLE    |         | tmq 获取列表大小的总次数         |
| tmq_list_get_size_success                           | DOUBLE    |         | tmq 成功获取列表大小的次数        |
| tmq_err2str_total                                   | DOUBLE    |         | tmq 错误码转字符串的总次数        |
| tmq_err2str_success                                 | DOUBLE    |         | tmq 成功将错误码转为字符串的次数     |
| tmq_conf_set_total                                  | DOUBLE    |         | tmq 设置配置的总次数           |
| tmq_conf_set_success                                | DOUBLE    |         | tmq 成功设置配置的次数          |
| tmq_conf_set_fail                                   | DOUBLE    |         | tmq 设置配置失败的次数          |
| tmq_get_res_type_total                              | DOUBLE    |         | tmq 获取资源类型的总次数         |
| tmq_get_res_type_success                            | DOUBLE    |         | tmq 成功获取资源类型的次数        |
| tmq_get_topic_name_total                            | DOUBLE    |         | tmq 获取主题名称的总次数         |
| tmq_get_topic_name_success                          | DOUBLE    |         | tmq 成功获取主题名称的次数        |
| tmq_get_vgroup_id_total                             | DOUBLE    |         | tmq 获取 vgroup ID 的总次数  |
| tmq_get_vgroup_id_success                           | DOUBLE    |         | tmq 成功获取 vgroup ID 的次数 |
| tmq_get_vgroup_offset_total                         | DOUBLE    |         | tmq 获取 vgroup 偏移量的总次数  |
| tmq_get_vgroup_offset_success                       | DOUBLE    |         | tmq 成功获取 vgroup 偏移量的次数 |
| tmq_get_db_name_total                               | DOUBLE    |         | tmq 获取数据库名称的总次数        |
| tmq_get_db_name_success                             | DOUBLE    |         | tmq 成功获取数据库名称的次数       |
| tmq_get_table_name_total                            | DOUBLE    |         | tmq 获取表名称的总次数          |
| tmq_get_table_name_success                          | DOUBLE    |         | tmq 成功获取表名称的次数         |
| tmq_get_connect_total                               | DOUBLE    |         | tmq 获取连接的总次数           |
| tmq_get_connect_success                             | DOUBLE    |         | tmq 成功获取连接的次数          |
| tmq_commit_sync_total                               | DOUBLE    |         | tmq 同步提交的总次数           |
| tmq_commit_sync_success                             | DOUBLE    |         | tmq 同步提交成功的次数          |
| tmq_commit_sync_fail                                | DOUBLE    |         | tmq 同步提交失败的次数          |
| tmq_fetch_raw_block_total                           | DOUBLE    |         | tmq 获取原始块的总次数          |
| tmq_fetch_raw_block_success                         | DOUBLE    |         | tmq 成功获取原始块的次数         |
| tmq_fetch_raw_block_fail                            | DOUBLE    |         | tmq 获取原始块失败的次数         |
| tmq_get_topic_assignment_total                      | DOUBLE    |         | tmq 获取主题分配的总次数         |
| tmq_get_topic_assignment_success                    | DOUBLE    |         | tmq 成功获取主题分配的次数        |
| tmq_get_topic_assignment_fail                       | DOUBLE    |         | tmq 获取主题分配失败的次数        |
| tmq_offset_seek_total                               | DOUBLE    |         | tmq 偏移量定位的总次数          |
| tmq_offset_seek_success                             | DOUBLE    |         | tmq 成功偏移量定位的次数         |
| tmq_offset_seek_fail                                | DOUBLE    |         | tmq 偏移量定位失败的次数         |
| tmq_committed_total                                 | DOUBLE    |         | tmq 获取已提交偏移量的总次数       |
| tmq_committed_success                               | DOUBLE    |         | tmq 成功获取已提交偏移量的次数      |
| tmq_commit_offset_sync_fail                         | DOUBLE    |         | tmq 同步提交偏移量失败的次数       |
| tmq_position_total                                  | DOUBLE    |         | tmq 获取当前位置的总次数         |
| tmq_position_success                                | DOUBLE    |         | tmq 成功获取当前位置的次数        |
| tmq_commit_offset_sync_total                        | DOUBLE    |         | tmq 同步提交偏移量的总次数        |
| tmq_commit_offset_sync_success                      | DOUBLE    |         | tmq 同步提交偏移量成功的次数       |
| endpoint                                            | NCHAR     | TAG     | 请求端点                   |

</details>

从 **3.3.6.29**/**3.3.8.3** 版本开始新增 `adapter_request_limit` 表记录 taosAdapter 查询请求限流数据：

<details>
<summary>详细信息</summary>

| field                 | type      | is\_tag | comment                          |
|:----------------------|:----------|:--------|:---------------------------------|
| _ts                   | TIMESTAMP |         | 数据采集时间戳                          |
| query_limit           | DOUBLE    |         | 允许同时执行的查询请求的最大并发数                |
| query_max_wait        | DOUBLE    |         | 查询队列中允许等待执行的最大查询数量               |
| query_inflight        | DOUBLE    |         | 当前正在执行的、受并发限制的查询数量               |
| query_wait_count      | DOUBLE    |         | 当前在队列中等待执行的查询数量                  |
| query_count           | DOUBLE    |         | 本采集周期内收到的、受并发限制的查询请求总数           |
| query_wait_fail_count | DOUBLE    |         | 本采集周期内因等待超时或超过最大等待队列长度而失败的查询请求数量 |
| endpoint              | NCHAR     | TAG     | 请求端点                             |
| user                  | NCHAR     | TAG     | 发起查询请求的认证用户名                     |

</details>

## httpd 升级为 taosAdapter 的变化

在 TDengine TSDB server 2.2.x.x 或更早期版本中，taosd 进程包含一个内嵌的 http 服务（httpd）。如前面所述，taosAdapter 是一个使用 systemd 管理的独立软件，拥有自己的进程。并且两者有一些配置参数和行为是不同的，请见下表：

| **#** | **embedded httpd**  | **taosAdapter**               | **comment**                                                                                    |
|-------|---------------------|-------------------------------|------------------------------------------------------------------------------------------------|
| 1     | httpEnableRecordSql | --logLevel=debug              |                                                                                                |
| 2     | httpMaxThreads      | n/a                           | taosAdapter 自动管理线程池，无需此参数                                                                      |
| 3     | telegrafUseFieldNum | 请参考 taosAdapter telegraf 配置方法 |                                                                                                |
| 4     | restfulRowLimit     | restfulRowLimit               | 内嵌 httpd 默认输出 10240 行数据，最大允许值为 102400。taosAdapter 也提供 restfulRowLimit 但是默认不做限制。您可以根据实际场景需求进行配置 |
| 5     | httpDebugFlag       | 不适用                           | httpdDebugFlag 对 taosAdapter 不起作用                                                              |
| 6     | httpDBNameMandatory | 不适用                           | taosAdapter 要求 URL 中必须指定数据库名                                                                   |
