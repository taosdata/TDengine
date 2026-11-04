---
title: 配置参考
sidebar_label: 配置参考
toc_max_heading_level: 4
---

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
- **`taosConfigDir`**：TDengine 的配置文件目录，默认值：`/etc/taos`。该目录下的 `taos.cfg` 文件将被加载。

### SSL {#ssl}

`[ssl]` 用于在 taosAdapter 上启用 HTTPS / WSS（企业版）。示例配置见 [example/config/taosadapter.toml](https://github.com/taosdata/taosadapter/blob/3.0/example/config/taosadapter.toml)。证书生成与启用步骤见 [全链路传输安全 · 服务端 HTTPS/WSS](../../../11-security-guide/02-full-trace-transport.md#211-server-httpswss) 与 [附录：生成自签名证书](../../../11-security-guide/02-full-trace-transport.md#11-appendix-self-signed-cert)。

- **`ssl.enable`**：是否启用 SSL。默认 `false`。企业版能力。
- **`ssl.certFile`**：服务端证书文件路径（PEM）。
- **`ssl.keyFile`**：服务端私钥文件路径（PEM）；建议权限 `600`。

启用后需重启 taosAdapter（如 `systemctl restart taosadapter`）。日志中通常可见 `SSL is enabled`。客户端 TrustStore / `wss` / `useSSL` 见 [客户端与连接器安全](../../../11-security-guide/05-client-connector-security.md)。

从 **3.3.4.0 版本** 开始，taosAdapter 支持设置调用 C 方法并发调用数：

- **`maxAsyncConcurrentLimit`**

  设置 C 异步方法的最大并发调用数（0 表示使用 CPU 核心数）。

- **`maxSyncConcurrentLimit`**

  设置 C 同步方法的最大并发调用数（0 表示使用 CPU 核心数）。

### 注册配置

从 **3.4.0.0** 版本开始，taosAdapter 将注册到 TDengine 中，可以通过 SQL 语句 `select * from performance_schema.perf_instances where type = 'taosadapter'` 进行查询。

注册配置参数如下：

- **`register.instance`**：taosAdapter 实例的地址，长度不超过 255 个字节。如未设置或设置为空字符串，系统将使用主机名与端口号自动拼接生成，若 `ssl.enable` 为 true，将拼接 https 协议头。
- **`register.description`**：taosAdapter 实例的描述信息，长度不超过 511 个字节，默认值为空字符串。
- **`register.duration`**：taosAdapter 实例注册的时间间隔，单位为秒，默认值为 10 秒。每隔此时间间隔，将重新注册以刷新其超时时间。必须大于 0 且小于 `register.expire`。
- **`register.expire`**：taosAdapter 实例注册的过期时间，单位为秒，默认值为 30 秒。若在此时间内未收到注册刷新请求，注册信息将被删除。必须大于 `register.duration`。

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

taosAdapter 使用连接池管理与 TDengine 的连接，以提高并发性能和资源利用率。连接池配置对以下接口生效，且以下接口共享一个连接池：

- RESTful 接口请求
- InfluxDB v1 写接口
- OpenTSDB JSON 和 telnet 格式写入
- Telegraf 数据写入
- collectd 数据写入
- StatsD 数据写入
- node_exporter 数据写入
- OpenMetrics 数据写入
- Prometheus remote_read 和 remote_write
- JSON 数据写入

连接池的配置参数如下：

- **`pool.maxConnect`**：连接池允许的最大连接数，默认值为 2 倍 CPU 核心数。建议保持默认设置。
- **`pool.maxIdle`**：连接池中允许的最大空闲连接数，默认与 `pool.maxConnect` 相同。建议保持默认设置。
- **`pool.idleTimeout`**：连接空闲超时时间，默认永不超时。建议保持默认设置。
- **`pool.waitTimeout`**：从连接池获取连接的超时时间，默认设置为 60 秒。如果在超时时间内未能获取连接，将返回 HTTP 状态码 503。该参数从 `v3.3.3.0` 开始提供。
- **`pool.maxWait`**：连接池中等待获取连接的请求数上限，默认值为 0，表示不限制。当排队请求数超过此值时，新的请求将返回 HTTP 状态码 503。该参数从 `v3.3.3.0` 开始提供。

### 查询结果集配置

taosAdapter 支持自动释放 WebSocket 接口（`/ws`）中长期空闲的查询结果集，避免客户端因查询超时等原因不再拉取数据而导致结果集驻留。

- **`queryResult.idleTimeout`**：查询结果集空闲超时时间，默认值为 1 小时。该配置适用于 `/ws` 接口的 `query` 以及 stmt/stmt2 的 `use_result` 产生的结果集。计时自结果集上一次 fetch 起算（从未 fetch 过的自创建时刻起算），每次 fetch 会重新计时；空闲超过该时长的结果集将被自动释放，此后客户端再 fetch 该结果集会收到 "result is nil" 错误。设置为 0 表示关闭该功能。该配置不影响 RESTful 接口，RESTful 接口在请求内即返回全部结果。

### HTTP 返回码配置

taosAdapter 通过参数 `httpCodeServerError` 来控制当底层 C 接口返回错误时，是否在 RESTful 接口请求中返回非 200 的 HTTP 状态码。当设置为 `true` 时，taosAdapter 会根据 C 接口返回的错误码映射为相应的 HTTP 状态码。具体映射规则请参考 [HTTP 响应码](../../../10-developer-guide/08-connectors-reference/10-rest-api.mdx#http-响应码)。

该配置只会影响 **RESTful 接口**。

**参数说明** ：

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

**参数说明** ：

- **`pauseQueryMemoryThreshold`**：
  - 当内存使用超过此阈值时，taosAdapter 将停止处理查询请求。
  - 默认值：`70`（即 70% 的系统物理内存）。
- **`pauseAllMemoryThreshold`**：
  - 当内存使用超过此阈值时，taosAdapter 将停止处理所有请求（包括写入和查询）。
  - 默认值：`80`（即 80% 的系统物理内存）。

当内存使用回落到阈值以下时，taosAdapter 会自动恢复相应功能。

**HTTP 返回内容** ：

- **超过 `pauseQueryMemoryThreshold` 时**：
  - HTTP 状态码：`503`
  - 返回内容：`"query memory exceeds threshold"`
- **超过 `pauseAllMemoryThreshold` 时**：
  - HTTP 状态码：`503`
  - 返回内容：`"memory exceeds threshold"`

**状态检查接口** ：

可以通过以下接口检查 taosAdapter 的内存状态：

- **正常状态**：`http://<fqdn>:6041/-/ping` 返回 `code 200`。
- **内存超过阈值**：
  - 如果内存超过 `pauseAllMemoryThreshold`，返回 `code 503`。
  - 如果内存超过 `pauseQueryMemoryThreshold`，且请求参数包含 `action=query`，返回 `code 503`。

**相关配置参数** ：

- **`monitor.collectDuration`**：内存监控间隔，默认值为 `3s`，环境变量为 `TAOS_MONITOR_COLLECT_DURATION`。
- **`monitor.incgroup`**：是否在容器中运行（容器中运行设置为 `true`），默认值为 `false`，环境变量为 `TAOS_MONITOR_INCGROUP`。
- **`monitor.pauseQueryMemoryThreshold`**：查询请求暂停的内存阈值（百分比），默认值为 `70`，环境变量为 `TAOS_MONITOR_PAUSE_QUERY_MEMORY_THRESHOLD`。
- **`monitor.pauseAllMemoryThreshold`**：查询和写入请求暂停的内存阈值（百分比），默认值为 `80`，环境变量为 `TAOS_MONITOR_PAUSE_ALL_MEMORY_THRESHOLD`。

你可以根据具体项目应用场景和运营策略进行相应调整，并建议使用运营监控软件及时进行系统内存状态监控。负载均衡器也可以通过这个接口检查 taosAdapter 运行状态。

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

**参数说明** ：

- **`smlAutoCreateDB`**：
  - **设置为 `true` 时**：在 schemaless 协议写入时，如果目标数据库不存在，taosAdapter 会自动创建该数据库。
  - **设置为 `false` 时**：用户需要手动创建数据库，否则写入会失败（默认值）。

### 结果返回条数配置

taosAdapter 提供了参数 `restfulRowLimit`，用于控制 HTTP 接口返回的结果条数。

`restfulRowLimit` 参数只会影响以下接口的返回结果：

- RESTful 接口
- Prometheus remote_read 接口

**参数说明** ：

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

  是否启用记录 SQL 到 CSV 文件（默认值：`false`）具体内容见 [记录 SQL 到 csv 文件](03-operations.md#记录-sql-到-csv-文件)。

- **`log.enableStmtToCsvLogging`**

  是否启用记录 Stmt 到 CSV 文件（默认值：`false`）具体内容见 [记录 STMT 到 csv 文件](03-operations.md#记录-stmt-到-csv-文件)。

- **`log.enableRecordHttpSql`**

  **不建议继续使用此参数，推荐使用[记录 SQL 到 csv 文件](03-operations.md#记录-sql-到-csv-文件)作为替代方案**

  是否记录 HTTP SQL 请求（默认值：`false`）。

- **`log.sqlRotationCount`**

  **不建议继续使用此参数，推荐使用[记录 SQL 到 csv 文件](03-operations.md#记录-sql-到-csv-文件)作为替代方案**

  SQL 日志轮转数量（默认值：`2`）。

- **`log.sqlRotationSize`**

  **不建议继续使用此参数，推荐使用[记录 SQL 到 csv 文件](03-operations.md#记录-sql-到-csv-文件)作为替代方案**

  单个 SQL 日志文件最大大小（支持 KB/MB/GB 单位，默认值：`"1GB"`）。

- **`log.sqlRotationTime`**

  **不建议继续使用此参数，推荐使用[记录 SQL 到 csv 文件](03-operations.md#记录-sql-到-csv-文件)作为替代方案**

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

- **`collectd.token`**

  设置连接数据库使用的 token（默认值：`""`），TDengine TSDB Enterprise `v3.4.0.0` 及以上生效。

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

- **`opentsdb_telnet.token`**

  设置连接数据库使用的 token（默认值：`""`），TDengine TSDB Enterprise `v3.4.0.0` 及以上生效。

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

- **`statsd.token`**

  设置连接数据库使用的 token（默认值：`""`），TDengine TSDB Enterprise `v3.4.0.0` 及以上生效。

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

  配置连接 TDengine 的用户名（默认值：`"root"`）。

- **`open_metrics.password`**

  设置连接 TDengine 的密码（默认值：`"taosdata"`）。

- **`open_metrics.token`**

  设置连接数据库使用的 token（默认值：`""`），TDengine TSDB Enterprise `v3.4.0.0` 及以上生效。

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

- **`node_exporter.token`**

  设置连接数据库使用的 token（默认值：`""`），TDengine TSDB Enterprise `v3.4.0.0` 及以上生效。

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
  - 配置禁用查询 SQL 的正则表达式列表。支持 [Google RE2 语法](https://github.com/google/re2/wiki/Syntax)。
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
| `collectd.token`                      | `TAOS_ADAPTER_COLLECTD_TOKEN`                         |
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
| `log.enableSTMTToCsvLogging`          | `TAOS_ADAPTER_LOG_ENABLE_STMT_TO_CSV_LOGGING`         |
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
| `node_exporter.token`                 | `TAOS_ADAPTER_NODE_EXPORTER_TOKEN`                    |
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
| `open_metrics.token`                  | `TAOS_ADAPTER_OPEN_METRICS_TOKEN`                     |
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
| `opentsdb_telnet.token`               | `TAOS_ADAPTER_OPENTSDB_TELNET_TOKEN`                  |
| `opentsdb_telnet.ttl`                 | `TAOS_ADAPTER_OPENTSDB_TELNET_TTL`                    |
| `opentsdb_telnet.user`                | `TAOS_ADAPTER_OPENTSDB_TELNET_USER`                   |
| `pool.idleTimeout`                    | `TAOS_ADAPTER_POOL_IDLE_TIMEOUT`                      |
| `pool.maxConnect`                     | `TAOS_ADAPTER_POOL_MAX_CONNECT`                       |
| `pool.maxIdle`                        | `TAOS_ADAPTER_POOL_MAX_IDLE`                          |
| `pool.maxWait`                        | `TAOS_ADAPTER_POOL_MAX_WAIT`                          |
| `pool.waitTimeout`                    | `TAOS_ADAPTER_POOL_WAIT_TIMEOUT`                      |
| `P`, `port`                           | `TAOS_ADAPTER_PORT`                                   |
| `prometheus.enable`                   | `TAOS_ADAPTER_PROMETHEUS_ENABLE`                      |
| `queryResult.idleTimeout`             | `TAOS_ADAPTER_QUERY_RESULT_IDLE_TIMEOUT`              |
| `register.description`                | `TAOS_ADAPTER_REGISTER_DESCRIPTION`                   |
| `register.duration`                   | `TAOS_ADAPTER_REGISTER_DURATION`                      |
| `register.expire`                     | `TAOS_ADAPTER_REGISTER_EXPIRE`                        |
| `register.instance`                   | `TAOS_ADAPTER_REGISTER_INSTANCE`                      |
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
| `statsd.token`                        | `TAOS_ADAPTER_STATSD_TOKEN`                           |
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
