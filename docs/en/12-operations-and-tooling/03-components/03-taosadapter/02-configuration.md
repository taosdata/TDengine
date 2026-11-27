---
title: Configuration Reference
sidebar_label: Configuration Reference
toc_max_heading_level: 4
---

## Configuration

taosAdapter supports configuration through command-line parameters, environment variables, and configuration files. The default configuration file is `/etc/taos/taosadapter.toml`, and you can specify the configuration file using the -c or --config command-line parameter..

Command-line parameters take precedence over environment variables, which take precedence over configuration files. The command-line usage is arg=val, such as taosadapter -p=30000 --debug=true.

See the example configuration file at [example/config/taosadapter.toml](https://github.com/taosdata/taosadapter/blob/3.0/example/config/taosadapter.toml).

### Basic Configuration

The basic configuration parameters for `taosAdapter` are as follows:

- **`debug`**: Whether to enable debug mode (pprof)
  - **When set to `true` (default)**: Enables Go pprof debug mode, allowing access to debug information via `http://<fqdn>:<port>/debug/pprof`.
  - **When set to `false`**: Disables debug mode, preventing access to debug information.

- **`instanceId`**: The instance ID of `taosAdapter`, used to distinguish logs from different instances. Default value: `32`.

- **`port`**: The port on which `taosAdapter` provides HTTP/WebSocket services. Default value: `6041`.

- **`taosConfigDir`**: The configuration file directory for TDengine. Default value: `/etc/taos`. The `taos.cfg` file in this directory will be loaded.

### SSL {#ssl}

`[ssl]` enables HTTPS / WSS on taosAdapter (Enterprise Edition). See the example configuration at [example/config/taosadapter.toml](https://github.com/taosdata/taosadapter/blob/3.0/example/config/taosadapter.toml). For certificate generation and enablement steps, see [Full-Trace Transport Security · Server HTTPS/WSS](../../../11-security-guide/02-full-trace-transport.md#211-server-side-httpswss) and [Appendix: Generate a Self-Signed Certificate](../../../11-security-guide/02-full-trace-transport.md#11-appendix-generate-a-self-signed-certificate).

- **`ssl.enable`**: Whether to enable SSL. Default `false`. Enterprise capability.
- **`ssl.certFile`**: Path to the server certificate file (PEM).
- **`ssl.keyFile`**: Path to the server private key file (PEM); recommended permission `600`.

After enabling, restart taosAdapter (for example `systemctl restart taosadapter`). Logs typically show `SSL is enabled`. For client TrustStore / `wss` / `useSSL`, see [Client and Connector Security](../../../11-security-guide/05-client-connector-security.md).

Starting from version 3.3.4.0, taosAdapter supports setting the number of concurrent calls for invoking C methods:

- **`maxAsyncConcurrentLimit`**

  Sets the maximum number of concurrent calls for C asynchronous methods (`0` means using the number of CPU cores).

- **`maxSyncConcurrentLimit`**

  Sets the maximum number of concurrent calls for C synchronous methods (`0` means using the number of CPU cores).

### Registration Configuration

Starting from version **3.4.0.0**, taosAdapter will register itself to the TDengine TSDB. It can be queried using the SQL statement `select * from performance_schema.perf_instances where type = 'taosadapter'`.

The registration configuration parameters are as follows:

- **`register.instance`**: The address of the taosAdapter instance, with a maximum length of 255 bytes. If not set or set to an empty string, the system will automatically generate it by concatenating the hostname and port number. If `ssl.enable` is true, a `https` protocol header will be prepended.
- **`register.description`**: The description of the taosAdapter instance, with a maximum length of 511 bytes. The default value is an empty string.
- **`register.duration`**: The registration interval for the taosAdapter instance, in seconds. The default value is 10 seconds. Every time this interval elapses, the instance will re-register to refresh its expiration time. This value must be greater than 0 and less than `register.expire`.
- **`register.expire`**: The expiration time for the taosAdapter instance registration, in seconds. The default value is 30 seconds. If no registration refresh request is received within this time, the registration information will be deleted. This value must be greater than `register.duration`.

### Cross-Origin Configuration

When making API calls from the browser, please configure the following Cross-Origin Resource Sharing (CORS) parameters based on your actual situation:

- **`cors.allowAllOrigins`**: Whether to allow all origins to access, default is true.
- **`cors.allowOrigins`**: A comma-separated list of origins allowed to access. Multiple origins can be specified.
- **`cors.allowHeaders`**: A comma-separated list of request headers allowed for cross-origin access. Multiple headers can be specified.
- **`cors.exposeHeaders`**: A comma-separated list of response headers exposed for cross-origin access. Multiple headers can be specified.
- **`cors.allowCredentials`**: Whether to allow cross-origin requests to include user credentials, such as cookies, HTTP authentication information, or client SSL certificates.
- **`cors.allowWebSockets`**: Whether to allow WebSockets connections.

If you are not making API calls through a browser, you do not need to worry about these configurations.

The above configurations take effect for the following interfaces:

- RESTful API requests
- WebSocket API requests
- InfluxDB v1 write interface
- OpenTSDB HTTP write interface

For details about the CORS protocol, please refer to: [https://www.w3.org/wiki/CORS_Enabled](https://www.w3.org/wiki/CORS_Enabled) or [https://developer.mozilla.org/docs/Web/HTTP/CORS](https://developer.mozilla.org/docs/Web/HTTP/CORS).

### Connection Pool Configuration

taosAdapter uses a connection pool to manage connections to TDengine, improving concurrency performance and resource utilization. The connection pool configuration applies to the following interfaces, and these interfaces share a single connection pool:

- RESTful API requests
- InfluxDB v1 write interface
- OpenTSDB JSON and telnet format writing
- Telegraf data writing
- collectd data writing
- StatsD data writing
- node_exporter data collection writing
- OpenMetrics data collection and writing
- Prometheus remote_read and remote_write
- JSON data writing

The configuration parameters for the connection pool are as follows:

- **`pool.maxConnect`**: The maximum number of connections allowed in the pool, default is twice the number of CPU cores. It is recommended to keep the default setting.
- **`pool.maxIdle`**: The maximum number of idle connections in the pool, default is the same as `pool.maxConnect`. It is recommended to keep the default setting.
- **`pool.idleTimeout`**: Connection idle timeout, default is never timeout. It is recommended to keep the default setting.
- **`pool.waitTimeout`**: Timeout for obtaining a connection from the pool, default is set to 60 seconds. If a connection is not obtained within the timeout period, HTTP status code 503 will be returned. This parameter is available starting from version 3.3.3.0.
- **`pool.maxWait`**: The maximum number of requests waiting to get a connection in the pool, default is 0, which means no limit. When the number of queued requests exceeds this value, new requests will return HTTP status code 503. This parameter is available starting from version 3.3.3.0.

### Query Result Set Configuration

taosAdapter can automatically release query result sets that stay idle for a long time on the WebSocket interface (`/ws`), preventing result sets from being retained when a client stops fetching data, for example after a client-side query timeout.

- **`queryResult.idleTimeout`**: Idle timeout for query result sets, default is 1 hour. This configuration applies to result sets created by the `query` action and by stmt/stmt2 `use_result` on the `/ws` interface. The idle timer starts from the last fetch of a result set (or from its creation if it has never been fetched) and is reset on every fetch. A result set that stays idle longer than this duration is released automatically; afterwards, fetch requests for its id fail with a "result is nil" error. Set to 0 to disable this cleanup. This configuration does not affect the RESTful interface, which returns all results within the request.

### HTTP Response Code Configuration

taosAdapter uses the parameter `httpCodeServerError` to set whether to return a non-200 HTTP status code when the C interface returns an error. When set to true, it will return different HTTP status codes based on the error code returned by C. See [HTTP Response Codes](../../../10-developer-guide/08-connectors-reference/10-rest-api.mdx) for details.

This configuration only affects the **RESTful interface**.

Parameter Description:

- **`httpCodeServerError`**:
  - **When set to `true`**: Map the error code returned by the C interface to the corresponding HTTP status code.
  - **When set to `false`**: Regardless of the error returned by the C interface, always return the HTTP status code `200` (default value).

### Memory limit configuration

taosAdapter will monitor the memory usage during its operation and adjust it through two thresholds. The valid value range is an integer from 1 to 100, and the unit is the percentage of system physical memory.

This configuration only affects the following interfaces:

- RESTful interface request
- InfluxDB v1 write interface
- OpenTSDB HTTP write interface
- Prometheus remote_read and remote_write interfaces

#### Parameter Description

- **`pauseQueryMemoryThreshold`**:
  - When memory usage exceeds this threshold, taosAdapter will stop processing query requests.
  - Default value: `70` (i.e. 70% of system physical memory).
- **`pauseAllMemoryThreshold`**:
  - When memory usage exceeds this threshold, taosAdapter will stop processing all requests (including writes and queries).
  - Default value: `80` (i.e. 80% of system physical memory).

When memory usage falls below the threshold, taosAdapter will automatically resume the corresponding function.

#### HTTP return content

- **When `pauseQueryMemoryThreshold` is exceeded**:
  - HTTP status code: `503`
  - Return content: `"query memory exceeds threshold"`

- **When `pauseAllMemoryThreshold` is exceeded**:
  - HTTP status code: `503`
  - Return content: `"memory exceeds threshold"`

#### Status check interface

The memory status of taosAdapter can be checked through the following interface:

- **Normal status**: `http://<fqdn>:6041/-/ping` returns `code 200`.
- **Memory exceeds threshold**:
  - If the memory exceeds `pauseAllMemoryThreshold`, `code 503` is returned.
  - If the memory exceeds `pauseQueryMemoryThreshold` and the request parameter contains `action=query`, `code 503` is returned.

#### Related configuration parameters

- **`monitor.collectDuration`**: memory monitoring interval, default value is `3s`, environment variable is `TAOS_MONITOR_COLLECT_DURATION`.
- **`monitor.incgroup`**: whether to run in a container (set to `true` for running in a container), default value is `false`, environment variable is `TAOS_MONITOR_INCGROUP`.
- **`monitor.pauseQueryMemoryThreshold`**: memory threshold (percentage) for query request pause, default value is `70`, environment variable is `TAOS_MONITOR_PAUSE_QUERY_MEMORY_THRESHOLD`.
- **`monitor.pauseAllMemoryThreshold`**: memory threshold (percentage) for query and write request pause, default value is `80`, environment variable is `TAOS_MONITOR_PAUSE_ALL_MEMORY_THRESHOLD`.

You can make corresponding adjustments based on the specific project application scenario and operation strategy, and it is recommended to use operation monitoring software to monitor the system memory status in a timely manner. The load balancer can also check the operation status of taosAdapter through this interface.

### Schemaless write create DB configuration

Starting from **version 3.0.4.0**, taosAdapter provides the parameter `smlAutoCreateDB` to control whether to automatically create a database (DB) when writing to the schemaless protocol.

The `smlAutoCreateDB` parameter only affects the following interfaces:

- InfluxDB v1 write interface
- OpenTSDB JSON and telnet format writing
- Telegraf data writing
- collectd data writing
- StatsD data writing
- node_exporter data writing
- OpenMetrics data collection and writing

#### Parameter Description

- **`smlAutoCreateDB`**:
  - **When set to `true`**: When writing to the schemaless protocol, if the target database does not exist, taosAdapter will automatically create the database.
  - **When set to `false`**: The user needs to manually create the database, otherwise the write will fail (default value).

### Number of results returned configuration

taosAdapter provides the parameter `restfulRowLimit` to control the number of results returned by the HTTP interface.

The `restfulRowLimit` parameter only affects the return results of the following interfaces:

- RESTful interface
- Prometheus remote_read interface

#### Parameter Description

- **`restfulRowLimit`**:
  - **When set to a positive integer**: The number of results returned by the interface will not exceed this value.
  - **When set to `-1`**: The number of results returned by the interface is unlimited (default value).

### Log configuration

The log can be configured with the following parameters:

- **`log.path`**

  Specifies the log storage path (Default: `"/var/log/taos"`).

- **`log.level`**

  Sets the log level (Default: `"info"`).

- **`log.keepDays`**

  Number of days to retain logs (Positive integer, Default: `30`).

- **`log.rotationCount`**

  Number of log files to rotate (Default: `30`).

- **`log.rotationSize`**

  Maximum size of a single log file (Supports KB/MB/GB units, Default: `"1GB"`).

- **`log.compress`**

  Whether to compress old log files (Default: `false`).

- **`log.rotationTime`**

  Log rotation interval (Deprecated, fixed at 24-hour rotation).

- **`log.reservedDiskSize`**

  Disk space reserved for log directory (Supports KB/MB/GB units, Default: `"1GB"`).

- **`log.enableSqlToCsvLogging`**

  Whether to record SQL to CSV files(Default: `false`).For details, see [Recording SQL to CSV Files](03-operations.md#recording-sql-to-csv-files).

- **`log.enableStmtToCsvLogging`**

  Whether to record STMT to CSV files(Default: `false`).For details, see [Recording STMT to CSV Files](03-operations.md#recording-stmt-to-csv-files).

- **`log.enableRecordHttpSql`**

  **This parameter is deprecated; use [Recording SQL to CSV Files](03-operations.md#recording-sql-to-csv-files) instead.**

  Whether to record HTTP SQL requests (Default: `false`).

- **`log.sqlRotationCount`**

  **This parameter is deprecated; use [Recording SQL to CSV Files](03-operations.md#recording-sql-to-csv-files) instead.**

  Number of SQL log files to rotate (Default: `2`).

- **`log.sqlRotationSize`**

  **This parameter is deprecated; use [Recording SQL to CSV Files](03-operations.md#recording-sql-to-csv-files) instead.**

  Maximum size of a single SQL log file (Supports KB/MB/GB units, Default: `"1GB"`).

- **`log.sqlRotationTime`**

  **This parameter is deprecated; use [Recording SQL to CSV Files](03-operations.md#recording-sql-to-csv-files) instead.**

  SQL log rotation interval (Default: `24h`).

You can set the taosAdapter log output detail level by setting the --log.level parameter or the environment variable TAOS_ADAPTER_LOG_LEVEL. Valid values include: panic, fatal, error, warn, warning, info, debug, and trace.

### Third-party Data Source Configuration

#### Collectd Configuration

- **`collectd.enable`**

  Enable/disable collectd protocol support (Default: `false`)

- **`collectd.port`**

  Collectd service listening port (Default: `6045`)

- **`collectd.db`**

  Target database for collectd data (Default: `"collectd"`)

- **`collectd.user`**

  Database username (Default: `"root"`)

- **`collectd.password`**

  Database password (Default: `"taosdata"`)

- **`collectd.token`**

  Database token (Default: `""`). Available in TDengine Enterprise Edition 3.4.0.0 and above.

- **`collectd.ttl`**

  Data time-to-live (Default: `0` = no expiration)

- **`collectd.worker`**

  Number of write worker threads (Default: `10`)

#### InfluxDB Configuration

- **`influxdb.enable`**

  Enable/disable InfluxDB protocol support (Default: `true`)

#### OpenTSDB Configuration

- **`opentsdb.enable`**

  Enable OpenTSDB HTTP protocol (Default: `true`)

- **`opentsdb_telnet.enable`**

  Enable OpenTSDB Telnet (Warning: no auth, Default: `false`)

- **`opentsdb_telnet.ports`**

  Listening ports (Default: `[6046,6047,6048,6049]`)

- **`opentsdb_telnet.dbs`**

  Target databases (Default: `["opentsdb_telnet","collectd_tsdb","icinga2_tsdb","tcollector_tsdb"]`)

- **`opentsdb_telnet.user`**

  Database username (Default: `"root"`)

- **`opentsdb_telnet.password`**

  Database password (Default: `"taosdata"`)

- **`opentsdb_telnet.ttl`**

  Data TTL (Default: `0`)

- **`opentsdb_telnet.token`**

  Database token (Default: `""`). Available in TDengine Enterprise Edition 3.4.0.0 and above.

- **`opentsdb_telnet.batchSize`**

  Batch write size (Default: `1`)

- **`opentsdb_telnet.flushInterval`**

  Flush interval (Default: `0s`)

- **`opentsdb_telnet.maxTCPConnections`**

  Max TCP connections (Default: `250`)

- **`opentsdb_telnet.tcpKeepAlive`**

  Enable TCP KeepAlive (Default: `false`)

#### StatsD Configuration

- **`statsd.enable`**

  Enable StatsD protocol (Default: `false`)

- **`statsd.port`**

  Listening port (Default: `6044`)

- **`statsd.protocol`**

  Transport protocol (Options: tcp/udp/tcp4/udp4, Default: `"udp4"`)

- **`statsd.db`**

  Target database (Default: `"statsd"`)

- **`statsd.user`**

  Database username (Default: `"root"`)

- **`statsd.password`**

  Database password (Default: `"taosdata"`)

- **`statsd.token`**

  Database token (Default: `""`). Available in TDengine Enterprise Edition 3.4.0.0 and above.

- **`statsd.ttl`**

  Data TTL (Default: `0`)

- **`statsd.gatherInterval`**

  Collection interval (Default: `5s`)

- **`statsd.worker`**

  Worker threads (Default: `10`)

- **`statsd.allowPendingMessages`**

  Max pending messages (Default: `50000`)

- **`statsd.maxTCPConnections`**

  Max TCP connections (Default: `250`)

- **`statsd.tcpKeepAlive`**

  Enable TCP KeepAlive (Default: `false`)

- **`statsd.deleteCounters`**

  Clear counter cache after collection (Default: `true`)

- **`statsd.deleteGauges`**

  Clear gauge cache after collection (Default: `true`)

- **`statsd.deleteSets`**

  Clear sets cache after collection (Default: `true`)

- **`statsd.deleteTimings`**

  Clear timings cache after collection (Default: `true`)

#### Prometheus Configuration

- **`prometheus.enable`**

  Enable Prometheus protocol (Default: `true`)

#### OpenMetrics Configuration

- **`open_metrics.enable`**

  Enable/disable OpenMetrics data collection (Default: `false`).

- **`open_metrics.user`**

  Username for TDengine connection (Default: `"root"`).

- **`open_metrics.password`**

  Password for TDengine connection (Default: `"taosdata"`).

- **`open_metrics.urls`**

  List of OpenMetrics data collection endpoints (Default: `["http://localhost:9100"]`, automatically appends `/metrics` if no route specified).

- **`open_metrics.dbs`**

  Target databases for data writing (Default: `["open_metrics"]`, must match number of collection URLs).

- **`open_metrics.responseTimeoutSeconds`**

  Collection timeout in seconds (Default: `[5]`, must match number of collection URLs).

- **`open_metrics.httpUsernames`**

  Basic authentication usernames (If enabled, must match number of collection URLs, Default: empty).

- **`open_metrics.httpPasswords`**

  Basic authentication passwords (If enabled, must match number of collection URLs, Default: empty).

- **`open_metrics.httpBearerTokenStrings`**

  Bearer token authentication strings (If enabled, must match number of collection URLs, Default: empty).

- **`open_metrics.caCertFiles`**

  Root certificate file paths (If enabled, must match number of collection URLs, Default: empty).

- **`open_metrics.certFiles`**

  Client certificate file paths (If enabled, must match number of collection URLs, Default: empty).

- **`open_metrics.keyFiles`**

  Client certificate key file paths (If enabled, must match number of collection URLs, Default: empty).

- **`open_metrics.insecureSkipVerify`**

  Skip HTTPS certificate verification (Default: `true`).

- **`open_metrics.gatherDurationSeconds`**

  Collection interval in seconds (Default: `[5]`, must match number of collection URLs).

- **`open_metrics.token`**

  Database token (Default: `""`). Available in TDengine Enterprise Edition 3.4.0.0 and above.

- **`open_metrics.ttl`**

  Table Time-To-Live in seconds (`0` means no expiration, if enabled must match number of collection URLs, Default: empty).

- **`open_metrics.ignoreTimestamp`**

  Ignore timestamps in collected data (uses collection time if ignored, Default: `false`).

#### Node Exporter Configuration

- **`node_exporter.enable`**

  Enable node_exporter data collection (Default: `false`)

- **`node_exporter.db`**

  Target database name (Default: `"node_exporter"`)

- **`node_exporter.urls`**

  Service endpoints (Default: `["http://localhost:9100"]`)

- **`node_exporter.gatherDuration`**

  Collection interval (Default: `5s`)

- **`node_exporter.responseTimeout`**

  Request timeout (Default: `5s`)

- **`node_exporter.user`**

  Database username (Default: `"root"`)

- **`node_exporter.password`**

  Database password (Default: `"taosdata"`)

- **`node_exporter.token`**

  Database token (Default: `""`). Available in TDengine Enterprise Edition 3.4.0.0 and above.

- **`node_exporter.ttl`**

  Data TTL (Default: `0`)

- **`node_exporter.httpUsername`**

  HTTP Basic Auth username (Optional)

- **`node_exporter.httpPassword`**

  HTTP Basic Auth password (Optional)

- **`node_exporter.httpBearerTokenString`**

  HTTP Bearer Token (Optional)

- **`node_exporter.insecureSkipVerify`**

  Skip SSL verification (Default: `true`)

- **`node_exporter.certFile`**

  Client certificate path (Optional)

- **`node_exporter.keyFile`**

  Client key path (Optional)

- **`node_exporter.caCertFile`**

  CA certificate path (Optional)

### Metrics Reporting Configuration

taosAdapter reports metrics to taosKeeper with these parameters:

- **`uploadKeeper.enable`**

  Enable metrics reporting (Default: `true`)

- **`uploadKeeper.url`**

  taosKeeper endpoint (Default: `http://127.0.0.1:6043/adapter_report`)

- **`uploadKeeper.interval`**

  Reporting interval (Default: `15s`)

- **`uploadKeeper.timeout`**

  Request timeout (Default: `5s`)

- **`uploadKeeper.retryTimes`**

  Max retries (Default: `3`)

- **`uploadKeeper.retryInterval`**

  Retry interval (Default: `5s`)

### Query Request Concurrency Limit Configuration

Starting from version **3.3.6.29**/**3.3.8.3**, taosAdapter supports configuring concurrency limits for query requests to prevent excessive concurrent queries from exhausting system resources.
When this feature is enabled, taosAdapter controls the number of concurrent query requests being processed simultaneously based on the configured concurrency limit. Requests exceeding the limit will enter a waiting state until processing resources become available.

If the waiting time exceeds the configured timeout or the number of waiting requests exceeds the configured maximum waiting requests, taosAdapter will directly return an error response, indicating that there are too many requests.
RESTful requests will return HTTP status code `503`, and WebSocket requests will return error code `0xFFFE`.

This configuration affects the following interfaces:

- **RESTful Interface**
- **WebSocket SQL Execution Interface**

Parameter Description

- **`request.queryLimitEnable`**
  - **When set to `true`**: Enables the query request concurrency limit feature.
  - **When set to `false`**: Disables the query request concurrency limit feature (default value).
- **`request.default.queryLimit`**
  - Sets the default concurrency limit for query requests (default value: `0`, meaning no limit).
- **`request.default.queryWaitTimeout`**
  - Sets the maximum waiting time (in seconds) for requests that exceed the concurrency limit. Requests that time out while waiting will return an error directly. Default value: `900`.
- **`request.default.queryMaxWait`**
  - Sets the maximum number of waiting requests allowed when the concurrency limit is exceeded. Requests exceeding this number will return an error directly. Default value: `0`, meaning no limit.
- **`request.excludeQueryLimitSql`**
  - Configures a list of SQL statements that are not subject to concurrency limits. Must start with `select` (case-insensitive).
- **`request.excludeQueryLimitSqlRegex`**
  - Configures a list of regular expressions for SQL statements that are not subject to concurrency limits.

Customizable per User

Configurable only via the configuration file:

- **`request.users.<username>.queryLimit`**
  - Sets the query request concurrency limit for the specified user. Takes precedence over the default setting.
- **`request.users.<username>.queryWaitTimeout`**
  - Sets the maximum waiting time (in seconds) for requests that exceed the concurrency limit for the specified user. Takes precedence over the default setting.
- **`request.users.<username>.queryMaxWait`**
  - Sets the maximum number of waiting requests allowed when the concurrency limit is exceeded for the specified user. Takes precedence over the default setting.

Example

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

- `queryLimitEnable = true` enables the query request concurrency limit feature.
- `excludeQueryLimitSql = ["select 1","select server_version()"]` excludes two commonly used SQL queries for ping.
- `excludeQueryLimitSqlRegex = ['(?i)^select\s+.*from\s+information_schema.*']` excludes all SQL queries that query the information_schema database.
- `request.default` configures the default query request concurrency limit to 200, wait timeout to 900 seconds, and maximum wait requests to 0 (unlimited).
- `request.users.root` configures the query request concurrency limit for user root to 100, wait timeout to 200 seconds, and maximum wait requests to 10.

When user root initiates a query request, taosAdapter will perform concurrency limit processing based on the above configuration.
When the number of query requests exceeds 100, subsequent requests will enter a waiting state until resources are available.
If the wait time exceeds 200 seconds or the number of waiting requests exceeds 10, taosAdapter will directly return an error response.

When other users initiate query requests, the default concurrency limit configuration will be used for processing.
Each user's configuration is independent and does not share the concurrency limit of `request.default`.
For example, when user user1 initiates 200 concurrent query requests, user user2 can also initiate 200 concurrent query requests simultaneously without blocking.

### Reject Query SQL Configuration

Starting from **version 3.3.6.34 / 3.4.0.0**, taosAdapter supports rejecting specific query SQL statements through configuration, preventing the execution of unsafe or highly resource-consuming queries.  
When this feature is enabled, taosAdapter checks each SQL statement that does **not** start with `insert` (case-insensitive). If the SQL matches any of the configured reject patterns, an error response is returned indicating that the query is forbidden.

When a rejected SQL query is matched, the RESTful API returns HTTP status code `403`, and the WebSocket interface returns error code `0xFFFD`.  
Meanwhile, taosAdapter prints a warning log containing details such as the SQL source, for example:

```text
reject sql, client_ip:192.168.1.98, port:59912, user:root, app:test_app, reject_regex:(?i)^drop\s+table\s+.*, sql:DROP taBle testdb.stb
```

This configuration affects the following interfaces:

- **RESTful interface**
- **WebSocket SQL execution interface**

Parameter Description

- **`rejectQuerySqlRegex`**
  - A list of regex patterns for rejecting SQL queries. Supports [Google RE2 syntax](https://github.com/google/re2/wiki/Syntax).
  - Default: an empty list, meaning no queries are rejected.

Example

```toml
rejectQuerySqlRegex = ['(?i)^drop\s+database\s+.*','(?i)^drop\s+table\s+.*','(?i)^alter\s+table\s+.*']
```

The configuration `rejectQuerySqlRegex = ['(?i)^drop\\s+database\\s+.*','(?i)^drop\\s+table\\s+.*','(?i)^alter\\s+table\\s+.*']` rejects all `drop database`, `drop table`, and `alter table` queries, ignoring case.

### Environment Variables

Configuration Parameters and their corresponding environment variables:

<details>
<summary>Details</summary>

| Configuration Parameter               | Environment Variable                                  |
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
| `log.enableStmtToCsvLogging`          | `TAOS_ADAPTER_LOG_ENABLE_STMT_TO_CSV_LOGGING`         |
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
