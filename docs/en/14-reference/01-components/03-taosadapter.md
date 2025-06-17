---
title: taosAdapter Reference
sidebar_label: taosAdapter
slug: /tdengine-reference/components/taosadapter
---

import Image from '@theme/IdealImage';
import imgAdapter from '../../assets/taosadapter-01.png';
import Prometheus from "../../assets/resources/_prometheus.mdx"
import CollectD from "../../assets/resources/_collectd.mdx"
import StatsD from "../../assets/resources/_statsd.mdx"
import Icinga2 from "../../assets/resources/_icinga2.mdx"
import TCollector from "../../assets/resources/_tcollector.mdx"

taosAdapter is a companion tool for TDengine, serving as a bridge and adapter between the TDengine cluster and applications. It provides an easy and efficient way to ingest data directly from data collection agents (such as Telegraf, StatsD, collectd, etc.). It also offers InfluxDB/OpenTSDB compatible data ingestion interfaces, allowing InfluxDB/OpenTSDB applications to be seamlessly ported to TDengine.
The connectors of TDengine in various languages communicate with TDengine through the WebSocket interface, hence the taosAdapter must be installed.

The architecture diagram is as follows:

<figure>
<Image img={imgAdapter} alt="taosAdapter architecture"/>
<figcaption>Figure 1. taosAdapter architecture</figcaption>
</figure>

## Feature List

The taosAdapter provides the following features:

- WebSocket Interface:
  Supports executing SQL, schemaless writing, parameter binding, and data subscription through the WebSocket protocol.
- InfluxDB v1 write interface:
  [https://docs.influxdata.com/influxdb/v2.0/reference/api/influxdb-1x/write/](https://docs.influxdata.com/influxdb/v2.0/reference/api/influxdb-1x/write/)
- Compatible with OpenTSDB JSON and telnet format writing:
  - [http://opentsdb.net/docs/build/html/api_http/put.html](http://opentsdb.net/docs/build/html/api_http/put.html)
  - [http://opentsdb .net/docs/build/html/api_telnet/put.html](http://opentsdb.net/docs/build/html/api_telnet/put.html)
- collectd data writing:
  collectd is a system statistics collection daemon, visit [https://collectd.org/](https://collectd.org/) for more information.
- StatsD data writing:
  StatsD is a simple yet powerful daemon for gathering statistics. Visit [https://github.com/statsd/statsd](https://github.com/statsd/statsd) for more information.
- icinga2 OpenTSDB writer data writing:
  icinga2 is a software for collecting check results metrics and performance data. Visit [https://icinga.com/docs/icinga-2/latest/doc/14-features/#opentsdb-writer](https://icinga.com/docs/icinga-2/latest/doc/14-features/#opentsdb-writer) for more information.
- TCollector data writing:
  TCollector is a client process that collects data from local collectors and pushes it to OpenTSDB. Visit [http://opentsdb.net/docs/build/html/user_guide/utilities/tcollector.html](http://opentsdb.net/docs/build/html/user_guide/utilities/tcollector.html) for more information.
- node_exporter data collection and writing:
  node_exporter is an exporter of machine metrics. Visit [https://github.com/prometheus/node_exporter](https://github.com/prometheus/node_exporter) for more information.
- Supports Prometheus remote_read and remote_write:
  remote_read and remote_write are Prometheus's data read-write separation cluster solutions. Visit [https://prometheus.io/blog/2019/10/10/remote-read-meets-streaming/#remote-apis](https://prometheus.io/blog/2019/10/10/remote-read-meets-streaming/#remote-apis) for more information.
- RESTful API:
  [RESTful API](../../client-libraries/rest-api/)

### WebSocket Interface

Through the WebSocket interface of taosAdapter, connectors in various languages can achieve SQL execution, schemaless writing, parameter binding, and data subscription functionalities. Refer to the [Development Guide](../../../developer-guide/connecting-to-tdengine/#websocket-connection) for more details.

### InfluxDB v1 write interface

You can use any client that supports the HTTP protocol to write data in InfluxDB compatible format to TDengine by accessing the Restful interface URL `http://<fqdn>:6041/influxdb/v1/write`.

Supported InfluxDB parameters are as follows:

- `db` specifies the database name used by TDengine
- `precision` the time precision used by TDengine
- `u` TDengine username
- `p` TDengine password
- `ttl` the lifespan of automatically created subtables, determined by the TTL parameter of the first data entry in the subtable, which cannot be updated. For more information, please refer to the TTL parameter in the [table creation document](../../sql-manual/manage-tables/).

Note: Currently, InfluxDB's token authentication method is not supported, only Basic authentication and query parameter verification are supported.
Example: `curl --request POST http://127.0.0.1:6041/influxdb/v1/write?db=test --user "root:taosdata" --data-binary "measurement,host=host1 field1=2i,field2=2.0 1577836800000000000"`

### OpenTSDB JSON and telnet format writing

You can use any client that supports the HTTP protocol to write data in OpenTSDB compatible format to TDengine by accessing the Restful interface URL `http://<fqdn>:6041/<APIEndPoint>`. EndPoint as follows:

```text
/opentsdb/v1/put/json/<db>
/opentsdb/v1/put/telnet/<db>
```

### collectd data writing

<CollectD />

### StatsD data writing

<StatsD />

### icinga2 OpenTSDB writer data writing

<Icinga2 />

### TCollector data writing

<TCollector />

### node_exporter data collection and writing

An exporter used by Prometheus that exposes hardware and operating system metrics from \*NIX kernels

- Enable configuration of taosAdapter node_exporter.enable
- Set the relevant configuration for node_exporter
- Restart taosAdapter

### Supports Prometheus remote_read and remote_write

<Prometheus />

### RESTful API

You can use any client that supports the HTTP protocol to write data to TDengine or query data from TDengine by accessing the RESTful interface URL `http://<fqdn>:6041/rest/sql`. For details, please refer to the [REST API documentation](../../client-libraries/rest-api/).

## Installation

taosAdapter is part of the TDengine server software. If you are using TDengine server, you do not need any additional steps to install taosAdapter. If you need to deploy taosAdapter separately from the TDengine server, you should install the complete TDengine on that server to install taosAdapter. If you need to compile taosAdapter from source code, you can refer to the [Build taosAdapter](https://github.com/taosdata/taosadapter/blob/3.0/BUILD.md) document.

After the installation is complete, you can start the taosAdapter service using the command `systemctl start taosadapter`.

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

Starting from version 3.3.4.0, taosAdapter supports setting the number of concurrent calls for invoking C methods:

- **`maxAsyncConcurrentLimit`**

  Sets the maximum number of concurrent calls for C asynchronous methods (`0` means using the number of CPU cores).

- **`maxSyncConcurrentLimit`**

  Sets the maximum number of concurrent calls for C synchronous methods (`0` means using the number of CPU cores).

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
- Prometheus remote_read and remote_write

The configuration parameters for the connection pool are as follows:

- **`pool.maxConnect`**: The maximum number of connections allowed in the pool, default is twice the number of CPU cores. It is recommended to keep the default setting.
- **`pool.maxIdle`**: The maximum number of idle connections in the pool, default is the same as `pool.maxConnect`. It is recommended to keep the default setting.
- **`pool.idleTimeout`**: Connection idle timeout, default is never timeout. It is recommended to keep the default setting.
- **`pool.waitTimeout`**: Timeout for obtaining a connection from the pool, default is set to 60 seconds. If a connection is not obtained within the timeout period, HTTP status code 503 will be returned. This parameter is available starting from version 3.3.3.0.
- **`pool.maxWait`**: The maximum number of requests waiting to get a connection in the pool, default is 0, which means no limit. When the number of queued requests exceeds this value, new requests will return HTTP status code 503. This parameter is available starting from version 3.3.3.0.

### HTTP Response Code Configuration

taosAdapter uses the parameter `httpCodeServerError` to set whether to return a non-200 HTTP status code when the C interface returns an error. When set to true, it will return different HTTP status codes based on the error code returned by C. See [HTTP Response Codes](../../client-libraries/rest-api/) for details.

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

- **`log.enableRecordHttpSql`**

  Whether to record HTTP SQL requests (Default: `false`).

- **`log.sqlRotationCount`**

  Number of SQL log files to rotate (Default: `2`).

- **`log.sqlRotationSize`**

  Maximum size of a single SQL log file (Supports KB/MB/GB units, Default: `"1GB"`).

- **`log.sqlRotationTime`**

  SQL log rotation interval (Default: `24h`).

1. You can set the taosAdapter log output detail level by setting the --log.level parameter or the environment variable TAOS_ADAPTER_LOG_LEVEL. Valid values ​​include: panic, fatal, error, warn, warning, info, debug, and trace.
2. Starting from **3.3.5.0 version**, taosAdapter supports dynamic modification of log level through HTTP interface. Users can dynamically adjust the log level by sending HTTP PUT request to /config interface. The authentication method of this interface is the same as /rest/sql interface, and the configuration item key-value pair in JSON format must be passed in the request body.

The following is an example of setting the log level to debug through the curl command:

```shell
curl --location --request PUT 'http://127.0.0.1:6041/config' \
-u root:taosdata \
--data '{"log.level": "debug"}'
```

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

- **`collectd.ttl`**

  Data time-to-live (Default: `0` = no expiration)

- **`collectd.worker`**

  Number of write worker threads (Default: `10`)

#### InfluxDB Configuration

- **`influxdb.enable`**

  Enable/disable InfluxDB protocol support (Default: `true`)

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

## Service Management

### Starting/Stopping taosAdapter

On Linux systems, the taosAdapter service is managed by default by systemd. Use the command `systemctl start taosadapter` to start the taosAdapter service. Use the command `systemctl stop taosadapter` to stop the taosAdapter service.

### Upgrading taosAdapter

taosAdapter and TDengine server need to use the same version. Please upgrade taosAdapter by upgrading the TDengine server.
taosAdapter deployed separately from taosd must be upgraded by upgrading the TDengine server on its server.

### Removing taosAdapter

Use the command rmtaos to remove the TDengine server software, including taosAdapter.

## Monitoring Metrics

Currently, taosAdapter only collects monitoring indicators for RESTful/WebSocket related requests. There are no monitoring indicators for other interfaces.

taosAdapter reports monitoring indicators to taosKeeper, which will be written to the monitoring database by taosKeeper. The default is the `log` database, which can be modified in the taoskeeper configuration file. The following is a detailed introduction to these monitoring indicators.

The `adapter_requests` table records taosAdapter monitoring data:

<details>
<summary>Details</summary>

| field            | type         | is_tag | comment                                   |
|:-----------------|:-------------|:-------|:------------------------------------------|
| ts               | TIMESTAMP    |        | data collection timestamp                 |
| total            | INT UNSIGNED |        | total number of requests                  |
| query            | INT UNSIGNED |        | number of query requests                  |
| write            | INT UNSIGNED |        | number of write requests                  |
| other            | INT UNSIGNED |        | number of other requests                  |
| in_process       | INT UNSIGNED |        | number of requests in process             |
| success          | INT UNSIGNED |        | number of successful requests             |
| fail             | INT UNSIGNED |        | number of failed requests                 |
| query_success    | INT UNSIGNED |        | number of successful query requests       |
| query_fail       | INT UNSIGNED |        | number of failed query requests           |
| write_success    | INT UNSIGNED |        | number of successful write requests       |
| write_fail       | INT UNSIGNED |        | number of failed write requests           |
| other_success    | INT UNSIGNED |        | number of successful other requests       |
| other_fail       | INT UNSIGNED |        | number of failed other requests           |
| query_in_process | INT UNSIGNED |        | number of query requests in process       |
| write_in_process | INT UNSIGNED |        | number of write requests in process       |
| endpoint         | VARCHAR      |        | request endpoint                          |
| req_type         | NCHAR        | tag    | request type: 0 for REST, 1 for WebSocket |

</details>

The `adapter_status` table records the status data of taosAdapter:

<details>
<summary>Details</summary>

| field                       | type      | is\_tag | comment                                                                            |
|:----------------------------|:----------|:--------|:-----------------------------------------------------------------------------------|
| _ts                         | TIMESTAMP |         | data collection timestamp                                                          |
| go_heap_sys                 | DOUBLE    |         | heap memory allocated by Go runtime (bytes)                                        |
| go_heap_inuse               | DOUBLE    |         | heap memory in use by Go runtime (bytes)                                           |
| go_stack_sys                | DOUBLE    |         | stack memory allocated by Go runtime (bytes)                                       |
| go_stack_inuse              | DOUBLE    |         | stack memory in use by Go runtime (bytes)                                          |
| rss                         | DOUBLE    |         | actual physical memory occupied by the process (bytes)                             |
| ws_query_conn               | DOUBLE    |         | current WebSocket connections for `/rest/ws` endpoint                              |
| ws_stmt_conn                | DOUBLE    |         | current WebSocket connections for `/rest/stmt` endpoint                            |
| ws_sml_conn                 | DOUBLE    |         | current WebSocket connections for `/rest/schemaless` endpoint                      |
| ws_ws_conn                  | DOUBLE    |         | current WebSocket connections for `/ws` endpoint                                   |
| ws_tmq_conn                 | DOUBLE    |         | current WebSocket connections for `/rest/tmq` endpoint                             |
| async_c_limit               | DOUBLE    |         | total concurrency limit for the C asynchronous interface                           |
| async_c_inflight            | DOUBLE    |         | current concurrency for the C asynchronous interface                               |
| sync_c_limit                | DOUBLE    |         | total concurrency limit for the C synchronous interface                            |
| sync_c_inflight             | DOUBLE    |         | current concurrency for the C synchronous interface                                |
| `ws_query_conn_inc`         | DOUBLE    |         | New connections on `/rest/ws` interface (Available since v3.3.6.10)                |
| `ws_query_conn_dec`         | DOUBLE    |         | Closed connections on `/rest/ws` interface (Available since v3.3.6.10)             |
| `ws_stmt_conn_inc`          | DOUBLE    |         | New connections on `/rest/stmt` interface (Available since v3.3.6.10)              |
| `ws_stmt_conn_dec`          | DOUBLE    |         | Closed connections on `/rest/stmt` interface (Available since v3.3.6.10)           |
| `ws_sml_conn_inc`           | DOUBLE    |         | New connections on `/rest/schemaless` interface (Available since v3.3.6.10)        |
| `ws_sml_conn_dec`           | DOUBLE    |         | Closed connections on `/rest/schemaless` interface (Available since v3.3.6.10)     |
| `ws_ws_conn_inc`            | DOUBLE    |         | New connections on `/ws` interface (Available since v3.3.6.10)                     |
| `ws_ws_conn_dec`            | DOUBLE    |         | Closed connections on `/ws` interface (Available since v3.3.6.10)                  |
| `ws_tmq_conn_inc`           | DOUBLE    |         | New connections on `/rest/tmq` interface (Available since v3.3.6.10)               |
| `ws_tmq_conn_dec`           | DOUBLE    |         | Closed connections on `/rest/tmq` interface (Available since v3.3.6.10)            |
| `ws_query_sql_result_count` | DOUBLE    |         | Current SQL query results held by `/rest/ws` interface (Available since v3.3.6.10) |
| `ws_stmt_stmt_count`        | DOUBLE    |         | Current stmt objects held by `/rest/stmt` interface (Available since v3.3.6.10)    |
| `ws_ws_sql_result_count`    | DOUBLE    |         | Current SQL query results held by `/ws` interface (Available since v3.3.6.10)      |
| `ws_ws_stmt_count`          | DOUBLE    |         | Current stmt objects held by `/ws` interface (Available since v3.3.6.10)           |
| `ws_ws_stmt2_count`         | DOUBLE    |         | Current stmt2 objects held by `/ws` interface (Available since v3.3.6.10)          |
| endpoint                    | NCHAR     | TAG     | request endpoint                                                                   |

</details>

The `adapter_conn_pool` table records the connection pool monitoring data of taosAdapter:

<details>
<summary>Details</summary>

| field            | type      | is\_tag | comment                                                     |
|:-----------------|:----------|:--------|:------------------------------------------------------------|
| _ts              | TIMESTAMP |         | data collection timestamp                                   |
| conn_pool_total  | DOUBLE    |         | maximum connection limit for the connection pool            |
| conn_pool_in_use | DOUBLE    |         | current number of connections in use in the connection pool |
| endpoint         | NCHAR     | TAG     | request endpoint                                            |
| user             | NCHAR     | TAG     | username to which the connection pool belongs               |

</details>

Starting from version **3.3.6.10**, the `adapter_c_interface` table has been added to record taosAdapter C interface call metrics:

<details>
<summary>Details</summary>

| field                                               | type      | is\_tag | comment                                                  |
|:----------------------------------------------------|:----------|:--------|:---------------------------------------------------------|
| _ts                                                 | TIMESTAMP |         | Data collection timestamp                                |
| taos_connect_total                                  | DOUBLE    |         | Count of total connection attempts                       |
| taos_connect_success                                | DOUBLE    |         | Count of successful connections                          |
| taos_connect_fail                                   | DOUBLE    |         | Count of failed connections                              |
| taos_close_total                                    | DOUBLE    |         | Count of total close attempts                            |
| taos_close_success                                  | DOUBLE    |         | Count of successful closes                               |
| taos_schemaless_insert_total                        | DOUBLE    |         | Count of schemaless insert operations                    |
| taos_schemaless_insert_success                      | DOUBLE    |         | Count of successful schemaless inserts                   |
| taos_schemaless_insert_fail                         | DOUBLE    |         | Count of failed schemaless inserts                       |
| taos_schemaless_free_result_total                   | DOUBLE    |         | Count of schemaless result set releases                  |
| taos_schemaless_free_result_success                 | DOUBLE    |         | Count of successful schemaless result set releases       |
| taos_query_total                                    | DOUBLE    |         | Count of synchronous SQL executions                      |
| taos_query_success                                  | DOUBLE    |         | Count of successful synchronous SQL executions           |
| taos_query_fail                                     | DOUBLE    |         | Count of failed synchronous SQL executions               |
| taos_query_free_result_total                        | DOUBLE    |         | Count of synchronous SQL result set releases             |
| taos_query_free_result_success                      | DOUBLE    |         | Count of successful synchronous SQL result set releases  |
| taos_query_a_with_reqid_total                       | DOUBLE    |         | Count of async SQL with request ID                       |
| taos_query_a_with_reqid_success                     | DOUBLE    |         | Count of successful async SQL with request ID            |
| taos_query_a_with_reqid_callback_total              | DOUBLE    |         | Count of async SQL callbacks with request ID             |
| taos_query_a_with_reqid_callback_success            | DOUBLE    |         | Count of successful async SQL callbacks with request ID  |
| taos_query_a_with_reqid_callback_fail               | DOUBLE    |         | Count of failed async SQL callbacks with request ID      |
| taos_query_a_free_result_total                      | DOUBLE    |         | Count of async SQL result set releases                   |
| taos_query_a_free_result_success                    | DOUBLE    |         | Count of successful async SQL result set releases        |
| tmq_consumer_poll_result_total                      | DOUBLE    |         | Count of consumer polls with data                        |
| tmq_free_result_total                               | DOUBLE    |         | Count of TMQ data releases                               |
| tmq_free_result_success                             | DOUBLE    |         | Count of successful TMQ data releases                    |
| taos_stmt2_init_total                               | DOUBLE    |         | Count of stmt2 initializations                           |
| taos_stmt2_init_success                             | DOUBLE    |         | Count of successful stmt2 initializations                |
| taos_stmt2_init_fail                                | DOUBLE    |         | Count of failed stmt2 initializations                    |
| taos_stmt2_close_total                              | DOUBLE    |         | Count of stmt2 closes                                    |
| taos_stmt2_close_success                            | DOUBLE    |         | Count of successful stmt2 closes                         |
| taos_stmt2_close_fail                               | DOUBLE    |         | Count of failed stmt2 closes                             |
| taos_stmt2_get_fields_total                         | DOUBLE    |         | Count of stmt2 field fetches                             |
| taos_stmt2_get_fields_success                       | DOUBLE    |         | Count of successful stmt2 field fetches                  |
| taos_stmt2_get_fields_fail                          | DOUBLE    |         | Count of failed stmt2 field fetches                      |
| taos_stmt2_free_fields_total                        | DOUBLE    |         | Count of stmt2 field releases                            |
| taos_stmt2_free_fields_success                      | DOUBLE    |         | Count of successful stmt2 field releases                 |
| taos_stmt_init_with_reqid_total                     | DOUBLE    |         | Count of stmt initializations with request ID            |
| taos_stmt_init_with_reqid_success                   | DOUBLE    |         | Count of successful stmt initializations with request ID |
| taos_stmt_init_with_reqid_fail                      | DOUBLE    |         | Count of failed stmt initializations with request ID     |
| taos_stmt_close_total                               | DOUBLE    |         | Count of stmt closes                                     |
| taos_stmt_close_success                             | DOUBLE    |         | Count of successful stmt closes                          |
| taos_stmt_close_fail                                | DOUBLE    |         | Count of failed stmt closes                              |
| taos_stmt_get_tag_fields_total                      | DOUBLE    |         | Count of stmt tag field fetches                          |
| taos_stmt_get_tag_fields_success                    | DOUBLE    |         | Count of successful stmt tag field fetches               |
| taos_stmt_get_tag_fields_fail                       | DOUBLE    |         | Count of failed stmt tag field fetches                   |
| taos_stmt_get_col_fields_total                      | DOUBLE    |         | Count of stmt column field fetches                       |
| taos_stmt_get_col_fields_success                    | DOUBLE    |         | Count of successful stmt column field fetches            |
| taos_stmt_get_col_fields_fail                       | DOUBLE    |         | Count of failed stmt column field fetches                |
| taos_stmt_reclaim_fields_total                      | DOUBLE    |         | Count of stmt field releases                             |
| taos_stmt_reclaim_fields_success                    | DOUBLE    |         | Count of successful stmt field releases                  |
| tmq_get_json_meta_total                             | DOUBLE    |         | Count of TMQ JSON metadata fetches                       |
| tmq_get_json_meta_success                           | DOUBLE    |         | Count of successful TMQ JSON metadata fetches            |
| tmq_free_json_meta_total                            | DOUBLE    |         | Count of TMQ JSON metadata releases                      |
| tmq_free_json_meta_success                          | DOUBLE    |         | Count of successful TMQ JSON metadata releases           |
| taos_fetch_whitelist_a_total                        | DOUBLE    |         | Count of async whitelist fetches                         |
| taos_fetch_whitelist_a_success                      | DOUBLE    |         | Count of successful async whitelist fetches              |
| taos_fetch_whitelist_a_callback_total               | DOUBLE    |         | Count of async whitelist callbacks                       |
| taos_fetch_whitelist_a_callback_success             | DOUBLE    |         | Count of successful async whitelist callbacks            |
| taos_fetch_whitelist_a_callback_fail                | DOUBLE    |         | Count of failed async whitelist callbacks                |
| taos_fetch_rows_a_total                             | DOUBLE    |         | Count of async row fetches                               |
| taos_fetch_rows_a_success                           | DOUBLE    |         | Count of successful async row fetches                    |
| taos_fetch_rows_a_callback_total                    | DOUBLE    |         | Count of async row callbacks                             |
| taos_fetch_rows_a_callback_success                  | DOUBLE    |         | Count of successful async row callbacks                  |
| taos_fetch_rows_a_callback_fail                     | DOUBLE    |         | Count of failed async row callbacks                      |
| taos_fetch_raw_block_a_total                        | DOUBLE    |         | Count of async raw block fetches                         |
| taos_fetch_raw_block_a_success                      | DOUBLE    |         | Count of successful async raw block fetches              |
| taos_fetch_raw_block_a_callback_total               | DOUBLE    |         | Count of async raw block callbacks                       |
| taos_fetch_raw_block_a_callback_success             | DOUBLE    |         | Count of successful async raw block callbacks            |
| taos_fetch_raw_block_a_callback_fail                | DOUBLE    |         | Count of failed async raw block callbacks                |
| tmq_get_raw_total                                   | DOUBLE    |         | Count of raw data fetches                                |
| tmq_get_raw_success                                 | DOUBLE    |         | Count of successful raw data fetches                     |
| tmq_get_raw_fail                                    | DOUBLE    |         | Count of failed raw data fetches                         |
| tmq_free_raw_total                                  | DOUBLE    |         | Count of raw data releases                               |
| tmq_free_raw_success                                | DOUBLE    |         | Count of successful raw data releases                    |
| tmq_consumer_new_total                              | DOUBLE    |         | Count of new consumer creations                          |
| tmq_consumer_new_success                            | DOUBLE    |         | Count of successful new consumer creations               |
| tmq_consumer_new_fail                               | DOUBLE    |         | Count of failed new consumer creations                   |
| tmq_consumer_close_total                            | DOUBLE    |         | Count of consumer closes                                 |
| tmq_consumer_close_success                          | DOUBLE    |         | Count of successful consumer closes                      |
| tmq_consumer_close_fail                             | DOUBLE    |         | Count of failed consumer closes                          |
| tmq_subscribe_total                                 | DOUBLE    |         | Count of topic subscriptions                             |
| tmq_subscribe_success                               | DOUBLE    |         | Count of successful topic subscriptions                  |
| tmq_subscribe_fail                                  | DOUBLE    |         | Count of failed topic subscriptions                      |
| tmq_unsubscribe_total                               | DOUBLE    |         | Count of unsubscriptions                                 |
| tmq_unsubscribe_success                             | DOUBLE    |         | Count of successful unsubscriptions                      |
| tmq_unsubscribe_fail                                | DOUBLE    |         | Count of failed unsubscriptions                          |
| tmq_list_new_total                                  | DOUBLE    |         | Count of new topic list creations                        |
| tmq_list_new_success                                | DOUBLE    |         | Count of successful new topic list creations             |
| tmq_list_new_fail                                   | DOUBLE    |         | Count of failed new topic list creations                 |
| tmq_list_destroy_total                              | DOUBLE    |         | Count of topic list destructions                         |
| tmq_list_destroy_success                            | DOUBLE    |         | Count of successful topic list destructions              |
| tmq_conf_new_total                                  | DOUBLE    |         | Count of TMQ new config creations                        |
| tmq_conf_new_success                                | DOUBLE    |         | Count of successful TMQ new config creations             |
| tmq_conf_new_fail                                   | DOUBLE    |         | Count of failed TMQ new config creations                 |
| tmq_conf_destroy_total                              | DOUBLE    |         | Count of TMQ config destructions                         |
| tmq_conf_destroy_success                            | DOUBLE    |         | Count of successful TMQ config destructions              |
| taos_stmt2_prepare_total                            | DOUBLE    |         | Count of stmt2 prepares                                  |
| taos_stmt2_prepare_success                          | DOUBLE    |         | Count of successful stmt2 prepares                       |
| taos_stmt2_prepare_fail                             | DOUBLE    |         | Count of failed stmt2 prepares                           |
| taos_stmt2_is_insert_total                          | DOUBLE    |         | Count of insert checks                                   |
| taos_stmt2_is_insert_success                        | DOUBLE    |         | Count of successful insert checks                        |
| taos_stmt2_is_insert_fail                           | DOUBLE    |         | Count of failed insert checks                            |
| taos_stmt2_bind_param_total                         | DOUBLE    |         | Count of stmt2 parameter bindings                        |
| taos_stmt2_bind_param_success                       | DOUBLE    |         | Count of successful stmt2 parameter bindings             |
| taos_stmt2_bind_param_fail                          | DOUBLE    |         | Count of failed stmt2 parameter bindings                 |
| taos_stmt2_exec_total                               | DOUBLE    |         | Count of stmt2 executions                                |
| taos_stmt2_exec_success                             | DOUBLE    |         | Count of successful stmt2 executions                     |
| taos_stmt2_exec_fail                                | DOUBLE    |         | Count of failed stmt2 executions                         |
| taos_stmt2_error_total                              | DOUBLE    |         | Count of stmt2 error checks                              |
| taos_stmt2_error_success                            | DOUBLE    |         | Count of successful stmt2 error checks                   |
| taos_fetch_row_total                                | DOUBLE    |         | Count of sync row fetches                                |
| taos_fetch_row_success                              | DOUBLE    |         | Count of successful sync row fetches                     |
| taos_is_update_query_total                          | DOUBLE    |         | Count of update statement checks                         |
| taos_is_update_query_success                        | DOUBLE    |         | Count of successful update statement checks              |
| taos_affected_rows_total                            | DOUBLE    |         | Count of SQL affected rows fetches                       |
| taos_affected_rows_success                          | DOUBLE    |         | Count of successful SQL affected rows fetches            |
| taos_num_fields_total                               | DOUBLE    |         | Count of field count fetches                             |
| taos_num_fields_success                             | DOUBLE    |         | Count of successful field count fetches                  |
| taos_fetch_fields_e_total                           | DOUBLE    |         | Count of extended field info fetches                     |
| taos_fetch_fields_e_success                         | DOUBLE    |         | Count of successful extended field info fetches          |
| taos_fetch_fields_e_fail                            | DOUBLE    |         | Count of failed extended field info fetches              |
| taos_result_precision_total                         | DOUBLE    |         | Count of precision fetches                               |
| taos_result_precision_success                       | DOUBLE    |         | Count of successful precision fetches                    |
| taos_get_raw_block_total                            | DOUBLE    |         | Count of raw block fetches                               |
| taos_get_raw_block_success                          | DOUBLE    |         | Count of successful raw block fetches                    |
| taos_fetch_raw_block_total                          | DOUBLE    |         | Count of raw block pulls                                 |
| taos_fetch_raw_block_success                        | DOUBLE    |         | Count of successful raw block pulls                      |
| taos_fetch_raw_block_fail                           | DOUBLE    |         | Count of failed raw block pulls                          |
| taos_fetch_lengths_total                            | DOUBLE    |         | Count of field length fetches                            |
| taos_fetch_lengths_success                          | DOUBLE    |         | Count of successful field length fetches                 |
| taos_write_raw_block_with_reqid_total               | DOUBLE    |         | Count of request ID raw block writes                     |
| taos_write_raw_block_with_reqid_success             | DOUBLE    |         | Count of successful request ID raw block writes          |
| taos_write_raw_block_with_reqid_fail                | DOUBLE    |         | Count of failed request ID raw block writes              |
| taos_write_raw_block_with_fields_with_reqid_total   | DOUBLE    |         | Count of request ID field raw block writes               |
| taos_write_raw_block_with_fields_with_reqid_success | DOUBLE    |         | Count of successful request ID field raw block writes    |
| taos_write_raw_block_with_fields_with_reqid_fail    | DOUBLE    |         | Count of failed request ID field raw block writes        |
| tmq_write_raw_total                                 | DOUBLE    |         | Count of TMQ raw data writes                             |
| tmq_write_raw_success                               | DOUBLE    |         | Count of successful TMQ raw data writes                  |
| tmq_write_raw_fail                                  | DOUBLE    |         | Count of failed TMQ raw data writes                      |
| taos_stmt_prepare_total                             | DOUBLE    |         | Count of stmt prepares                                   |
| taos_stmt_prepare_success                           | DOUBLE    |         | Count of successful stmt prepares                        |
| taos_stmt_prepare_fail                              | DOUBLE    |         | Count of failed stmt prepares                            |
| taos_stmt_is_insert_total                           | DOUBLE    |         | Count of stmt insert checks                              |
| taos_stmt_is_insert_success                         | DOUBLE    |         | Count of successful stmt insert checks                   |
| taos_stmt_is_insert_fail                            | DOUBLE    |         | Count of failed stmt insert checks                       |
| taos_stmt_set_tbname_total                          | DOUBLE    |         | Count of stmt table name sets                            |
| taos_stmt_set_tbname_success                        | DOUBLE    |         | Count of successful stmt table name sets                 |
| taos_stmt_set_tbname_fail                           | DOUBLE    |         | Count of failed stmt table name sets                     |
| taos_stmt_set_tags_total                            | DOUBLE    |         | Count of stmt tag sets                                   |
| taos_stmt_set_tags_success                          | DOUBLE    |         | Count of successful stmt tag sets                        |
| taos_stmt_set_tags_fail                             | DOUBLE    |         | Count of failed stmt tag sets                            |
| taos_stmt_bind_param_batch_total                    | DOUBLE    |         | Count of stmt batch parameter bindings                   |
| taos_stmt_bind_param_batch_success                  | DOUBLE    |         | Count of successful stmt batch parameter bindings        |
| taos_stmt_bind_param_batch_fail                     | DOUBLE    |         | Count of failed stmt batch parameter bindings            |
| taos_stmt_add_batch_total                           | DOUBLE    |         | Count of stmt batch additions                            |
| taos_stmt_add_batch_success                         | DOUBLE    |         | Count of successful stmt batch additions                 |
| taos_stmt_add_batch_fail                            | DOUBLE    |         | Count of failed stmt batch additions                     |
| taos_stmt_execute_total                             | DOUBLE    |         | Count of stmt executions                                 |
| taos_stmt_execute_success                           | DOUBLE    |         | Count of successful stmt executions                      |
| taos_stmt_execute_fail                              | DOUBLE    |         | Count of failed stmt executions                          |
| taos_stmt_num_params_total                          | DOUBLE    |         | Count of stmt parameter count fetches                    |
| taos_stmt_num_params_success                        | DOUBLE    |         | Count of successful stmt parameter count fetches         |
| taos_stmt_num_params_fail                           | DOUBLE    |         | Count of failed stmt parameter count fetches             |
| taos_stmt_get_param_total                           | DOUBLE    |         | Count of stmt parameter fetches                          |
| taos_stmt_get_param_success                         | DOUBLE    |         | Count of successful stmt parameter fetches               |
| taos_stmt_get_param_fail                            | DOUBLE    |         | Count of failed stmt parameter fetches                   |
| taos_stmt_errstr_total                              | DOUBLE    |         | Count of stmt error info fetches                         |
| taos_stmt_errstr_success                            | DOUBLE    |         | Count of successful stmt error info fetches              |
| taos_stmt_affected_rows_once_total                  | DOUBLE    |         | Count of stmt affected rows fetches                      |
| taos_stmt_affected_rows_once_success                | DOUBLE    |         | Count of successful stmt affected rows fetches           |
| taos_stmt_use_result_total                          | DOUBLE    |         | Count of stmt result set uses                            |
| taos_stmt_use_result_success                        | DOUBLE    |         | Count of successful stmt result set uses                 |
| taos_stmt_use_result_fail                           | DOUBLE    |         | Count of failed stmt result set uses                     |
| taos_select_db_total                                | DOUBLE    |         | Count of database selections                             |
| taos_select_db_success                              | DOUBLE    |         | Count of successful database selections                  |
| taos_select_db_fail                                 | DOUBLE    |         | Count of failed database selections                      |
| taos_get_tables_vgId_total                          | DOUBLE    |         | Count of table vgroup ID fetches                         |
| taos_get_tables_vgId_success                        | DOUBLE    |         | Count of successful table vgroup ID fetches              |
| taos_get_tables_vgId_fail                           | DOUBLE    |         | Count of failed table vgroup ID fetches                  |
| taos_options_connection_total                       | DOUBLE    |         | Count of connection option sets                          |
| taos_options_connection_success                     | DOUBLE    |         | Count of successful connection option sets               |
| taos_options_connection_fail                        | DOUBLE    |         | Count of failed connection option sets                   |
| taos_validate_sql_total                             | DOUBLE    |         | Count of SQL validations                                 |
| taos_validate_sql_success                           | DOUBLE    |         | Count of successful SQL validations                      |
| taos_validate_sql_fail                              | DOUBLE    |         | Count of failed SQL validations                          |
| taos_check_server_status_total                      | DOUBLE    |         | Count of server status checks                            |
| taos_check_server_status_success                    | DOUBLE    |         | Count of successful server status checks                 |
| taos_get_current_db_total                           | DOUBLE    |         | Count of current database fetches                        |
| taos_get_current_db_success                         | DOUBLE    |         | Count of successful current database fetches             |
| taos_get_current_db_fail                            | DOUBLE    |         | Count of failed current database fetches                 |
| taos_get_server_info_total                          | DOUBLE    |         | Count of server info fetches                             |
| taos_get_server_info_success                        | DOUBLE    |         | Count of successful server info fetches                  |
| taos_options_total                                  | DOUBLE    |         | Count of option sets                                     |
| taos_options_success                                | DOUBLE    |         | Count of successful option sets                          |
| taos_options_fail                                   | DOUBLE    |         | Count of failed option sets                              |
| taos_set_conn_mode_total                            | DOUBLE    |         | Count of connection mode sets                            |
| taos_set_conn_mode_success                          | DOUBLE    |         | Count of successful connection mode sets                 |
| taos_set_conn_mode_fail                             | DOUBLE    |         | Count of failed connection mode sets                     |
| taos_reset_current_db_total                         | DOUBLE    |         | Count of current database resets                         |
| taos_reset_current_db_success                       | DOUBLE    |         | Count of successful current database resets              |
| taos_set_notify_cb_total                            | DOUBLE    |         | Count of notification callback sets                      |
| taos_set_notify_cb_success                          | DOUBLE    |         | Count of successful notification callback sets           |
| taos_set_notify_cb_fail                             | DOUBLE    |         | Count of failed notification callback sets               |
| taos_errno_total                                    | DOUBLE    |         | Count of error code fetches                              |
| taos_errno_success                                  | DOUBLE    |         | Count of successful error code fetches                   |
| taos_errstr_total                                   | DOUBLE    |         | Count of error message fetches                           |
| taos_errstr_success                                 | DOUBLE    |         | Count of successful error message fetches                |
| tmq_consumer_poll_total                             | DOUBLE    |         | Count of TMQ consumer polls                              |
| tmq_consumer_poll_success                           | DOUBLE    |         | Count of successful TMQ consumer polls                   |
| tmq_consumer_poll_fail                              | DOUBLE    |         | Count of failed TMQ consumer polls                       |
| tmq_subscription_total                              | DOUBLE    |         | Count of TMQ subscription info fetches                   |
| tmq_subscription_success                            | DOUBLE    |         | Count of successful TMQ subscription info fetches        |
| tmq_subscription_fail                               | DOUBLE    |         | Count of failed TMQ subscription info fetches            |
| tmq_list_append_total                               | DOUBLE    |         | Count of TMQ list appends                                |
| tmq_list_append_success                             | DOUBLE    |         | Count of successful TMQ list appends                     |
| tmq_list_append_fail                                | DOUBLE    |         | Count of failed TMQ list appends                         |
| tmq_list_get_size_total                             | DOUBLE    |         | Count of TMQ list size fetches                           |
| tmq_list_get_size_success                           | DOUBLE    |         | Count of successful TMQ list size fetches                |
| tmq_err2str_total                                   | DOUBLE    |         | Count of TMQ error code to string conversions            |
| tmq_err2str_success                                 | DOUBLE    |         | Count of successful TMQ error code to string conversions |
| tmq_conf_set_total                                  | DOUBLE    |         | Count of TMQ config sets                                 |
| tmq_conf_set_success                                | DOUBLE    |         | Count of successful TMQ config sets                      |
| tmq_conf_set_fail                                   | DOUBLE    |         | Count of failed TMQ config sets                          |
| tmq_get_res_type_total                              | DOUBLE    |         | Count of TMQ resource type fetches                       |
| tmq_get_res_type_success                            | DOUBLE    |         | Count of successful TMQ resource type fetches            |
| tmq_get_topic_name_total                            | DOUBLE    |         | Count of TMQ topic name fetches                          |
| tmq_get_topic_name_success                          | DOUBLE    |         | Count of successful TMQ topic name fetches               |
| tmq_get_vgroup_id_total                             | DOUBLE    |         | Count of TMQ vgroup ID fetches                           |
| tmq_get_vgroup_id_success                           | DOUBLE    |         | Count of successful TMQ vgroup ID fetches                |
| tmq_get_vgroup_offset_total                         | DOUBLE    |         | Count of TMQ vgroup offset fetches                       |
| tmq_get_vgroup_offset_success                       | DOUBLE    |         | Count of successful TMQ vgroup offset fetches            |
| tmq_get_db_name_total                               | DOUBLE    |         | Count of TMQ database name fetches                       |
| tmq_get_db_name_success                             | DOUBLE    |         | Count of successful TMQ database name fetches            |
| tmq_get_table_name_total                            | DOUBLE    |         | Count of TMQ table name fetches                          |
| tmq_get_table_name_success                          | DOUBLE    |         | Count of successful TMQ table name fetches               |
| tmq_get_connect_total                               | DOUBLE    |         | Count of TMQ connection fetches                          |
| tmq_get_connect_success                             | DOUBLE    |         | Count of successful TMQ connection fetches               |
| tmq_commit_sync_total                               | DOUBLE    |         | Count of TMQ sync commits                                |
| tmq_commit_sync_success                             | DOUBLE    |         | Count of successful TMQ sync commits                     |
| tmq_commit_sync_fail                                | DOUBLE    |         | Count of failed TMQ sync commits                         |
| tmq_fetch_raw_block_total                           | DOUBLE    |         | Count of TMQ raw block fetches                           |
| tmq_fetch_raw_block_success                         | DOUBLE    |         | Count of successful TMQ raw block fetches                |
| tmq_fetch_raw_block_fail                            | DOUBLE    |         | Count of failed TMQ raw block fetches                    |
| tmq_get_topic_assignment_total                      | DOUBLE    |         | Count of TMQ topic assignment fetches                    |
| tmq_get_topic_assignment_success                    | DOUBLE    |         | Count of successful TMQ topic assignment fetches         |
| tmq_get_topic_assignment_fail                       | DOUBLE    |         | Count of failed TMQ topic assignment fetches             |
| tmq_offset_seek_total                               | DOUBLE    |         | Count of TMQ offset seeks                                |
| tmq_offset_seek_success                             | DOUBLE    |         | Count of successful TMQ offset seeks                     |
| tmq_offset_seek_fail                                | DOUBLE    |         | Count of failed TMQ offset seeks                         |
| tmq_committed_total                                 | DOUBLE    |         | Count of TMQ committed offset fetches                    |
| tmq_committed_success                               | DOUBLE    |         | Count of successful TMQ committed offset fetches         |
| tmq_commit_offset_sync_fail                         | DOUBLE    |         | Count of failed TMQ sync offset commits                  |
| tmq_position_total                                  | DOUBLE    |         | Count of TMQ current position fetches                    |
| tmq_position_success                                | DOUBLE    |         | Count of successful TMQ current position fetches         |
| tmq_commit_offset_sync_total                        | DOUBLE    |         | Count of TMQ sync offset commits                         |
| tmq_commit_offset_sync_success                      | DOUBLE    |         | Count of successful TMQ sync offset commits              |
| endpoint                                            | NCHAR     | TAG     | Request endpoint                                         |

</details>

## Changes after upgrading httpd to taosAdapter

In TDengine server version 2.2.x.x or earlier, the taosd process included an embedded HTTP service(httpd). As mentioned earlier, taosAdapter is a standalone software managed by systemd, having its own process. Moreover, there are some differences in configuration parameters and behaviors between the two, as shown in the table below:

| **#** | **embedded httpd**  | **taosAdapter**                                            | **comment**                                                                                                                                                                                                                                        |
|-------|---------------------|------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 1     | httpEnableRecordSql | --logLevel=debug                                           |                                                                                                                                                                                                                                                    |
| 2     | httpMaxThreads      | n/a                                                        | taosAdapter automatically manages the thread pool, this parameter is not needed                                                                                                                                                                    |
| 3     | telegrafUseFieldNum | Please refer to taosAdapter telegraf configuration methods |                                                                                                                                                                                                                                                    |
| 4     | restfulRowLimit     | restfulRowLimit                                            | The embedded httpd defaults to outputting 10240 rows of data, with a maximum allowable value of 102400. taosAdapter also provides restfulRowLimit but does not impose a limit by default. You can configure it according to actual scenario needs. |
| 5     | httpDebugFlag       | Not applicable                                             | httpdDebugFlag does not affect taosAdapter                                                                                                                                                                                                         |
| 6     | httpDBNameMandatory | Not applicable                                             | taosAdapter requires the database name to be specified in the URL                                                                                                                                                                                  |
