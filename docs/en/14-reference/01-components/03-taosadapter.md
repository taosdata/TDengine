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
- Compatible with InfluxDB v1 write interface:
  [https://docs.influxdata.com/influxdb/v2.0/reference/api/influxdb-1x/write/](https://docs.influxdata.com/influxdb/v2.0/reference/api/influxdb-1x/write/)
- Compatible with OpenTSDB JSON and telnet format writing:
  - [http://opentsdb.net/docs/build/html/api_http/put.html](http://opentsdb.net/docs/build/html/api_http/put.html)
  - [http://opentsdb.net/docs/build/html/api_telnet/put.html](http://opentsdb.net/docs/build/html/api_telnet/put.html)
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

### Compatible with InfluxDB v1 write interface

You can use any client that supports the HTTP protocol to write data in InfluxDB compatible format to TDengine by accessing the Restful interface URL `http://<fqdn>:6041/influxdb/v1/write`.

Supported InfluxDB parameters are as follows:

- `db` specifies the database name used by TDengine
- `precision` the time precision used by TDengine
- `u` TDengine username
- `p` TDengine password
- `ttl` the lifespan of automatically created subtables, determined by the TTL parameter of the first data entry in the subtable, which cannot be updated. For more information, please refer to the TTL parameter in the [table creation document](../../sql-manual/manage-tables/).

Note: Currently, InfluxDB's token authentication method is not supported, only Basic authentication and query parameter verification are supported.
Example: `curl --request POST http://127.0.0.1:6041/influxdb/v1/write?db=test --user "root:taosdata" --data-binary "measurement,host=host1 field1=2i,field2=2.0 1577836800000000000"`

### Compatible with OpenTSDB JSON and telnet format writing

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

taosAdapter supports configuration through command-line parameters, environment variables, and configuration files. The default configuration file is `/etc/taos/taosadapter.toml`.

Command-line parameters take precedence over environment variables, which take precedence over configuration files. The command-line usage is arg=val, such as taosadapter -p=30000 --debug=true, detailed list as follows:

```shell
Usage of taosAdapter:
      --collectd.db string                           collectd db name. Env "TAOS_ADAPTER_COLLECTD_DB" (default "collectd")
      --collectd.enable                              enable collectd. Env "TAOS_ADAPTER_COLLECTD_ENABLE" (default true)
      --collectd.password string                     collectd password. Env "TAOS_ADAPTER_COLLECTD_PASSWORD" (default "taosdata")
      --collectd.port int                            collectd server port. Env "TAOS_ADAPTER_COLLECTD_PORT" (default 6045)
      --collectd.ttl int                             collectd data ttl. Env "TAOS_ADAPTER_COLLECTD_TTL"
      --collectd.user string                         collectd user. Env "TAOS_ADAPTER_COLLECTD_USER" (default "root")
      --collectd.worker int                          collectd write worker. Env "TAOS_ADAPTER_COLLECTD_WORKER" (default 10)
  -c, --config string                                config path default /etc/taos/taosadapter.toml
      --cors.allowAllOrigins                         cors allow all origins. Env "TAOS_ADAPTER_CORS_ALLOW_ALL_ORIGINS" (default true)
      --cors.allowCredentials                        cors allow credentials. Env "TAOS_ADAPTER_CORS_ALLOW_Credentials"
      --cors.allowHeaders stringArray                cors allow HEADERS. Env "TAOS_ADAPTER_ALLOW_HEADERS"
      --cors.allowOrigins stringArray                cors allow origins. Env "TAOS_ADAPTER_ALLOW_ORIGINS"
      --cors.allowWebSockets                         cors allow WebSockets. Env "TAOS_ADAPTER_CORS_ALLOW_WebSockets"
      --cors.exposeHeaders stringArray               cors expose headers. Env "TAOS_ADAPTER_Expose_Headers"
      --debug                                        enable debug mode. Env "TAOS_ADAPTER_DEBUG" (default true)
      --help                                         Print this help message and exit
      --httpCodeServerError                          Use a non-200 http status code when server returns an error. Env "TAOS_ADAPTER_HTTP_CODE_SERVER_ERROR"
      --influxdb.enable                              enable influxdb. Env "TAOS_ADAPTER_INFLUXDB_ENABLE" (default true)
      --instanceId int                               instance ID. Env "TAOS_ADAPTER_INSTANCE_ID" (default 32)
      --log.compress                                 whether to compress old log. Env "TAOS_ADAPTER_LOG_COMPRESS"
      --log.enableRecordHttpSql                      whether to record http sql. Env "TAOS_ADAPTER_LOG_ENABLE_RECORD_HTTP_SQL"
      --log.keepDays uint                            log retention days, must be a positive integer. Env "TAOS_ADAPTER_LOG_KEEP_DAYS" (default 30)
      --log.level string                             log level (trace debug info warning error). Env "TAOS_ADAPTER_LOG_LEVEL" (default "info")
      --log.path string                              log path. Env "TAOS_ADAPTER_LOG_PATH" (default "/var/log/taos")
      --log.reservedDiskSize string                  reserved disk size for log dir (KB MB GB), must be a positive integer. Env "TAOS_ADAPTER_LOG_RESERVED_DISK_SIZE" (default "1GB")
      --log.rotationCount uint                       log rotation count. Env "TAOS_ADAPTER_LOG_ROTATION_COUNT" (default 30)
      --log.rotationSize string                      log rotation size(KB MB GB), must be a positive integer. Env "TAOS_ADAPTER_LOG_ROTATION_SIZE" (default "1GB")
      --log.rotationTime duration                    deprecated: log rotation time always 24 hours. Env "TAOS_ADAPTER_LOG_ROTATION_TIME" (default 24h0m0s)
      --log.sqlRotationCount uint                    record sql log rotation count. Env "TAOS_ADAPTER_LOG_SQL_ROTATION_COUNT" (default 2)
      --log.sqlRotationSize string                   record sql log rotation size(KB MB GB), must be a positive integer. Env "TAOS_ADAPTER_LOG_SQL_ROTATION_SIZE" (default "1GB")
      --log.sqlRotationTime duration                 record sql log rotation time. Env "TAOS_ADAPTER_LOG_SQL_ROTATION_TIME" (default 24h0m0s)
      --logLevel string                              log level (trace debug info warning error). Env "TAOS_ADAPTER_LOG_LEVEL" (default "info")
      --maxAsyncConcurrentLimit int                  The maximum number of concurrent calls allowed for the C asynchronous method. 0 means use CPU core count. Env "TAOS_ADAPTER_MAX_ASYNC_CONCURRENT_LIMIT"
      --maxSyncConcurrentLimit int                   The maximum number of concurrent calls allowed for the C synchronized method. 0 means use CPU core count. Env "TAOS_ADAPTER_MAX_SYNC_CONCURRENT_LIMIT"
      --monitor.collectDuration duration             Set monitor duration. Env "TAOS_ADAPTER_MONITOR_COLLECT_DURATION" (default 3s)
      --monitor.disable                              Whether to disable monitoring. Env "TAOS_ADAPTER_MONITOR_DISABLE" (default true)
      --monitor.identity string                      The identity of the current instance, or 'hostname:port' if it is empty. Env "TAOS_ADAPTER_MONITOR_IDENTITY"
      --monitor.incgroup                             Whether running in cgroup. Env "TAOS_ADAPTER_MONITOR_INCGROUP"
      --monitor.pauseAllMemoryThreshold float        Memory percentage threshold for pause all. Env "TAOS_ADAPTER_MONITOR_PAUSE_ALL_MEMORY_THRESHOLD" (default 80)
      --monitor.pauseQueryMemoryThreshold float      Memory percentage threshold for pause query. Env "TAOS_ADAPTER_MONITOR_PAUSE_QUERY_MEMORY_THRESHOLD" (default 70)
      --node_exporter.caCertFile string              node_exporter ca cert file path. Env "TAOS_ADAPTER_NODE_EXPORTER_CA_CERT_FILE"
      --node_exporter.certFile string                node_exporter cert file path. Env "TAOS_ADAPTER_NODE_EXPORTER_CERT_FILE"
      --node_exporter.db string                      node_exporter db name. Env "TAOS_ADAPTER_NODE_EXPORTER_DB" (default "node_exporter")
      --node_exporter.enable                         enable node_exporter. Env "TAOS_ADAPTER_NODE_EXPORTER_ENABLE"
      --node_exporter.gatherDuration duration        node_exporter gather duration. Env "TAOS_ADAPTER_NODE_EXPORTER_GATHER_DURATION" (default 5s)
      --node_exporter.httpBearerTokenString string   node_exporter http bearer token. Env "TAOS_ADAPTER_NODE_EXPORTER_HTTP_BEARER_TOKEN_STRING"
      --node_exporter.httpPassword string            node_exporter http password. Env "TAOS_ADAPTER_NODE_EXPORTER_HTTP_PASSWORD"
      --node_exporter.httpUsername string            node_exporter http username. Env "TAOS_ADAPTER_NODE_EXPORTER_HTTP_USERNAME"
      --node_exporter.insecureSkipVerify             node_exporter skip ssl check. Env "TAOS_ADAPTER_NODE_EXPORTER_INSECURE_SKIP_VERIFY" (default true)
      --node_exporter.keyFile string                 node_exporter cert key file path. Env "TAOS_ADAPTER_NODE_EXPORTER_KEY_FILE"
      --node_exporter.password string                node_exporter password. Env "TAOS_ADAPTER_NODE_EXPORTER_PASSWORD" (default "taosdata")
      --node_exporter.responseTimeout duration       node_exporter response timeout. Env "TAOS_ADAPTER_NODE_EXPORTER_RESPONSE_TIMEOUT" (default 5s)
      --node_exporter.ttl int                        node_exporter data ttl. Env "TAOS_ADAPTER_NODE_EXPORTER_TTL"
      --node_exporter.urls strings                   node_exporter urls. Env "TAOS_ADAPTER_NODE_EXPORTER_URLS" (default [http://localhost:9100])
      --node_exporter.user string                    node_exporter user. Env "TAOS_ADAPTER_NODE_EXPORTER_USER" (default "root")
      --opentsdb.enable                              enable opentsdb. Env "TAOS_ADAPTER_OPENTSDB_ENABLE" (default true)
      --opentsdb_telnet.batchSize int                opentsdb_telnet batch size. Env "TAOS_ADAPTER_OPENTSDB_TELNET_BATCH_SIZE" (default 1)
      --opentsdb_telnet.dbs strings                  opentsdb_telnet db names. Env "TAOS_ADAPTER_OPENTSDB_TELNET_DBS" (default [opentsdb_telnet,collectd_tsdb,icinga2_tsdb,tcollector_tsdb])
      --opentsdb_telnet.enable                       enable opentsdb telnet,warning: without auth info(default false). Env "TAOS_ADAPTER_OPENTSDB_TELNET_ENABLE"
      --opentsdb_telnet.flushInterval duration       opentsdb_telnet flush interval (0s means not valid) . Env "TAOS_ADAPTER_OPENTSDB_TELNET_FLUSH_INTERVAL"
      --opentsdb_telnet.maxTCPConnections int        max tcp connections. Env "TAOS_ADAPTER_OPENTSDB_TELNET_MAX_TCP_CONNECTIONS" (default 250)
      --opentsdb_telnet.password string              opentsdb_telnet password. Env "TAOS_ADAPTER_OPENTSDB_TELNET_PASSWORD" (default "taosdata")
      --opentsdb_telnet.ports ints                   opentsdb telnet tcp port. Env "TAOS_ADAPTER_OPENTSDB_TELNET_PORTS" (default [6046,6047,6048,6049])
      --opentsdb_telnet.tcpKeepAlive                 enable tcp keep alive. Env "TAOS_ADAPTER_OPENTSDB_TELNET_TCP_KEEP_ALIVE"
      --opentsdb_telnet.ttl int                      opentsdb_telnet data ttl. Env "TAOS_ADAPTER_OPENTSDB_TELNET_TTL"
      --opentsdb_telnet.user string                  opentsdb_telnet user. Env "TAOS_ADAPTER_OPENTSDB_TELNET_USER" (default "root")
      --pool.idleTimeout duration                    Set idle connection timeout. Env "TAOS_ADAPTER_POOL_IDLE_TIMEOUT"
      --pool.maxConnect int                          max connections to server. Env "TAOS_ADAPTER_POOL_MAX_CONNECT"
      --pool.maxIdle int                             max idle connections to server. Env "TAOS_ADAPTER_POOL_MAX_IDLE"
      --pool.maxWait int                             max count of waiting for connection. Env "TAOS_ADAPTER_POOL_MAX_WAIT"
      --pool.waitTimeout int                         wait for connection timeout seconds. Env "TAOS_ADAPTER_POOL_WAIT_TIMEOUT" (default 60)
  -P, --port int                                     http port. Env "TAOS_ADAPTER_PORT" (default 6041)
      --prometheus.enable                            enable prometheus. Env "TAOS_ADAPTER_PROMETHEUS_ENABLE" (default true)
      --restfulRowLimit int                          restful returns the maximum number of rows (-1 means no limit). Env "TAOS_ADAPTER_RESTFUL_ROW_LIMIT" (default -1)
      --smlAutoCreateDB                              Whether to automatically create db when writing with schemaless. Env "TAOS_ADAPTER_SML_AUTO_CREATE_DB"
      --ssl.certFile string                          ssl cert file path. Env "TAOS_ADAPTER_SSL_CERT_FILE"
      --ssl.enable                                   enable ssl. Env "TAOS_ADAPTER_SSL_ENABLE"
      --ssl.keyFile string                           ssl key file path. Env "TAOS_ADAPTER_SSL_KEY_FILE"
      --statsd.allowPendingMessages int              statsd allow pending messages. Env "TAOS_ADAPTER_STATSD_ALLOW_PENDING_MESSAGES" (default 50000)
      --statsd.db string                             statsd db name. Env "TAOS_ADAPTER_STATSD_DB" (default "statsd")
      --statsd.deleteCounters                        statsd delete counter cache after gather. Env "TAOS_ADAPTER_STATSD_DELETE_COUNTERS" (default true)
      --statsd.deleteGauges                          statsd delete gauge cache after gather. Env "TAOS_ADAPTER_STATSD_DELETE_GAUGES" (default true)
      --statsd.deleteSets                            statsd delete set cache after gather. Env "TAOS_ADAPTER_STATSD_DELETE_SETS" (default true)
      --statsd.deleteTimings                         statsd delete timing cache after gather. Env "TAOS_ADAPTER_STATSD_DELETE_TIMINGS" (default true)
      --statsd.enable                                enable statsd. Env "TAOS_ADAPTER_STATSD_ENABLE"
      --statsd.gatherInterval duration               statsd gather interval. Env "TAOS_ADAPTER_STATSD_GATHER_INTERVAL" (default 5s)
      --statsd.maxTCPConnections int                 statsd max tcp connections. Env "TAOS_ADAPTER_STATSD_MAX_TCP_CONNECTIONS" (default 250)
      --statsd.password string                       statsd password. Env "TAOS_ADAPTER_STATSD_PASSWORD" (default "taosdata")
      --statsd.port int                              statsd server port. Env "TAOS_ADAPTER_STATSD_PORT" (default 6044)
      --statsd.protocol string                       statsd protocol [tcp or udp]. Env "TAOS_ADAPTER_STATSD_PROTOCOL" (default "udp4")
      --statsd.tcpKeepAlive                          enable tcp keep alive. Env "TAOS_ADAPTER_STATSD_TCP_KEEP_ALIVE"
      --statsd.ttl int                               statsd data ttl. Env "TAOS_ADAPTER_STATSD_TTL"
      --statsd.user string                           statsd user. Env "TAOS_ADAPTER_STATSD_USER" (default "root")
      --statsd.worker int                            statsd write worker. Env "TAOS_ADAPTER_STATSD_WORKER" (default 10)
      --taosConfigDir string                         load taos client config path. Env "TAOS_ADAPTER_TAOS_CONFIG_FILE"
      --uploadKeeper.enable                          Whether to enable sending metrics to keeper. Env "TAOS_ADAPTER_UPLOAD_KEEPER_ENABLE" (default true)
      --uploadKeeper.interval duration               send to Keeper interval. Env "TAOS_ADAPTER_UPLOAD_KEEPER_INTERVAL" (default 15s)
      --uploadKeeper.retryInterval duration          retry interval. Env "TAOS_ADAPTER_UPLOAD_KEEPER_RETRY_INTERVAL" (default 5s)
      --uploadKeeper.retryTimes uint                 retry times. Env "TAOS_ADAPTER_UPLOAD_KEEPER_RETRY_TIMES" (default 3)
      --uploadKeeper.timeout duration                send to Keeper timeout. Env "TAOS_ADAPTER_UPLOAD_KEEPER_TIMEOUT" (default 5s)
      --uploadKeeper.url string                      Keeper url. Env "TAOS_ADAPTER_UPLOAD_KEEPER_URL" (default "http://127.0.0.1:6043/adapter_report")
  -V, --version                                      Print the version and exit
```

See the example configuration file at [example/config/taosadapter.toml](https://github.com/taosdata/taosadapter/blob/3.0/example/config/taosadapter.toml).

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

* RESTful API requests
* WebSocket API requests
* InfluxDB v1 write interface
* OpenTSDB HTTP write interface

For details about the CORS protocol, please refer to: [https://www.w3.org/wiki/CORS_Enabled](https://www.w3.org/wiki/CORS_Enabled) or [https://developer.mozilla.org/docs/Web/HTTP/CORS](https://developer.mozilla.org/docs/Web/HTTP/CORS).

### Connection Pool Configuration

taosAdapter uses a connection pool to manage connections to TDengine, improving concurrency performance and resource utilization. The connection pool configuration applies to the following interfaces, and these interfaces share a single connection pool:

* RESTful API requests
* InfluxDB v1 write interface
* OpenTSDB JSON and telnet format writing
* Telegraf data writing
* collectd data writing
* StatsD data writing
* node_exporter data collection writing
* Prometheus remote_read and remote_write

The configuration parameters for the connection pool are as follows:

- **`pool.maxConnect`**: The maximum number of connections allowed in the pool, default is twice the number of CPU cores. It is recommended to keep the default setting.
- **`pool.maxIdle`**: The maximum number of idle connections in the pool, default is the same as `pool.maxConnect`. It is recommended to keep the default setting.
- **`pool.idleTimeout`**: Connection idle timeout, default is never timeout. It is recommended to keep the default setting.
- **`pool.waitTimeout`**: Timeout for obtaining a connection from the pool, default is set to 60 seconds. If a connection is not obtained within the timeout period, HTTP status code 503 will be returned. This parameter is available starting from version 3.3.3.0.
- **`pool.maxWait`**: The maximum number of requests waiting to get a connection in the pool, default is 0, which means no limit. When the number of queued requests exceeds this value, new requests will return HTTP status code 503. This parameter is available starting from version 3.3.3.0.

### HTTP Response Code Configuration

taosAdapter uses the parameter `httpCodeServerError` to set whether to return a non-200 HTTP status code when the C interface returns an error. When set to true, it will return different HTTP status codes based on the error code returned by C. See [HTTP Response Codes](../../client-libraries/rest-api/) for details.

This configuration only affects the **RESTful interface**.

**Parameter Description**

- **`httpCodeServerError`**:
  - **When set to `true`**: Map the error code returned by the C interface to the corresponding HTTP status code.
  - **When set to `false`**: Regardless of the error returned by the C interface, always return the HTTP status code `200` (default value).

### Memory limit configuration

taosAdapter will monitor the memory usage during its operation and adjust it through two thresholds. The valid value range is an integer from 1 to 100, and the unit is the percentage of system physical memory.

This configuration only affects the following interfaces:

* RESTful interface request
* InfluxDB v1 write interface
* OpenTSDB HTTP write interface
* Prometheus remote_read and remote_write interfaces

**Parameter Description**

- **`pauseQueryMemoryThreshold`**:
  - When memory usage exceeds this threshold, taosAdapter will stop processing query requests.
  - Default value: `70` (i.e. 70% of system physical memory).
- **`pauseAllMemoryThreshold`**:
  - When memory usage exceeds this threshold, taosAdapter will stop processing all requests (including writes and queries).
  - Default value: `80` (i.e. 80% of system physical memory).

When memory usage falls below the threshold, taosAdapter will automatically resume the corresponding function.

**HTTP return content:**

- **When `pauseQueryMemoryThreshold` is exceeded**:
  - HTTP status code: `503`
  - Return content: `"query memory exceeds threshold"`

- **When `pauseAllMemoryThreshold` is exceeded**:
  - HTTP status code: `503`
  - Return content: `"memory exceeds threshold"`

**Status check interface:**

The memory status of taosAdapter can be checked through the following interface:
- **Normal status**: `http://<fqdn>:6041/-/ping` returns `code 200`.
- **Memory exceeds threshold**:
  - If the memory exceeds `pauseAllMemoryThreshold`, `code 503` is returned.
  - If the memory exceeds `pauseQueryMemoryThreshold` and the request parameter contains `action=query`, `code 503` is returned.

**Related configuration parameters:**

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

**Parameter Description**

- **`smlAutoCreateDB`**:
  - **When set to `true`**: When writing to the schemaless protocol, if the target database does not exist, taosAdapter will automatically create the database.
  - **When set to `false`**: The user needs to manually create the database, otherwise the write will fail (default value).

### Number of results returned configuration

taosAdapter provides the parameter `restfulRowLimit` to control the number of results returned by the HTTP interface.

The `restfulRowLimit` parameter only affects the return results of the following interfaces:

- RESTful interface
- Prometheus remote_read interface

**Parameter Description**

- **`restfulRowLimit`**:
  - **When set to a positive integer**: The number of results returned by the interface will not exceed this value.
  - **When set to `-1`**: The number of results returned by the interface is unlimited (default value).

### Log configuration

1. You can set the taosAdapter log output detail level by setting the --log.level parameter or the environment variable TAOS_ADAPTER_LOG_LEVEL. Valid values ​​include: panic, fatal, error, warn, warning, info, debug, and trace.
2. Starting from **3.3.5.0 version**, taosAdapter supports dynamic modification of log level through HTTP interface. Users can dynamically adjust the log level by sending HTTP PUT request to /config interface. The authentication method of this interface is the same as /rest/sql interface, and the configuration item key-value pair in JSON format must be passed in the request body.

The following is an example of setting the log level to debug through the curl command:

```shell
curl --location --request PUT 'http://127.0.0.1:6041/config' \
-u root:taosdata \
--data '{"log.level": "debug"}'
```

## Service Management

### Starting/Stopping taosAdapter

On Linux systems, the taosAdapter service is managed by default by systemd. Use the command `systemctl start taosadapter` to start the taosAdapter service. Use the command `systemctl stop taosadapter` to stop the taosAdapter service.

### Upgrading taosAdapter

taosAdapter and TDengine server need to use the same version. Please upgrade taosAdapter by upgrading the TDengine server.
taosAdapter deployed separately from taosd must be upgraded by upgrading the TDengine server on its server.

### Removing taosAdapter

Use the command rmtaos to remove the TDengine server software, including taosAdapter.

## IPv6 Support

Starting from **version 3.3.7.0**, taosAdapter supports IPv6. No additional configuration is required.
taosAdapter automatically detects the system's IPv6 support: when available, it enables IPv6 and simultaneously listens on both IPv4 and IPv6 addresses.

## Monitoring Metrics

Currently, taosAdapter only collects monitoring indicators for RESTful/WebSocket related requests. There are no monitoring indicators for other interfaces.

taosAdapter reports monitoring indicators to taosKeeper, which will be written to the monitoring database by taosKeeper. The default is the `log` database, which can be modified in the taoskeeper configuration file. The following is a detailed introduction to these monitoring indicators.

The `adapter_requests` table records taosAdapter monitoring data, and the fields are as follows:

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

The `adapter_status` table records the status data of taosAdapter, and the fields are as follows:

| field            | type      | is\_tag | comment                                                       |
|:-----------------|:----------|:--------|:--------------------------------------------------------------|
| _ts              | TIMESTAMP |         | data collection timestamp                                     |
| go_heap_sys      | DOUBLE    |         | heap memory allocated by Go runtime (bytes)                   |
| go_heap_inuse    | DOUBLE    |         | heap memory in use by Go runtime (bytes)                      |
| go_stack_sys     | DOUBLE    |         | stack memory allocated by Go runtime (bytes)                  |
| go_stack_inuse   | DOUBLE    |         | stack memory in use by Go runtime (bytes)                     |
| rss              | DOUBLE    |         | actual physical memory occupied by the process (bytes)        |
| ws_query_conn    | DOUBLE    |         | current WebSocket connections for `/rest/ws` endpoint         |
| ws_stmt_conn     | DOUBLE    |         | current WebSocket connections for `/rest/stmt` endpoint       |
| ws_sml_conn      | DOUBLE    |         | current WebSocket connections for `/rest/schemaless` endpoint |
| ws_ws_conn       | DOUBLE    |         | current WebSocket connections for `/ws` endpoint              |
| ws_tmq_conn      | DOUBLE    |         | current WebSocket connections for `/rest/tmq` endpoint        |
| async_c_limit    | DOUBLE    |         | total concurrency limit for the C asynchronous interface      |
| async_c_inflight | DOUBLE    |         | current concurrency for the C asynchronous interface          |
| sync_c_limit     | DOUBLE    |         | total concurrency limit for the C synchronous interface       |
| sync_c_inflight  | DOUBLE    |         | current concurrency for the C synchronous interface           |
| endpoint         | NCHAR     | TAG     | request endpoint                                              |

The `adapter_conn_pool` table records the connection pool monitoring data of taosAdapter, and the fields are as follows:

| field            | type      | is\_tag | comment                                                     |
|:-----------------|:----------|:--------|:------------------------------------------------------------|
| _ts              | TIMESTAMP |         | data collection timestamp                                   |
| conn_pool_total  | DOUBLE    |         | maximum connection limit for the connection pool            |
| conn_pool_in_use | DOUBLE    |         | current number of connections in use in the connection pool |
| endpoint         | NCHAR     | TAG     | request endpoint                                            |
| user             | NCHAR     | TAG     | username to which the connection pool belongs               |

## Changes after upgrading httpd to taosAdapter

In TDengine server version 2.2.x.x or earlier, the taosd process included an embedded HTTP service(httpd). As mentioned earlier, taosAdapter is a standalone software managed by systemd, having its own process. Moreover, there are some differences in configuration parameters and behaviors between the two, as shown in the table below:

| **#** | **embedded httpd**  | **taosAdapter**                                            | **comment**                                                  |
| ----- | ------------------- | ---------------------------------------------------------- | ------------------------------------------------------------ |
| 1     | httpEnableRecordSql | --logLevel=debug                                           |                                                              |
| 2     | httpMaxThreads      | n/a                                                        | taosAdapter automatically manages the thread pool, this parameter is not needed |
| 3     | telegrafUseFieldNum | Please refer to taosAdapter telegraf configuration methods |                                                              |
| 4     | restfulRowLimit     | restfulRowLimit                                            | The embedded httpd defaults to outputting 10240 rows of data, with a maximum allowable value of 102400. taosAdapter also provides restfulRowLimit but does not impose a limit by default. You can configure it according to actual scenario needs. |
| 5     | httpDebugFlag       | Not applicable                                             | httpdDebugFlag does not affect taosAdapter                   |
| 6     | httpDBNameMandatory | Not applicable                                             | taosAdapter requires the database name to be specified in the URL |
