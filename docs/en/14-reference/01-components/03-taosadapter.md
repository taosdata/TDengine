---
title: taosAdapter Reference
sidebar_label: taosAdapter
slug: /tdengine-reference/components/taosadapter
---

import Image from '@theme/IdealImage';
import imgAdapter from '../../assets/taosadapter-01.png';
import Prometheus from "../../10-third-party/01-collection/_prometheus.mdx"
import CollectD from "../../10-third-party/01-collection/_collectd.mdx"
import StatsD from "../../10-third-party/01-collection/_statsd.mdx"
import Icinga2 from "../../10-third-party/01-collection/_icinga2.mdx"
import TCollector from "../../10-third-party/01-collection/_tcollector.mdx"

taosAdapter is a supporting tool for TDengine, acting as a bridge and adapter between the TDengine cluster and applications. It provides an easy-to-use and efficient way to ingest data directly from data collection agents such as Telegraf, StatsD, collectd, etc. It also offers InfluxDB/OpenTSDB-compatible data ingestion interfaces, allowing InfluxDB/OpenTSDB applications to be seamlessly ported to TDengine.

taosAdapter offers the following features:

- RESTful interface
- InfluxDB v1 write interface compatibility
- OpenTSDB JSON and telnet format writing compatibility
- Seamless connection to Telegraf
- Seamless connection to collectd
- Seamless connection to StatsD
- Support for Prometheus remote_read and remote_write
- Retrieve the VGroup ID of the table's virtual node group (VGroup)

## taosAdapter Architecture Diagram

<figure>
<Image img={imgAdapter} alt="taosAdapter architecture"/>
<figcaption>Figure 1. taosAdapter architecture</figcaption>
</figure>

## taosAdapter Deployment Method

### Installing taosAdapter

taosAdapter is part of the TDengine server software. If you are using the TDengine server, no additional steps are needed to install taosAdapter. If you need to deploy taosAdapter separately on a server outside the TDengine server, you should install the full TDengine on that server to install taosAdapter. If you need to compile taosAdapter from the source code, you can refer to the documentation on [Building taosAdapter](https://github.com/taosdata/taosadapter/blob/3.0/BUILD.md).

### Starting/Stopping taosAdapter

On Linux systems, the taosAdapter service is managed by systemd by default. Use the command `systemctl start taosadapter` to start the taosAdapter service. Use the command `systemctl stop taosadapter` to stop the taosAdapter service.

### Removing taosAdapter

Use the command `rmtaos` to remove the TDengine server software, including taosAdapter.

### Upgrading taosAdapter

taosAdapter and the TDengine server need to be the same version. Please upgrade taosAdapter by upgrading the TDengine server. The taosAdapter deployed separately from taosd must be upgraded by upgrading the TDengine server it is installed on.

## taosAdapter Parameter List

taosAdapter supports configuration through command-line parameters, environment variables, and configuration files. The default configuration file is `/etc/taos/taosadapter.toml`.

Command-line parameters take precedence over environment variables, which take precedence over configuration files. The command-line usage is `arg=val`, for example, `taosadapter -p=30000 --debug=true`. The detailed list is as follows:

```text
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

:::note
When making interface calls using a browser, please set the following CORS parameters according to your actual situation:

```text
AllowAllOrigins
AllowOrigins
AllowHeaders
ExposeHeaders
AllowCredentials
AllowWebSockets
```

If you are not making interface calls through a browser, there is no need to worry about these configurations.

For details on the CORS protocol, please refer to: [https://www.w3.org/wiki/CORS_Enabled](https://www.w3.org/wiki/CORS_Enabled) or [https://developer.mozilla.org/docs/Web/HTTP/CORS](https://developer.mozilla.org/docs/Web/HTTP/CORS).

:::

An example configuration file can be found at [example/config/taosadapter.toml](https://github.com/taosdata/taosadapter/blob/3.0/example/config/taosadapter.toml).

### Connection Pool Parameter Explanation

When using the RESTful interface for requests, the system will manage TDengine connections through a connection pool. The connection pool can be configured using the following parameters:

- **`pool.maxConnect`**: The maximum number of connections allowed in the connection pool; default value is twice the number of CPU cores. It is recommended to keep the default setting.
- **`pool.maxIdle`**: The maximum number of idle connections allowed in the connection pool; defaults to the same as `pool.maxConnect`. It is recommended to keep the default setting.
- **`pool.idleTimeout`**: The idle timeout for connections; defaults to never timing out. It is recommended to keep the default setting.
- **`pool.waitTimeout`**: The timeout for obtaining connections from the connection pool; defaults to 60 seconds. If a connection cannot be obtained within the timeout period, an HTTP status code of 503 will be returned. This parameter has been available since version 3.3.3.0.
- **`pool.maxWait`**: The upper limit on the number of requests waiting to obtain connections in the connection pool; default value is 0, indicating no limit. When the number of queued requests exceeds this value, new requests will return an HTTP status code of 503. This parameter has been available since version 3.3.3.0.

## Feature List

- RESTful interface
  [RESTful API](../../client-libraries/rest-api/)
- InfluxDB v1 write interface compatibility
  [https://docs.influxdata.com/influxdb/v2.0/reference/api/influxdb-1x/write/](https://docs.influxdata.com/influxdb/v2.0/reference/api/influxdb-1x/write/)
- OpenTSDB JSON and telnet format writing compatibility
  - [http://opentsdb.net/docs/build/html/api_http/put.html](http://opentsdb.net/docs/build/html/api_http/put.html)
  - [http://opentsdb.net/docs/build/html/api_telnet/put.html](http://opentsdb.net/docs/build/html/api_telnet/put.html)
- Seamless connection to collectd.
  collectd is a system statistics collection daemon; visit [https://collectd.org/](https://collectd.org/) for more information.
- Seamless connection to StatsD.
  StatsD is a simple yet powerful statistics aggregator daemon. Visit [https://github.com/statsd/statsd](https://github.com/statsd/statsd) for more information.
- Seamless connection to icinga2.
  icinga2 is software for collecting check results metrics and performance data. Visit [https://icinga.com/docs/icinga-2/latest/doc/14-features/#opentsdb-writer](https://icinga.com/docs/icinga-2/latest/doc/14-features/#opentsdb-writer) for more information.
- Seamless connection to tcollector.
  TCollector is a client process that collects data from local collectors and pushes it to OpenTSDB. Visit [http://opentsdb.net/docs/build/html/user_guide/utilities/tcollector.html](http://opentsdb.net/docs/build/html/user_guide/utilities/tcollector.html) for more information.
- Seamless connection to node_exporter.
  node_exporter is an exporter for machine metrics exposed by the *NIX kernel. Visit [https://github.com/prometheus/node_exporter](https://github.com/prometheus/node_exporter) for more information.
- Support for Prometheus remote_read and remote_write.
  remote_read and remote_write are cluster solutions for separating data read and write in Prometheus. Visit [https://prometheus.io/blog/2019/10/10/remote-read-meets-streaming/#remote-apis](https://prometheus.io/blog/2019/10/10/remote-read-meets-streaming/#remote-apis) for more information.
- Retrieve the VGroup ID of the table's virtual node group (VGroup).

## Interfaces

### TDengine RESTful Interface

You can use any client that supports the HTTP protocol to write data to TDengine or query data from TDengine by accessing the RESTful interface address `http://<fqdn>:6041/rest/sql`. For details, please refer to the [REST API documentation](../../client-libraries/rest-api/).

### InfluxDB

You can use any client that supports the HTTP protocol to access the RESTful interface address `http://<fqdn>:6041/influxdb/v1/write` to write InfluxDB-compatible format data to TDengine.

The following InfluxDB parameters are supported:

- `db` specifies the database name used by TDengine
- `precision` is the time precision used by TDengine
- `u` is the username for TDengine
- `p` is the password for TDengine
- `ttl` is the lifecycle of the automatically created subtable, based on the TTL parameter of the first data in the subtable and cannot be updated. For more information, please refer to the [Create Table Documentation](../../sql-manual/manage-tables/) for the TTL parameter.

:::note

Currently, the token authentication method of InfluxDB is not supported; only Basic authentication and query parameter authentication are supported.

Example: `curl --request POST http://127.0.0.1:6041/influxdb/v1/write?db=test --user "root:taosdata" --data-binary "measurement,host=host1 field1=2i,field2=2.0 1577836800000000000"`

:::

### OpenTSDB

You can use any client that supports the HTTP protocol to access the RESTful interface address `http://<fqdn>:6041/<APIEndPoint>` to write OpenTSDB-compatible format data to TDengine. The endpoints are as follows:

```text
/opentsdb/v1/put/json/<db>
/opentsdb/v1/put/telnet/<db>
```

### collectd

<CollectD />

### StatsD

<StatsD />

### icinga2 OpenTSDB writer

<Icinga2 />

### TCollector

<TCollector />

### node_exporter

Prometheus uses an exporter for machine metrics exposed by the \*NIX kernel.

- Enable taosAdapter's configuration `node_exporter.enable`
- Set relevant configurations for `node_exporter`
- Restart taosAdapter

### prometheus

<Prometheus />

### Retrieve the VGroup ID of a Table

You can access the HTTP interface `http://<fqdn>:6041/rest/vgid?db=<db>&table=<table>` to get the VGroup ID of the table.

## Memory Usage Optimization Methods

taosAdapter monitors its memory usage during runtime and adjusts based on two thresholds. The valid range is integers from -1 to 100, representing the percentage of the system's physical memory.

- pauseQueryMemoryThreshold
- pauseAllMemoryThreshold

When the pauseQueryMemoryThreshold is exceeded, processing of query requests will stop.

HTTP returns:

- code 503
- body "query memory exceeds threshold"

When the pauseAllMemoryThreshold is exceeded, processing of all write and query requests will stop.

HTTP returns:

- code 503
- body "memory exceeds threshold"

When memory falls below the thresholds, the corresponding functionalities will resume.

Status check interface `http://<fqdn>:6041/-/ping`

- Returns `code 200` when normal
- No parameters; if memory exceeds pauseAllMemoryThreshold, it will return `code 503`
- Request parameter `action=query`; if memory exceeds pauseQueryMemoryThreshold or pauseAllMemoryThreshold, it will return `code 503`

Corresponding configuration parameters:

```text
  monitor.collectDuration              Monitoring interval                                    Environment variable "TAOS_MONITOR_COLLECT_DURATION" (default value 3s)
  monitor.incgroup                     Whether running in cgroup (set to true if running in a container)      Environment variable "TAOS_MONITOR_INCGROUP"
  monitor.pauseAllMemoryThreshold      Memory threshold for pausing inserts and queries                   Environment variable "TAOS_MONITOR_PAUSE_ALL_MEMORY_THRESHOLD" (default value 80)
  monitor.pauseQueryMemoryThreshold    Memory threshold for pausing queries                        Environment variable "TAOS_MONITOR_PAUSE_QUERY_MEMORY_THRESHOLD" (default value 70)
```

You can make corresponding adjustments based on specific project application scenarios and operational strategies, and it is recommended to use operational monitoring software to monitor the system's memory status in a timely manner. Load balancers can also use this interface to check the running status of taosAdapter.

## taosAdapter Monitoring Metrics

taosAdapter collects monitoring metrics related to REST/Websocket requests. It reports these metrics to taosKeeper, which writes them into the monitoring database, which is by default the `log` database, and can be modified in the taoskeeper configuration file. The following is a detailed introduction to these monitoring metrics.

### adapter_requests Table

`adapter_requests` records taosadapter monitoring data.

| field              | type         | is\_tag | comment                                   |
| :----------------- | :----------- | :------ | :---------------------------------------- |
| ts                 | TIMESTAMP    |         | timestamp                                 |
| total              | INT UNSIGNED |         | Total number of requests                  |
| query              | INT UNSIGNED |         | Number of query requests                  |
| write              | INT UNSIGNED |         | Number of write requests                  |
| other              | INT UNSIGNED |         | Number of other requests                  |
| in\_process        | INT UNSIGNED |         | Number of requests in process             |
| success            | INT UNSIGNED |         | Number of successful requests             |
| fail               | INT UNSIGNED |         | Number of failed requests                 |
| query\_success     | INT UNSIGNED |         | Number of successful query requests       |
| query\_fail        | INT UNSIGNED |         | Number of failed query requests           |
| write\_success     | INT UNSIGNED |         | Number of successful write requests       |
| write\_fail        | INT UNSIGNED |         | Number of failed write requests           |
| other\_success     | INT UNSIGNED |         | Number of other successful requests       |
| other\_fail        | INT UNSIGNED |         | Number of other failed requests           |
| query\_in\_process | INT UNSIGNED |         | Number of queries in process              |
| write\_in\_process | INT UNSIGNED |         | Number of writes in process               |
| endpoint           | VARCHAR      |         | Request endpoint                          |
| req\_type          | NCHAR        | TAG     | Request type: 0 for REST, 1 for Websocket |

## Result Return Count Limit

taosAdapter controls the number of rows returned by the parameter `restfulRowLimit`, where -1 indicates no limit, and the default is unlimited.

This parameter controls the return of the following interfaces:

- `http://<fqdn>:6041/rest/sql`
- `http://<fqdn>:6041/prometheus/v1/remote_read/:db`

## HTTP Return Code Configuration

taosAdapter uses the parameter `httpCodeServerError` to set whether to return a non-200 HTTP status code when the C interface returns an error. When set to true, it will return different HTTP status codes based on the C return error code. For specifics, see [HTTP Response Codes](../../client-libraries/rest-api/#http-response-codes).

## Configuration for Automatic DB Creation on Schemaless Writes

Starting from version 3.0.4.0, taosAdapter provides the parameter `smlAutoCreateDB` to control whether to automatically create a DB when writing using the schemaless protocol. The default value is false, meaning the DB must be manually created by the user before performing schemaless writes.

## Troubleshooting

You can check the running status of taosAdapter by using the command `systemctl status taosadapter`.

You can also adjust the verbosity of taosAdapter's log output by setting the `--logLevel` parameter or the environment variable `TAOS_ADAPTER_LOG_LEVEL`. Valid values include: panic, fatal, error, warn, warning, info, debug, and trace.

## How to Migrate from Old Versions of TDengine to taosAdapter

In TDengine server version 2.2.x.x or earlier, the taosd process contained an embedded HTTP service. As mentioned earlier, taosAdapter is an independent software managed by systemd, with its own process. There are some configuration parameters and behaviors that differ between the two, as shown in the table below:

| **#** | **embedded httpd**  | **taosAdapter**                                        | **comment**                                                  |
| ----- | ------------------- | ------------------------------------------------------ | ------------------------------------------------------------ |
| 1     | httpEnableRecordSql | --logLevel=debug                                       |                                                              |
| 2     | httpMaxThreads      | n/a                                                    | taosAdapter automatically manages the thread pool; this parameter is not needed. |
| 3     | telegrafUseFieldNum | Refer to the taosAdapter telegraf configuration method |                                                              |
| 4     | restfulRowLimit     | restfulRowLimit                                        | The embedded httpd defaults to output 10,240 rows of data, with a maximum allowable value of 102,400. taosAdapter also provides restfulRowLimit but defaults to no limit. You can configure it according to your actual scenario needs. |
| 5     | httpDebugFlag       | Not applicable                                         | httpdDebugFlag does not apply to taosAdapter.                |
| 6     | httpDBNameMandatory | Not applicable                                         | taosAdapter requires the database name to be specified in the URL. |
