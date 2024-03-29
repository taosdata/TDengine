---
title: taosAdapter
sidebar_label: taosAdapter
description: This document describes how to use taosAdapter, a TDengine companion tool that acts as a bridge and adapter between TDengine clusters and applications.
---

import Prometheus from "./_prometheus.mdx"
import CollectD from "./_collectd.mdx"
import StatsD from "./_statsd.mdx"
import Icinga2 from "./_icinga2.mdx"
import TCollector from "./_tcollector.mdx"

taosAdapter is a TDengine companion tool that acts as a bridge and adapter between TDengine clusters and applications. It provides an easy-to-use and efficient way to ingest data directly from data collection agent software such as Telegraf, StatsD, collectd, etc. It also provides an InfluxDB/OpenTSDB compatible data ingestion interface that allows InfluxDB/OpenTSDB applications to be seamlessly ported to TDengine.

taosAdapter provides the following features.

- RESTful interface
- InfluxDB v1 compliant write interface
- OpenTSDB JSON and telnet format writes compatible
- Seamless connection to Telegraf
- Seamless connection to collectd
- Seamless connection to StatsD
- Supports Prometheus remote_read and remote_write
- Get table's VGroup ID

## taosAdapter architecture diagram

![TDengine Database taosAdapter Architecture](taosAdapter-architecture.webp)

## taosAdapter Deployment Method

### Install taosAdapter

If you use the TDengine server, you don't need additional steps to install taosAdapter.  If you need to deploy taosAdapter separately on another server other than the TDengine server, you should install the full TDengine server package on that server to install taosAdapter. If you need to build taosAdapter from source code, you can refer to the [Building taosAdapter]( https://github.com/taosdata/taosadapter/blob/3.0/BUILD.md) documentation.

### Start/Stop taosAdapter

On Linux systems, the taosAdapter service is managed by `systemd` by default. You can use the command `systemctl start taosadapter` to start the taosAdapter service and use the command `systemctl stop taosadapter` to stop the taosAdapter service.

### Remove taosAdapter

Use the command `rmtaos` to remove the TDengine server software if you use tar.gz package. If you installed using a .deb or .rpm package, use the corresponding command, for your package manager, like apt or rpm to remove the TDengine server, including taosAdapter.

### Upgrade taosAdapter

taosAdapter and TDengine server need to use the same version. Please upgrade the taosAdapter by upgrading the TDengine server.
You need to upgrade the taosAdapter deployed separately from TDengine server by upgrading the TDengine server on the deployed server.

## taosAdapter parameter list

taosAdapter is configurable via command-line arguments, environment variables and configuration files. The default configuration file is /etc/taos/taosadapter.toml on Linux.

Command-line arguments take precedence over environment variables over configuration files. The command-line usage is arg=val, e.g., taosadapter -p=30000 --debug=true. The detailed list is as follows:

```shell
Usage of taosAdapter:
      --collectd.db string                               collectd db name. Env "TAOS_ADAPTER_COLLECTD_DB" (default "collectd")
      --collectd.enable                                  enable collectd. Env "TAOS_ADAPTER_COLLECTD_ENABLE" (default true)
      --collectd.password string                         collectd password. Env "TAOS_ADAPTER_COLLECTD_PASSWORD" (default "taosdata")
      --collectd.port int                                collectd server port. Env "TAOS_ADAPTER_COLLECTD_PORT" (default 6045)
      --collectd.ttl int                                 collectd data ttl. Env "TAOS_ADAPTER_COLLECTD_TTL"
      --collectd.user string                             collectd user. Env "TAOS_ADAPTER_COLLECTD_USER" (default "root")
      --collectd.worker int                              collectd write worker. Env "TAOS_ADAPTER_COLLECTD_WORKER" (default 10)
  -c, --config string                                    config path default /etc/taos/taosadapter.toml
      --cors.allowAllOrigins                             cors allow all origins. Env "TAOS_ADAPTER_CORS_ALLOW_ALL_ORIGINS" (default true)
      --cors.allowCredentials                            cors allow credentials. Env "TAOS_ADAPTER_CORS_ALLOW_Credentials"
      --cors.allowHeaders stringArray                    cors allow HEADERS. Env "TAOS_ADAPTER_ALLOW_HEADERS"
      --cors.allowOrigins stringArray                    cors allow origins. Env "TAOS_ADAPTER_ALLOW_ORIGINS"
      --cors.allowWebSockets                             cors allow WebSockets. Env "TAOS_ADAPTER_CORS_ALLOW_WebSockets"      --cors.exposeHeaders stringArray                   cors expose headers. Env "TAOS_ADAPTER_Expose_Headers"
      --debug                                            enable debug mode. Env "TAOS_ADAPTER_DEBUG" (default true)
      --help                                             Print this help message and exit
      --httpCodeServerError                              Use a non-200 http status code when server returns an error. Env "TAOS_ADAPTER_HTTP_CODE_SERVER_ERROR"
      --influxdb.enable                                  enable influxdb. Env "TAOS_ADAPTER_INFLUXDB_ENABLE" (default true)
      --log.enableRecordHttpSql                          whether to record http sql. Env "TAOS_ADAPTER_LOG_ENABLE_RECORD_HTTP_SQL"
      --log.path string                                  log path. Env "TAOS_ADAPTER_LOG_PATH" (default "/var/log/taos")      --log.rotationCount uint                           log rotation count. Env "TAOS_ADAPTER_LOG_ROTATION_COUNT" (default 30)
      --log.rotationSize string                          log rotation size(KB MB GB), must be a positive integer. Env "TAOS_ADAPTER_LOG_ROTATION_SIZE" (default "1GB")
      --log.rotationTime duration                        log rotation time. Env "TAOS_ADAPTER_LOG_ROTATION_TIME" (default 24h0m0s)
      --log.sqlRotationCount uint                        record sql log rotation count. Env "TAOS_ADAPTER_LOG_SQL_ROTATION_COUNT" (default 2)
      --log.sqlRotationSize string                       record sql log rotation size(KB MB GB), must be a positive integer. Env "TAOS_ADAPTER_LOG_SQL_ROTATION_SIZE" (default "1GB")
      --log.sqlRotationTime duration                     record sql log rotation time. Env "TAOS_ADAPTER_LOG_SQL_ROTATION_TIME" (default 24h0m0s)
      --logLevel string                                  log level (panic fatal error warn warning info debug trace). Env "TAOS_ADAPTER_LOG_LEVEL" (default "info")
      --monitor.collectDuration duration                 Set monitor duration. Env "TAOS_ADAPTER_MONITOR_COLLECT_DURATION" (default 3s)
      --monitor.disable                                  Whether to disable monitoring. Env "TAOS_ADAPTER_MONITOR_DISABLE"
      --monitor.disableCollectClientIP                   Whether to disable collecting clientIP. Env "TAOS_ADAPTER_MONITOR_DISABLE_COLLECT_CLIENT_IP"
      --monitor.identity string                          The identity of the current instance, or 'hostname:port' if it is empty. Env "TAOS_ADAPTER_MONITOR_IDENTITY"
      --monitor.incgroup                                 Whether running in cgroup. Env "TAOS_ADAPTER_MONITOR_INCGROUP"
      --monitor.password string                          TDengine password. Env "TAOS_ADAPTER_MONITOR_PASSWORD" (default "taosdata")
      --monitor.pauseAllMemoryThreshold float            Memory percentage threshold for pause all. Env "TAOS_ADAPTER_MONITOR_PAUSE_ALL_MEMORY_THRESHOLD" (default 80)
      --monitor.pauseQueryMemoryThreshold float          Memory percentage threshold for pause query. Env "TAOS_ADAPTER_MONITOR_PAUSE_QUERY_MEMORY_THRESHOLD" (default 70)
      --monitor.user string                              TDengine user. Env "TAOS_ADAPTER_MONITOR_USER" (default "root")      --monitor.writeInterval duration                   Set write to TDengine interval. Env "TAOS_ADAPTER_MONITOR_WRITE_INTERVAL" (default 30s)
      --monitor.writeToTD                                Whether write metrics to TDengine. Env "TAOS_ADAPTER_MONITOR_WRITE_TO_TD"
      --node_exporter.caCertFile string                  node_exporter ca cert file path. Env "TAOS_ADAPTER_NODE_EXPORTER_CA_CERT_FILE"
      --node_exporter.certFile string                    node_exporter cert file path. Env "TAOS_ADAPTER_NODE_EXPORTER_CERT_FILE"
      --node_exporter.db string                          node_exporter db name. Env "TAOS_ADAPTER_NODE_EXPORTER_DB" (default "node_exporter")
      --node_exporter.enable                             enable node_exporter. Env "TAOS_ADAPTER_NODE_EXPORTER_ENABLE"
      --node_exporter.gatherDuration duration            node_exporter gather duration. Env "TAOS_ADAPTER_NODE_EXPORTER_GATHER_DURATION" (default 5s)
      --node_exporter.httpBearerTokenString string       node_exporter http bearer token. Env "TAOS_ADAPTER_NODE_EXPORTER_HTTP_BEARER_TOKEN_STRING"
      --node_exporter.httpPassword string                node_exporter http password. Env "TAOS_ADAPTER_NODE_EXPORTER_HTTP_PASSWORD"
      --node_exporter.httpUsername string                node_exporter http username. Env "TAOS_ADAPTER_NODE_EXPORTER_HTTP_USERNAME"
      --node_exporter.insecureSkipVerify                 node_exporter skip ssl check. Env "TAOS_ADAPTER_NODE_EXPORTER_INSECURE_SKIP_VERIFY" (default true)
      --node_exporter.keyFile string                     node_exporter cert key file path. Env "TAOS_ADAPTER_NODE_EXPORTER_KEY_FILE"
      --node_exporter.password string                    node_exporter password. Env "TAOS_ADAPTER_NODE_EXPORTER_PASSWORD" (default "taosdata")
      --node_exporter.responseTimeout duration           node_exporter response timeout. Env "TAOS_ADAPTER_NODE_EXPORTER_RESPONSE_TIMEOUT" (default 5s)
      --node_exporter.ttl int                            node_exporter data ttl. Env "TAOS_ADAPTER_NODE_EXPORTER_TTL"
      --node_exporter.urls strings                       node_exporter urls. Env "TAOS_ADAPTER_NODE_EXPORTER_URLS" (default [http://localhost:9100])
      --node_exporter.user string                        node_exporter user. Env "TAOS_ADAPTER_NODE_EXPORTER_USER" (default "root")
      --opentsdb.enable                                  enable opentsdb. Env "TAOS_ADAPTER_OPENTSDB_ENABLE" (default true)
      --opentsdb_telnet.batchSize int                    opentsdb_telnet batch size. Env "TAOS_ADAPTER_OPENTSDB_TELNET_BATCH_SIZE" (default 1)
      --opentsdb_telnet.dbs strings                      opentsdb_telnet db names. Env "TAOS_ADAPTER_OPENTSDB_TELNET_DBS" (default [opentsdb_telnet,collectd_tsdb,icinga2_tsdb,tcollector_tsdb])
      --opentsdb_telnet.enable                           enable opentsdb telnet,warning: without auth info(default false). Env "TAOS_ADAPTER_OPENTSDB_TELNET_ENABLE"
      --opentsdb_telnet.flushInterval duration           opentsdb_telnet flush interval (0s means not valid) . Env "TAOS_ADAPTER_OPENTSDB_TELNET_FLUSH_INTERVAL"
      --opentsdb_telnet.maxTCPConnections int            max tcp connections. Env "TAOS_ADAPTER_OPENTSDB_TELNET_MAX_TCP_CONNECTIONS" (default 250)
      --opentsdb_telnet.password string                  opentsdb_telnet password. Env "TAOS_ADAPTER_OPENTSDB_TELNET_PASSWORD" (default "taosdata")
      --opentsdb_telnet.ports ints                       opentsdb telnet tcp port. Env "TAOS_ADAPTER_OPENTSDB_TELNET_PORTS" (default [6046,6047,6048,6049])
      --opentsdb_telnet.tcpKeepAlive                     enable tcp keep alive. Env "TAOS_ADAPTER_OPENTSDB_TELNET_TCP_KEEP_ALIVE"
      --opentsdb_telnet.ttl int                          opentsdb_telnet data ttl. Env "TAOS_ADAPTER_OPENTSDB_TELNET_TTL"
      --opentsdb_telnet.user string                      opentsdb_telnet user. Env "TAOS_ADAPTER_OPENTSDB_TELNET_USER" (default "root")
      --pool.idleTimeout duration                        Set idle connection timeout. Env "TAOS_ADAPTER_POOL_IDLE_TIMEOUT"
      --pool.maxConnect int                              max connections to server. Env "TAOS_ADAPTER_POOL_MAX_CONNECT"
      --pool.maxIdle int                                 max idle connections to server. Env "TAOS_ADAPTER_POOL_MAX_IDLE"
  -P, --port int                                         http port. Env "TAOS_ADAPTER_PORT" (default 6041)
      --prometheus.enable                                enable prometheus. Env "TAOS_ADAPTER_PROMETHEUS_ENABLE" (default true)
      --restfulRowLimit int                              restful returns the maximum number of rows (-1 means no limit). Env "TAOS_ADAPTER_RESTFUL_ROW_LIMIT" (default -1)
      --smlAutoCreateDB                                  Whether to automatically create db when writing with schemaless. Env "TAOS_ADAPTER_SML_AUTO_CREATE_DB"
      --statsd.allowPendingMessages int                  statsd allow pending messages. Env "TAOS_ADAPTER_STATSD_ALLOW_PENDING_MESSAGES" (default 50000)
      --statsd.db string                                 statsd db name. Env "TAOS_ADAPTER_STATSD_DB" (default "statsd")      --statsd.deleteCounters                            statsd delete counter cache after gather. Env "TAOS_ADAPTER_STATSD_DELETE_COUNTERS" (default true)
      --statsd.deleteGauges                              statsd delete gauge cache after gather. Env "TAOS_ADAPTER_STATSD_DELETE_GAUGES" (default true)
      --statsd.deleteSets                                statsd delete set cache after gather. Env "TAOS_ADAPTER_STATSD_DELETE_SETS" (default true)
      --statsd.deleteTimings                             statsd delete timing cache after gather. Env "TAOS_ADAPTER_STATSD_DELETE_TIMINGS" (default true)
      --statsd.enable                                    enable statsd. Env "TAOS_ADAPTER_STATSD_ENABLE" (default true)
      --statsd.gatherInterval duration                   statsd gather interval. Env "TAOS_ADAPTER_STATSD_GATHER_INTERVAL" (default 5s)
      --statsd.maxTCPConnections int                     statsd max tcp connections. Env "TAOS_ADAPTER_STATSD_MAX_TCP_CONNECTIONS" (default 250)
      --statsd.password string                           statsd password. Env "TAOS_ADAPTER_STATSD_PASSWORD" (default "taosdata")
      --statsd.port int                                  statsd server port. Env "TAOS_ADAPTER_STATSD_PORT" (default 6044)
      --statsd.protocol string                           statsd protocol [tcp or udp]. Env "TAOS_ADAPTER_STATSD_PROTOCOL" (default "udp")
      --statsd.tcpKeepAlive                              enable tcp keep alive. Env "TAOS_ADAPTER_STATSD_TCP_KEEP_ALIVE"      --statsd.ttl int                                   statsd data ttl. Env "TAOS_ADAPTER_STATSD_TTL"
      --statsd.user string                               statsd user. Env "TAOS_ADAPTER_STATSD_USER" (default "root")
      --statsd.worker int                                statsd write worker. Env "TAOS_ADAPTER_STATSD_WORKER" (default 10)
      --taosConfigDir string                             load taos client config path. Env "TAOS_ADAPTER_TAOS_CONFIG_FILE"
      --tmq.releaseIntervalMultiplierForAutocommit int   When set to autocommit, the interval for message release is a multiple of the autocommit interval, with a default value of 2 and a minimum value of 1 and a maximum value of 10. Env "TAOS_ADAPTER_TMQ_RELEASE_INTERVAL_MULTIPLIER_FOR_AUTOCOMMIT" (default 2)
      --version                                          Print the version and exit
```

Note:
Please set the following Cross-Origin Resource Sharing (CORS) parameters according to the actual situation when using a browser for interface calls.

```text
AllowAllOrigins
AllowOrigins
AllowHeaders
ExposeHeaders
AllowCredentials
AllowWebSockets
```

You do not need to care about these configurations if you do not make interface calls through the browser.

For details on the CORS protocol, please refer to: [https://www.w3.org/wiki/CORS_Enabled](https://www.w3.org/wiki/CORS_Enabled) or [https://developer.mozilla.org/zh-CN/docs/Web/HTTP/CORS](https://developer.mozilla.org/zh-CN/docs/Web/HTTP/CORS).

See [example/config/taosadapter.toml](https://github.com/taosdata/taosadapter/blob/3.0/example/config/taosadapter.toml) for sample configuration files.

## Feature List

- RESTful interface
  [https://docs.tdengine.com/reference/rest-api/](https://docs.tdengine.com/reference/rest-api/)
- Compatible with InfluxDB v1 write interface
  [https://docs.influxdata.com/influxdb/v2.0/reference/api/influxdb-1x/write/](https://docs.influxdata.com/influxdb/v2.0/reference/api/influxdb-1x/write/)
- Compatible with OpenTSDB JSON and telnet format writes
  - [http://opentsdb.net/docs/build/html/api_http/put.html](http://opentsdb.net/docs/build/html/api_http/put.html)
  - [http://opentsdb.net/docs/build/html/api_telnet/put.html](http://opentsdb.net/docs/build/html/api_telnet/put.html)
- Seamless connection to collectd
  collectd is a system statistics collection daemon, please visit [https://collectd.org/](https://collectd.org/) for more information.
- Seamless connection with StatsD
  StatsD is a simple yet powerful daemon for aggregating statistical information. Please visit [https://github.com/statsd/statsd](https://github.com/statsd/statsd) for more information.
- Seamless connection with icinga2
  icinga2 is a software that collects inspection result metrics and performance data. Please visit [https://icinga.com/docs/icinga-2/latest/doc/14-features/#opentsdb-writer](https://icinga.com/docs/icinga-2/latest/doc/14-features/#opentsdb-writer) for more information.
- Seamless connection to TCollector
  TCollector is a client process that collects data from a local collector and pushes the data to OpenTSDB. Please visit [http://opentsdb.net/docs/build/html/user_guide/utilities/tcollector.html](http://opentsdb.net/docs/build/html/user_guide/utilities/tcollector.html) for more information.
- Seamless connection to node_exporter
  node_export is an exporter for machine metrics. Please visit [https://github.com/prometheus/node_exporter](https://github.com/prometheus/node_exporter) for more information.
- Support for Prometheus remote_read and remote_write
  remote_read and remote_write are interfaces for Prometheus data read and write from/to other data storage solution. Please visit [https://prometheus.io/blog/2019/10/10/remote-read-meets-streaming/#remote-apis](https://prometheus.io/blog/2019/10/10/remote-read-meets-streaming/#remote-apis) for more information.
- Get table's VGroup ID. 

## Interfaces

### TDengine RESTful interface

You can use any client that supports the http protocol to write data to or query data from TDengine by accessing the REST interface address `http://<fqdn>:6041/rest/sql`. See the [official documentation](../rest-api/) for details.

### InfluxDB

You can use any client that supports the http protocol to access the RESTful interface address `http://<fqdn>:6041/<APIEndPoint>` to write data in InfluxDB compatible format to TDengine. The EndPoint is as follows:

```text
/influxdb/v1/write
```

Support InfluxDB query parameters as follows.

- `db` Specifies the database name used by TDengine
- `precision` The time precision used by TDengine
- `u` TDengine user name
- `p` TDengine password
- `ttl` The time to live of automatically created sub-table. This value cannot be updated. TDengine will use the ttl value of the first data of sub-table to create sub-table. For more information, please refer [Create Table](../../taos-sql/table/#create-table)

Note:      InfluxDB token authorization is not supported at present. Only Basic authorization and query parameter validation are supported.
Example:   curl --request POST http://127.0.0.1:6041/influxdb/v1/write?db=test --user "root:taosdata" --data-binary "measurement,host=host1 field1=2i,field2=2.0 1577836800000000000"

### OpenTSDB

You can use any client that supports the http protocol to access the RESTful interface address `http://<fqdn>:6041/<APIEndPoint>` to write data in OpenTSDB compatible format to TDengine. The EndPoint is as follows:

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

node_export is an exporter of hardware and OS metrics exposed by the \*NIX kernel used by Prometheus

- Enable the taosAdapter configuration `node_exporter.enable`
- Set the configuration of the node_exporter
- Restart taosAdapter

### Prometheus

<Prometheus />

### Get table's VGroup ID

You can call `http://<fqdn>:6041/rest/vgid?db=<db>&table=<table>` to get table's VGroup ID. 

## Memory usage optimization methods

taosAdapter will monitor its memory usage during operation and adjust it with two thresholds. Valid values are integers between 1 to 100, and represent a percentage of the system's physical memory.

- pauseQueryMemoryThreshold
- pauseAllMemoryThreshold

Stops processing query requests when the `pauseQueryMemoryThreshold` threshold is exceeded.

HTTP response content.

- code 503
- body "query memory exceeds threshold"

Stops processing all write and query requests when the `pauseAllMemoryThreshold` threshold is exceeded.

HTTP response content.

- code 503
- body "memory exceeds threshold"

Resume the corresponding function when the memory falls back below the threshold.

Status check interface `http://<fqdn>:6041/-/ping`

- Normal returns `code 200`
- No parameter If memory exceeds pauseAllMemoryThreshold returns `code 503`
- Request parameter `action=query` returns `code 503` if memory exceeds `pauseQueryMemoryThreshold` or `pauseAllMemoryThreshold`

Corresponding configuration parameter

```text
  monitor.collectDuration monitoring interval environment variable `TAOS_MONITOR_COLLECT_DURATION` (default value 3s)
  monitor.incgroup whether to run in cgroup (set to true for running in container) environment variable `TAOS_MONITOR_INCGROUP`
  monitor.pauseAllMemoryThreshold memory threshold for no more inserts and queries environment variable `TAOS_MONITOR_PAUSE_ALL_MEMORY_THRESHOLD` (default 80)
  monitor.pauseQueryMemoryThreshold memory threshold for no more queries Environment variable `TAOS_MONITOR_PAUSE_QUERY_MEMORY_THRESHOLD` (default 70)
```

You should adjust this parameter based on your specific application scenario and operation strategy. We recommend using monitoring software to monitor system memory status. The load balancer can also check the taosAdapter running status through this interface.

## taosAdapter Monitoring Metrics

taosAdapter collects HTTP-related metrics, CPU percentage, and memory percentage.

### HTTP interface

Provides an interface conforming to [OpenMetrics](https://github.com/OpenObservability/OpenMetrics/blob/main/specification/OpenMetrics.md).

```text
http://<fqdn>:6041/metrics
```

### Write to TDengine

taosAdapter supports writing the metrics of HTTP monitoring, CPU percentage, and memory percentage to TDengine.

For configuration parameters

| **Configuration items** | **Description** | **Default values** |
|-------------------------|--------------------------------------------|----------|
| monitor.collectDuration | CPU and memory collection interval | 3s |
| monitor.identity | The current taosadapter identifier will be used if not set to `hostname:port` | |
| monitor.incgroup | whether it is running in a cgroup (set to true for running in a container) | false |
| monitor.writeToTD | Whether to write to TDengine | false |
| monitor.user | TDengine connection username | root |
| monitor.password | TDengine connection password | taosdata |
| monitor.writeInterval | Write to TDengine interval | 30s |

## Limit the number of results returned

taosAdapter controls the number of results returned by the parameter `restfulRowLimit`, -1 means no limit, default is no limit.

This parameter controls the number of results returned by the following interfaces:

- `http://<fqdn>:6041/rest/sql`
- `http://<fqdn>:6041/prometheus/v1/remote_read/:db`

## Configure http return code

taosAdapter uses the parameter `httpCodeServerError` to set whether to return a non-200 http status code  http status code other than when the C interface returns an error. When set to true, different http status codes will be returned according to the error code returned by C. For details, see [RESTful API](https://docs.tdengine.com/reference/rest-api/) HTTP Response Code chapter.

## Configure whether schemaless writes automatically create DBs

Starting from version 3.0.4.0, the taosAdapter provides the parameter "smlAutoCreateDB" to control whether to automatically create DBs when writing with the schemaless protocol. The default value is false, which means that the DB will not be automatically created and the user needs to manually create the DB before performing schemaless writing.

## Troubleshooting

You can check the taosAdapter running status with the `systemctl status taosadapter` command.

You can also adjust the level of the taosAdapter log output by setting the `--logLevel` parameter or the environment variable `TAOS_ADAPTER_LOG_LEVEL`. Valid values are: panic, fatal, error, warn, warning, info, debug and trace.

## How to migrate from older TDengine versions to taosAdapter

In TDengine server 2.2.x.x or earlier, the TDengine server process (taosd) contains an embedded HTTP service. As mentioned earlier, taosAdapter is a standalone software managed using `systemd` and has its own process ID. There are some configuration parameters and behaviors that are different between the two. See the following table for details.

| **#** | **embedded httpd** | **taosAdapter** | **comment** |
|-------|---------------------|-------------------------------|------------------------------------------------------------------------------------------------|
| 1 | httpEnableRecordSql | --logLevel=debug | |
| 2 | httpMaxThreads | n/a | taosAdapter Automatically manages thread pools without this parameter |
| 3 | telegrafUseFieldNum | See the taosAdapter telegraf configuration method | |
| 4 | restfulRowLimit | restfulRowLimit | Embedded httpd outputs 10240 rows of data by default, the maximum allowed is 102400. taosAdapter also provides restfulRowLimit but it is not limited by default. You can configure it according to the actual scenario.
| 5 | httpDebugFlag | Not applicable | httpdDebugFlag does not work for taosAdapter |
| 6 | httpDBNameMandatory | N/A | taosAdapter requires the database name to be specified in the URL |
