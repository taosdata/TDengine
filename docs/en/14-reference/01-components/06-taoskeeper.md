---
sidebar_label: taosKeeper
title: taosKeeper Reference
slug: /tdengine-reference/components/taoskeeper
toc_max_heading_level: 4
---

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";

taosKeeper is a monitoring metric export tool for TDengine version 3.0, which can obtain the running status of TDengine with a few simple configurations. taosKeeper uses the TDengine RESTful interface, so there is no need to install the TDengine client to use it.

## Installation

There are two ways to install taosKeeper:

- taosKeeper is automatically installed with the official TDengine installation package, for details please refer to [TDengine Installation](../../../get-started/).

- Compile and install taosKeeper separately, for details please refer to the [taosKeeper](https://github.com/taosdata/taoskeeper) repository.

## Configuration

taosKeeper needs to be executed in the operating system terminal, and this tool supports three configuration methods: command line parameters, environment variables, and configuration files. The priority is: command line parameters, environment variables, configuration file parameters. Generally, we recommend using the configuration file.

### Command Line Parameters and Environment Variables

For explanations of command line parameters and environment variables, refer to the output of the command `taoskeeper --help`. Below is an example:

```shell
Usage of taoskeeper:
  -R, --RotationInterval string                      interval for refresh metrics, such as "300ms", Valid time units are "ns", "us" (or "µs"), "ms", "s", "m", "h". Env "TAOS_KEEPER_ROTATION_INTERVAL" (default "15s")
  -c, --config string                                config path default /etc/taos/taoskeeper.toml
      --drop string                                  run taoskeeper in command mode, only support old_taosd_metric_stables. 
      --environment.incgroup                         whether running in cgroup. Env "TAOS_KEEPER_ENVIRONMENT_INCGROUP"
      --fromTime string                              parameter of transfer, example: 2020-01-01T00:00:00+08:00 (default "2020-01-01T00:00:00+08:00")
      --gopoolsize int                               coroutine size. Env "TAOS_KEEPER_POOL_SIZE" (default 50000)
  -h, --help                                         Print this help message and exit
  -H, --host string                                  http host. Env "TAOS_KEEPER_HOST"
      --instanceId int                               instance ID. Env "TAOS_KEEPER_INSTANCE_ID" (default 64)
      --log.compress                                 whether to compress old log. Env "TAOS_KEEPER_LOG_COMPRESS"
      --log.keepDays uint                            log retention days, must be a positive integer. Env "TAOS_KEEPER_LOG_KEEP_DAYS" (default 3)
      --log.level string                             log level (trace debug info warning error). Env "TAOS_KEEPER_LOG_LEVEL" (default "info")
      --log.path string                              log path. Env "TAOS_KEEPER_LOG_PATH" (default "/var/log/taos")
      --log.reservedDiskSize string                  reserved disk size for log dir (KB MB GB), must be a positive integer. Env "TAOS_KEEPER_LOG_RESERVED_DISK_SIZE" (default "1GB")
      --log.rotationCount uint                       log rotation count. Env "TAOS_KEEPER_LOG_ROTATION_COUNT" (default 3)
      --log.rotationSize string                      log rotation size(KB MB GB), must be a positive integer. Env "TAOS_KEEPER_LOG_ROTATION_SIZE" (default "1GB")
      --log.rotationTime duration                    deprecated: log rotation time always 24 hours. Env "TAOS_KEEPER_LOG_ROTATION_TIME" (default 24h0m0s)
      --logLevel string                              log level (trace debug info warning error). Env "TAOS_KEEPER_LOG_LEVEL" (default "info")
      --metrics.database.name string                 database for storing metrics data. Env "TAOS_KEEPER_METRICS_DATABASE" (default "log")
      --metrics.database.options.buffer int          database option buffer for audit database. Env "TAOS_KEEPER_METRICS_BUFFER" (default 64)
      --metrics.database.options.cachemodel string   database option cachemodel for audit database. Env "TAOS_KEEPER_METRICS_CACHEMODEL" (default "both")
      --metrics.database.options.keep int            database option buffer for audit database. Env "TAOS_KEEPER_METRICS_KEEP" (default 90)
      --metrics.database.options.vgroups int         database option vgroups for audit database. Env "TAOS_KEEPER_METRICS_VGROUPS" (default 1)
      --metrics.prefix string                        prefix in metrics names. Env "TAOS_KEEPER_METRICS_PREFIX"
      --metrics.tables stringArray                   export some tables that are not super table, multiple values split with white space. Env "TAOS_KEEPER_METRICS_TABLES"
  -P, --port int                                     http port. Env "TAOS_KEEPER_PORT" (default 6043)
      --prometheus.cacheTTL int                      memory cache TTL in v2 mode (in seconds, default: 300). Env "TAOS_KEEPER_CACHE_TTL" (default 300)
      --prometheus.includeTables stringArray         additional tables to cache in v2 memory mode (e.g., taosd_write_metrics). Env "TAOS_KEEPER_INCLUDE_TABLES"
      --ssl.certFile string                          ssl cert file path. Env "TAOS_KEEPER_SSL_CERT_FILE"
      --ssl.enable                                   enable ssl. Env "TAOS_KEEPER_SSL_ENABLE"
      --ssl.keyFile string                           ssl key file path. Env "TAOS_KEEPER_SSL_KEY_FILE"
      --tdengine.host string                         TDengine server's ip. Env "TAOS_KEEPER_TDENGINE_HOST" (default "127.0.0.1")
      --tdengine.password string                     TDengine server's password. Env "TAOS_KEEPER_TDENGINE_PASSWORD" (default "taosdata")
      --tdengine.port int                            TDengine REST server(taosAdapter)'s port. Env "TAOS_KEEPER_TDENGINE_PORT" (default 6041)
      --tdengine.username string                     TDengine server's username. Env "TAOS_KEEPER_TDENGINE_USERNAME" (default "root")
      --tdengine.usessl                              TDengine server use ssl or not. Env "TAOS_KEEPER_TDENGINE_USESSL"
      --transfer string                              run taoskeeper in command mode, only support old_taosd_metric. transfer old metrics data to new tables and exit
  -V, --version                                      Print the version and exit
```

### Configuration File

taosKeeper supports specifying a configuration file with the command `taoskeeper -c <keeper config file>`.
If no configuration file is specified, taosKeeper will use the default configuration file located at: `/etc/taos/taoskeeper.toml`.
If neither a taosKeeper configuration file is specified nor does `/etc/taos/taoskeeper.toml` exist, the default configuration will be used.

Below is an example of the configuration file:

```toml
# The ID of the currently running taoskeeper instance, default is 64.
instanceId = 64

# Listening host, supports IPv4/Ipv6, default is ""
host = ""
# Listening port, default is 6043.
port = 6043

# Go pool size
gopoolsize = 50000

# Interval for metrics
RotationInterval = "15s"

[tdengine]
host = "127.0.0.1"
port = 6041
username = "root"
password = "taosdata"
usessl = false

[metrics]
# Metrics prefix in metrics names.
prefix = "taos"

# Export some tables that are not supertable.
tables = []

# Database for storing metrics data.
[metrics.database]
name = "log"

# Database options for db storing metrics data.
[metrics.database.options]
vgroups = 1
buffer = 64
keep = 90
cachemodel = "both"

# prometheus v2 memory cache mode configuration
[prometheus]
# Additional tables to cache (restore from default exclusion list)
# Example: includeTables = ["taosd_write_metrics", "adapter_c_interface"]
includeTables = []

# TTL for cached metrics in memory (in seconds)
# Data older than this will be automatically cleaned up
# Default: 300 (5 minutes), Minimum: 60 (1 minute)
cacheTTL = 300

[environment]
# Whether running in cgroup.
incgroup = false

[log]
# The directory where log files are stored.
# path = "/var/log/taos"
level = "info"
# Number of log file rotations before deletion.
rotationCount = 3
# The number of days to retain log files.
keepDays = 3
# The maximum size of a log file before rotation.
rotationSize = "1GB"
# If set to true, log files will be compressed.
compress = false
# Minimum disk space to reserve. Log files will not be written if disk space falls below this limit.
reservedDiskSize = "1GB"
```

## Startup

**Before running taosKeeper, ensure that the TDengine cluster and taosAdapter are already running correctly.** Additionally, monitoring services must be enabled in TDengine, with at least `monitor` and `monitorFqdn` configured in the TDengine configuration file `taos.cfg`.

```shell
monitor 1
monitorFqdn localhost # FQDN for taoskeeper service
```

For details on TDengine monitoring configuration, please refer to: [TDengine Monitoring Configuration](../../../operations-and-maintenance/monitor-your-cluster/).

<Tabs>
<TabItem label="Linux" value="linux">

After installation, please use the `systemctl` command to start the taoskeeper service process.

```shell
systemctl start taoskeeper
```

Check if the service is working properly:

```shell
systemctl status taoskeeper
```

If the service process is active, the status command will display the following information:

```text
Active: active (running)
```

If the background service process is stopped, the status command will display the following information:

```text
Active: inactive (dead)
```

The following `systemctl` commands can help you manage the taoskeeper service:

- Start the service process: `systemctl start taoskeeper`

- Stop the service process: `systemctl stop taoskeeper`

- Restart the service process: `systemctl restart taoskeeper`

- Check the service status: `systemctl status taoskeeper`

:::info

- `systemctl` commands require _root_ permissions to run. If you are not a _root_ user, please add `sudo` before the command.
- If the system does not support `systemd`, you can also manually run `/usr/local/taos/bin/taoskeeper` to start the taoskeeper service.
- Troubleshooting: If the service is abnormal, please check the log for more information. Log files are by default located in `/var/log/taos`.

:::
</TabItem>

<TabItem label="macOS" value="macOS">

After installation, you can run `sudo launchctl start com.tdengine.taoskeeper` to start the taoskeeper service process.

The following `launchctl` commands are used to manage the taoskeeper service:

- Start the service process: `sudo launchctl start com.tdengine.taoskeeper`

- Stop the service process: `sudo launchctl stop com.tdengine.taoskeeper`

- Check the service status: `sudo launchctl list | grep taoskeeper`

:::info

- `launchctl` commands managing `com.tdengine.taoskeeper` require administrator privileges, always use `sudo` to enhance security.
- The command `sudo launchctl list | grep taoskeeper` returns the PID of the `taoskeeper` program in the first column. If it is `-`, it indicates that the taoskeeper service is not running.
- Troubleshooting: If the service is abnormal, please check the log for more information. Log files are by default located in `/var/log/taos`.

:::

</TabItem>
</Tabs>

## Health Check

You can access the taosKeeper's `check_health` interface to determine if the service is alive. If the service is normal, it will return an HTTP 200 status code:

```shell
curl -i http://127.0.0.1:6043/check_health
```

Response:

```text
HTTP/1.1 200 OK
Content-Type: application/json; charset=utf-8
Date: Wed, 07 Aug 2024 06:19:50 GMT
Content-Length: 21

{"version":"3.3.2.3"}
```

## Data Collection and Monitoring

taosKeeper, as a tool for exporting monitoring metrics of TDengine, can record the monitoring data generated by TDengine in a specified database (the default monitoring data is `log`). These monitoring data can be used to configure TDengine monitoring.

### Viewing Monitoring Data

You can view the supertables under the `log` database, each supertable corresponds to a set of monitoring metrics, specific metrics are not further described.

```shell
taos> use log;
Database changed.

taos> show stables;
          stable_name           |
=================================
 taosd_dnodes_status            |
 taosd_vnodes_info              |
 keeper_monitor                 |
 taosd_vgroups_info             |
 taos_sql_req                   |
 taos_slow_sql                  |
 taosd_mnodes_info              |
 taosd_cluster_info             |
 taosd_sql_req                  |
 taosd_dnodes_info              |
 adapter_requests               |
 taosd_cluster_basic            |
 taosd_dnodes_data_dirs         |
 taosd_dnodes_log_dirs          |
Query OK, 14 row(s) in set (0.006542s)

```

You can view the most recent report record of a supertable, such as:

```shell
taos> select last_row(*) from taosd_dnodes_info;
      last_row(_ts)      |   last_row(disk_engine)   |  last_row(system_net_in)  |   last_row(vnodes_num)    | last_row(system_net_out)  |     last_row(uptime)      |    last_row(has_mnode)    |  last_row(io_read_disk)   | last_row(error_log_count) |     last_row(io_read)     |    last_row(cpu_cores)    |    last_row(has_qnode)    |    last_row(has_snode)    |   last_row(disk_total)    |   last_row(mem_engine)    | last_row(info_log_count)  |   last_row(cpu_engine)    |  last_row(io_write_disk)  | last_row(debug_log_count) |    last_row(disk_used)    |    last_row(mem_total)    |    last_row(io_write)     |     last_row(masters)     |   last_row(cpu_system)    | last_row(trace_log_count) |    last_row(mem_free)     |
======================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================
 2024-08-07 14:54:09.174 |         0.000000000000000 |      3379.093240947399863 |        37.000000000000000 |      5265.998201139278535 |     64402.000000000000000 |         1.000000000000000 |      8323.261934108399146 |         6.000000000000000 |     40547.386655118425551 |        16.000000000000000 |         0.000000000000000 |         0.000000000000000 |     5.272955781120000e+11 |   2443032.000000000000000 |       423.000000000000000 |         0.556269622200215 |    677731.836503547732718 |    356380.000000000000000 |     4.997186764800000e+10 |  65557284.000000000000000 |    714177.054532129666768 |        37.000000000000000 |         2.642280705451021 |         0.000000000000000 |  11604276.000000000000000 |
Query OK, 1 row(s) in set (0.003168s)
```

### Configuring Monitoring with TDInsight

After collecting monitoring data, you can use TDInsight to configure monitoring for TDengine. For details, please refer to the [TDinsight Reference Manual](../tdinsight/).

## Integrating Prometheus

taoskeeper provides `/metrics/v2` and `/metrics` endpoints, which return monitoring data in Prometheus format. Prometheus can scrape monitoring data from taoskeeper to monitor TDengine.

The differences between the two endpoints are as follows. The `/metrics/v2` endpoint is recommended:

| Feature | /metrics | /metrics/v2 |
|---------|----------|-------------|
| Data Source | Real-time query from TDengine | Memory cache |
| Metric Completeness | Only partial early metrics | Latest complete metrics |
| Performance | Lower | Higher |
| Data Latency | No latency | Up to one monitoring cycle delay |
| Data Completeness | Complete for single instance | **Requires configuring all instance endpoints** |
| Default Cached Tables | None | Some tables excluded by default, can be included via configuration |

### /metrics/v2 Endpoint

#### How It Works

The v2 endpoint uses a memory cache mode, storing metric data in memory to avoid querying the TDengine database for each request. Data is automatically expired and cleaned up according to the configured `cacheTTL`.

**Important**: In v2 memory cache mode, the TDengine cluster may shard monitoring data and report it to different taoskeeper instances. Therefore, a single taoskeeper instance only receives partial metric data, and **you must configure all taoskeeper instance endpoints in Prometheus** to obtain complete monitoring data.

#### Configuration

Add the `[prometheus]` configuration section in the configuration file:

```toml
[prometheus]
# Additional tables to cache (restore from default exclusion list)
# Example: includeTables = ["taosd_write_metrics", "adapter_c_interface"]
includeTables = []

# TTL for cached metrics in memory (in seconds)
# Data older than this will be automatically cleaned up
# Default: 300 (5 minutes), Minimum: 60 (1 minute)
cacheTTL = 300
```

#### Usage Example

```bash
curl http://127.0.0.1:6043/metrics/v2?max_age=30
```

Partial result set:

```bash
# HELP taosd_cluster_info_dbs_total Metric from taosd_cluster_info
# TYPE taosd_cluster_info_dbs_total gauge
taosd_cluster_info_dbs_total{cluster_id="2564125112573221874"} 13
# HELP taosd_cluster_info_dnodes_alive Metric from taosd_cluster_info
# TYPE taosd_cluster_info_dnodes_alive gauge
taosd_cluster_info_dnodes_alive{cluster_id="2564125112573221874"} 1
# HELP taosd_cluster_info_dnodes_total Metric from taosd_cluster_info
# TYPE taosd_cluster_info_dnodes_total gauge
taosd_cluster_info_dnodes_total{cluster_id="2564125112573221874"} 1
# HELP taosd_cluster_info_grants_expire_time Metric from taosd_cluster_info
# TYPE taosd_cluster_info_grants_expire_time gauge
taosd_cluster_info_grants_expire_time{cluster_id="2564125112573221874"} 0
```

### /metrics Endpoint

**Important**: This endpoint is deprecated and not recommended for use.

Below, the data format returned by the `/metrics` endpoint is demonstrated using the `curl` command:

```bash
curl http://127.0.0.1:6043/metrics
```

Partial result set:

```bash
# HELP taos_cluster_info_connections_total
# TYPE taos_cluster_info_connections_total counter
taos_cluster_info_connections_total{cluster_id="554014120921134497"} 8
# HELP taos_cluster_info_dbs_total
# TYPE taos_cluster_info_dbs_total counter
taos_cluster_info_dbs_total{cluster_id="554014120921134497"} 2
# HELP taos_cluster_info_dnodes_alive
# TYPE taos_cluster_info_dnodes_alive counter
taos_cluster_info_dnodes_alive{cluster_id="554014120921134497"} 1
# HELP taos_cluster_info_dnodes_total
# TYPE taos_cluster_info_dnodes_total counter
taos_cluster_info_dnodes_total{cluster_id="554014120921134497"} 1
# HELP taos_cluster_info_first_ep
# TYPE taos_cluster_info_first_ep gauge
taos_cluster_info_first_ep{cluster_id="554014120921134497",value="tdengine:6030"} 1
# HELP taos_cluster_info_first_ep_dnode_id
# TYPE taos_cluster_info_first_ep_dnode_id counter
taos_cluster_info_first_ep_dnode_id{cluster_id="554014120921134497"} 1
```

### Scrape Configuration

Prometheus provides `scrape_configs` to configure how to scrape monitoring data from endpoints.

**Multi-Instance Mode (/metrics/v2)**:

If using the `/metrics/v2` endpoint (memory cache mode), since each taoskeeper instance only receives partial metric data, **you must configure all taoskeeper instance endpoints in Prometheus**, and Prometheus will automatically aggregate data from different instances.

```yaml
scrape_configs:
  - job_name: "taoskeeper-v2"
    metrics_path: '/metrics/v2'
    params:
      max_age: ['30']  # Optional: only return metric data from the last 30 seconds
    static_configs:
      - targets:
        - "taoskeeper-1:6043"
        - "taoskeeper-2:6043"
        - "taoskeeper-3:6043"
        # Add all endpoints according to actual deployment
```

**Parameter Description**:

- `max_age` (optional): Filter the time range of cached data (in seconds), only returning metric data from the recent specified number of seconds. Default value is 30 seconds.

**Single-Instance Mode (/metrics)**:

If using the `/metrics` endpoint, you only need to configure one taoskeeper endpoint to get all monitoring data.

```yaml
scrape_configs:
  - job_name: "taoskeeper"
    metrics_path: '/metrics'
    static_configs:
      - targets: ["taoskeeper-1:6043"]
```

For more configuration information, please refer to [Prometheus Configuration Documentation](https://prometheus.io/docs/prometheus/latest/configuration/configuration/#scrape_config).

---

## /metrics/v2 Metric Details

This section lists all monitoring metrics returned by the `/metrics/v2` endpoint.  

### Cluster Information

#### Supported Tags

- `cluster_id`: Cluster ID
- `first_ep`: Cluster first ep (string label)
- `cluster_version`: Cluster deployed binary version (string label)

#### Related Metrics and Their Meanings

| Metric Name                              | Type    | Meaning                       |
| ----------------------------------------- | ------- | ---------------------------- |
| taosd_cluster_basic_first_ep_dnode_id     | counter | Cluster first ep's dnode id |

### Cluster Monitoring

#### Supported Tags

- `cluster_id`: Cluster ID

#### Related Metrics and Their Meanings

| Metric Name                              | Type    | Meaning                                   |
| ----------------------------------------- | ------- | ------------------------------------------ |
| taosd_cluster_info_cluster_uptime         | gauge   | Cluster uptime (seconds)                   |
| taosd_cluster_info_dbs_total              | gauge   | Number of databases in the cluster         |
| taosd_cluster_info_tbs_total              | gauge   | Number of tables in the cluster            |
| taosd_cluster_info_stbs_total             | gauge   | Number of supertables in the cluster       |
| taosd_cluster_info_dnodes_total           | gauge   | Number of dnodes in the cluster            |
| taosd_cluster_info_dnodes_alive           | gauge   | Number of dnodes in active running state   |
| taosd_cluster_info_mnodes_total           | gauge   | Number of mnodes in the cluster            |
| taosd_cluster_info_mnodes_alive           | gauge   | Number of mnodes in active running state   |
| taosd_cluster_info_vgroups_total          | gauge   | Number of vgroups in the cluster           |
| taosd_cluster_info_vgroups_alive          | gauge   | Number of vgroups in active running state  |
| taosd_cluster_info_vnodes_total           | gauge   | Number of vnodes in the cluster            |
| taosd_cluster_info_vnodes_alive           | gauge   | Number of vnodes in active running state   |
| taosd_cluster_info_connections_total      | gauge   | Number of connections                      |
| taosd_cluster_info_topics_total           | gauge   | Number of topics in the cluster            |
| taosd_cluster_info_streams_total          | gauge   | Number of streams in the cluster           |
| taosd_cluster_info_grants_expire_time     | gauge   | Cluster authorization expiration remaining time (seconds) |
| taosd_cluster_info_grants_timeseries_used | gauge   | Number of time series currently owned by the cluster |
| taosd_cluster_info_grants_timeseries_total | gauge   | Total number of time series allowed by cluster authorization |

### vgroup

#### Supported Tags

- `cluster_id`: Cluster ID
- `vgroup_id`: Virtual group ID
- `database_name`: Database name

#### Related Metrics and Their Meanings

| Metric Name                        | Type    | Meaning                                                   |
| ----------------------------------- | ------- | ---------------------------------------------------------- |
| taosd_vgroups_info_tables_num       | gauge   | Number of tables in this vgroup                           |
| taosd_vgroups_info_status           | gauge   | Status of this vgroup, range: unsynced = 0, ready = 1     |

### dnode

#### Supported Tags

- `cluster_id`: Cluster ID
- `dnode_ep`: dnode endpoint
- `dnode_id`: dnode ID

#### Related Metrics and Their Meanings

| Metric Name                            | Type    | Meaning                                                                    |
| --------------------------------------- | ------- | --------------------------------------------------------------------------- |
| taosd_dnodes_info_uptime                | gauge   | Uptime of this dnode (seconds)                                              |
| taosd_dnodes_info_cpu_engine            | gauge   | CPU used by the process on this dnode (%, multiplied by 100)                |
| taosd_dnodes_info_cpu_system            | gauge   | CPU used by the system on this dnode (%, multiplied by 100)                 |
| taosd_dnodes_info_cpu_cores             | gauge   | Number of CPU cores on this dnode                                           |
| taosd_dnodes_info_mem_engine            | gauge   | Memory used by the process on this dnode (KB)                               |
| taosd_dnodes_info_mem_free              | gauge   | Free physical memory on this dnode (KB)                                     |
| taosd_dnodes_info_mem_total             | gauge   | Total memory on this dnode (KB)                                             |
| taosd_dnodes_info_mem_cache_buffer      | counter | Cache/buffer memory on this dnode (KB)                                      |
| taosd_dnodes_info_disk_used             | gauge   | Used disk capacity on this dnode (Byte)                                     |
| taosd_dnodes_info_disk_total            | gauge   | Total disk capacity on this dnode (Byte)                                    |
| taosd_dnodes_info_disk_engine           | counter | Disk capacity used by the process on this dnode (Byte)                      |
| taosd_dnodes_info_system_net_in         | gauge   | Network incoming rate on this dnode (Byte/s)                                |
| taosd_dnodes_info_system_net_out        | gauge   | Network outgoing rate on this dnode (Byte/s)                                |
| taosd_dnodes_info_io_read               | gauge   | IO read rate on this dnode (Byte/s)                                         |
| taosd_dnodes_info_io_write              | gauge   | IO write rate on this dnode (Byte/s)                                        |
| taosd_dnodes_info_io_read_disk          | gauge   | Disk IO read rate on this dnode (Byte/s)                                    |
| taosd_dnodes_info_io_write_disk         | gauge   | Disk IO write rate on this dnode (Byte/s)                                   |
| taosd_dnodes_info_vnodes_num            | gauge   | Number of vnodes on this dnode                                              |
| taosd_dnodes_info_masters               | gauge   | Number of vnodes in leader state on this dnode                              |
| taosd_dnodes_info_has_mnode             | gauge   | Whether there is an Mnode on this dnode, range: yes = 1, no = 0            |
| taosd_dnodes_info_has_qnode             | gauge   | Whether there is a qnode on this dnode, range: yes = 1, no = 0             |
| taosd_dnodes_info_has_snode             | gauge   | Whether there is an snode on this dnode, range: yes = 1, no = 0             |
| taosd_dnodes_info_has_bnode             | gauge   | Whether there is a bnode on this dnode, range: yes = 1, no = 0             |
| taosd_dnodes_info_errors                | gauge   | Number of errors on this dnode                                              |
| taosd_dnodes_info_error_log_count       | gauge   | Number of error logs on this dnode                                          |
| taosd_dnodes_info_info_log_count        | gauge   | Number of info logs on this dnode                                           |
| taosd_dnodes_info_debug_log_count       | gauge   | Number of debug logs on this dnode                                          |
| taosd_dnodes_info_trace_log_count       | gauge   | Number of trace logs on this dnode                                          |

### dnode Status

#### Supported Tags

- `cluster_id`: Cluster ID
- `dnode_ep`: dnode endpoint
- `dnode_id`: dnode ID

#### Related Metrics and Their Meanings

| Metric Name                        | Type    | Meaning                                                 |
| ----------------------------------- | ------- | -------------------------------------------------------- |
| taosd_dnodes_status_status          | gauge   | Status of this dnode, range: ready = 1, offline = 0     |

### Data Directory

#### Supported Tags

- `cluster_id`: Cluster ID
- `dnode_ep`: dnode endpoint
- `dnode_id`: dnode ID
- `data_dir_name`: Data directory name
- `data_dir_level`: Data directory level

#### Related Metrics and Their Meanings

| Metric Name                        | Type    | Meaning                    |
| ----------------------------------- | ------- | --------------------------- |
| taosd_dnodes_data_dirs_avail       | gauge   | Available space (Byte)      |
| taosd_dnodes_data_dirs_used        | gauge   | Used space (Byte)           |
| taosd_dnodes_data_dirs_total       | gauge   | Total space (Byte)          |

### Log Directory

#### Supported Tags

- `cluster_id`: Cluster ID
- `dnode_ep`: dnode endpoint
- `dnode_id`: dnode ID
- `log_dir_name`: Log directory name

#### Related Metrics and Their Meanings

| Metric Name                        | Type    | Meaning                    |
| ----------------------------------- | ------- | --------------------------- |
| taosd_dnodes_log_dirs_avail        | gauge   | Available space (Byte)      |
| taosd_dnodes_log_dirs_used         | gauge   | Used space (Byte)           |
| taosd_dnodes_log_dirs_total        | gauge   | Total space (Byte)          |

### dnode Metrics

#### Supported Tags

- `cluster_id`: Cluster ID
- `dnode_ep`: dnode endpoint
- `dnode_id`: dnode ID

#### Related Metrics and Their Meanings

| Metric Name                              | Type    | Meaning                                             |
| ----------------------------------------- | ------- | ---------------------------------------------------- |
| taosd_dnodes_metrics_apply_memory_allowed | gauge   | Apply queue maximum allowed memory (Byte)            |
| taosd_dnodes_metrics_apply_memory_used    | gauge   | Apply queue currently used memory (Byte)             |
| taosd_dnodes_metrics_rpc_queue_memory_allowed | gauge | RPC queue maximum allowed memory (Byte)            |
| taosd_dnodes_metrics_rpc_queue_memory_used | gauge  | RPC queue currently used memory (Byte)             |

### Write Metrics

#### Supported Tags

- `cluster_id`: Cluster ID
- `dnode_ep`: dnode endpoint
- `dnode_id`: dnode ID
- `vgroup_id`: Virtual group ID

#### Related Metrics and Their Meanings

| Metric Name                            | Type    | Meaning                                                  |
| --------------------------------------- | ------- | --------------------------------------------------------- |
| taosd_write_metrics_total_rows          | gauge   | Number of rows processed by vnode                         |
| taosd_write_metrics_wal_write_time      | gauge   | Vnode WAL write time (ms)                                 |
| taosd_write_metrics_commit_count        | gauge   | Number of commit requests processed by vnode              |
| taosd_write_metrics_commit_time         | gauge   | Vnode commit request processing time (ms)                 |
| taosd_write_metrics_blocked_commit_count | gauge  | Number of times vnode commit was blocked                  |
| taosd_write_metrics_blocked_commit_time  | gauge  | Time vnode was blocked during commit (ms)                  |
| taosd_write_metrics_merge_count         | gauge   | Number of merge requests processed by vnode               |
| taosd_write_metrics_merge_time          | gauge   | Vnode merge request processing time (ms)                  |
| taosd_write_metrics_last_cache_commit_time | gauge | Vnode last cache commit time (ms)                         |
| taosd_write_metrics_last_cache_commit_count | gauge | Number of vnode last cache commits                      |

### Performance Metrics

#### Supported Tags

- `cluster_id`: Cluster ID

#### Related Metrics and Their Meanings

| Metric Name                        | Type    | Meaning                                  |
| ----------------------------------- | ------- | ----------------------------------------- |
| taosd_perf_slow_queries_total      | gauge   | Total number of slow queries             |
| taosd_perf_very_slow_queries_total | gauge   | Number of slow queries with exec_time > 300ms |

### mnode

#### Supported Tags

- `cluster_id`: Cluster ID
- `mnode_ep`: mnode endpoint
- `mnode_id`: mnode ID

#### Related Metrics and Their Meanings

| Metric Name                | Type    | Meaning                                                                   |
| --------------------------- | ------- | ------------------------------------------------------------------------- |
| taosd_mnodes_info_role      | gauge   | Role of this mnode, range: offline = 0, follower = 100, candidate = 101 |

### vnode

#### Supported Tags

- `cluster_id`: Cluster ID
- `vgroup_id`: Virtual group ID
- `database_name`: Database name
- `dnode_id`: dnode ID

#### Related Metrics and Their Meanings

| Metric Name                | Type    | Meaning                                                                                                    |
| --------------------------- | ------- | ----------------------------------------------------------------------------------------------------------- |
| taosd_vnodes_info_role      | gauge   | Role of this vnode, range: offline = 0, follower = 100, candidate = 101, leader = 102, error = 103, learner = 104 |

### Server-Side SQL Requests

#### Supported Tags

- `cluster_id`: Cluster ID
- `dnode_ep`: dnode endpoint
- `dnode_id`: dnode ID
- `vgroup_id`: Virtual group ID
- `username`: Username
- `sql_type`: SQL type (range: select, insert, inserted_rows, delete)
- `result`: Request result (range: Success, Failed)

For `inserted_rows` type SQL, the `result` range is only `Success`.

#### Related Metrics and Their Meanings

| Metric Name                | Type    | Meaning         |
| --------------------------- | ------- | ---------------- |
| taosd_sql_req_count         | counter | Request count    |

### Client-Side SQL Requests

#### Supported Tags

- `cluster_id`: Cluster ID
- `username`: Username
- `sql_type`: SQL type (range: select, insert, delete)
- `result`: Request result (range: Success, Failed)

#### Related Metrics and Their Meanings

| Metric Name                | Type    | Meaning         |
| --------------------------- | ------- | ---------------- |
| taos_sql_req_count          | counter | Request count    |

### Slow SQL

#### Supported Tags

- `cluster_id`: Cluster ID
- `username`: Username
- `result`: Request result (range: Success, Failed, Cancel)
- `duration`: Slow SQL duration level marker (range: 3-10s, 10-100s, 100-1000s, 1000s-)

#### Related Metrics and Their Meanings

| Metric Name                | Type    | Meaning         |
| --------------------------- | ------- | ---------------- |
| taos_slow_sql_count         | counter | Request count    |

### taosadapter

#### Supported Tags

- `endpoint`: Endpoint
- `req_type`: Request type, 0 for rest, 1 for websocket

#### Related Metrics and Their Meanings

| Metric Name                            | Type    | Meaning                                  |
| -------------------------------------- | ------- | ---------------------------------------- |
| taos_adapter_requests_fail             | counter | Number of failed requests                |
| taos_adapter_requests_in_process       | counter | Number of requests in process            |
| taos_adapter_requests_other            | counter | Number of other type requests            |
| taos_adapter_requests_other_fail       | counter | Number of failed other type requests     |
| taos_adapter_requests_other_success    | counter | Number of successful other type requests |
| taos_adapter_requests_query            | counter | Number of query requests                 |
| taos_adapter_requests_query_fail       | counter | Number of failed query requests          |
| taos_adapter_requests_query_in_process | counter | Number of queries in process             |
| taos_adapter_requests_query_success    | counter | Number of successful query requests      |
| taos_adapter_requests_success          | counter | Number of successful requests            |
| taos_adapter_requests_total            | counter | Total number of requests                 |
| taos_adapter_requests_write            | counter | Number of write requests                 |
| taos_adapter_requests_write_fail       | counter | Number of failed write requests          |
| taos_adapter_requests_write_in_process | counter | Number of writes in process              |
| taos_adapter_requests_write_success    | counter | Number of successful write requests      |

### taoskeeper {#taoskeeper-1}

#### Supported Tags

- `identify`: Node endpoint

#### Related Metrics and Their Meanings

| Metric Name             | Type  | Meaning                                  |
| ----------------------- | ----- | ---------------------------------------- |
| taos_keeper_monitor_cpu | gauge | taoskeeper CPU usage rate (range 0~1)    |
| taos_keeper_monitor_mem | gauge | taoskeeper memory usage rate (range 0~1) |

---

### /metrics Metric Details

#### taosd Cluster

##### Supported Tags for Monitoring Information

- `cluster_id`: Cluster ID

##### Related Metrics and Their Meanings

| Metric Name                         | Type    | Meaning                                                         |
| ----------------------------------- | ------- | --------------------------------------------------------------- |
| taos_cluster_info_connections_total | counter | Total number of connections                                     |
| taos_cluster_info_dbs_total         | counter | Total number of databases                                       |
| taos_cluster_info_dnodes_alive      | counter | Number of alive dnodes                                          |
| taos_cluster_info_dnodes_total      | counter | Total number of dnodes                                          |
| taos_cluster_info_first_ep          | gauge   | First endpoint, label value indicates endpoint value            |
| taos_cluster_info_first_ep_dnode_id | counter | dnode ID of the first endpoint                                  |
| taos_cluster_info_master_uptime     | gauge   | Master node uptime in days                                      |
| taos_cluster_info_mnodes_alive      | counter | Number of alive mnodes                                          |
| taos_cluster_info_mnodes_total      | counter | Total number of mnodes                                          |
| taos_cluster_info_stbs_total        | counter | Total number of supertables                                     |
| taos_cluster_info_streams_total     | counter | Total number of streams                                         |
| taos_cluster_info_tbs_total         | counter | Total number of tables                                          |
| taos_cluster_info_topics_total      | counter | Total number of topics                                          |
| taos_cluster_info_version           | gauge   | Version information, label value indicates version number       |
| taos_cluster_info_vgroups_alive     | counter | Number of alive virtual groups                                  |
| taos_cluster_info_vgroups_total     | counter | Total number of virtual groups                                  |
| taos_cluster_info_vnodes_alive      | counter | Number of alive virtual nodes                                   |
| taos_cluster_info_vnodes_total      | counter | Total number of virtual nodes                                   |
| taos_grants_info_expire_time        | counter | Remaining time until cluster authorization expires (in seconds) |
| taos_grants_info_timeseries_total   | counter | Total number of time-series allowed by cluster authorization    |
| taos_grants_info_timeseries_used    | counter | Number of time-series currently owned by the cluster            |

#### dnode

##### Supported Tags for Monitoring Information

- `cluster_id`: Cluster id
- `dnode_ep`: dnode endpoint
- `dnode_id`: dnode id

##### Relevant Metrics and Their Meanings

| Metric Name                    | Type    | Meaning                                                                                                               |
| ------------------------------ | ------- | --------------------------------------------------------------------------------------------------------------------- |
| taos_d_info_status             | gauge   | dnode status, the label value indicates the status, ready means normal, offline means offline, unknown means unknown. |
| taos_dnodes_info_cpu_cores     | gauge   | Number of CPU cores                                                                                                   |
| taos_dnodes_info_cpu_engine    | gauge   | Percentage of CPU used by the process on this dnode (range 0~100)                                                     |
| taos_dnodes_info_cpu_system    | gauge   | Percentage of CPU used by the system on this dnode (range 0~100)                                                      |
| taos_dnodes_info_disk_engine   | counter | Disk capacity used by the process on this dnode (unit Byte)                                                           |
| taos_dnodes_info_disk_total    | counter | Total disk capacity of this dnode (unit Byte)                                                                         |
| taos_dnodes_info_disk_used     | counter | Disk capacity used on this dnode (unit Byte)                                                                          |
| taos_dnodes_info_has_mnode     | counter | Whether there is an mnode                                                                                             |
| taos_dnodes_info_has_qnode     | counter | Whether there is a qnode                                                                                              |
| taos_dnodes_info_has_snode     | counter | Whether there is an snode                                                                                             |
| taos_dnodes_info_io_read       | gauge   | IO read rate of this dnode (unit Byte/s)                                                                              |
| taos_dnodes_info_io_read_disk  | gauge   | Disk IO read rate of this dnode (unit Byte/s)                                                                         |
| taos_dnodes_info_io_write      | gauge   | IO write rate of this dnode (unit Byte/s)                                                                             |
| taos_dnodes_info_io_write_disk | gauge   | Disk IO write rate of this dnode (unit Byte/s)                                                                        |
| taos_dnodes_info_masters       | counter | Number of master nodes                                                                                                |
| taos_dnodes_info_mem_engine    | counter | Memory used by the process on this dnode (unit KB)                                                                    |
| taos_dnodes_info_mem_system    | counter | Memory used by the system on this dnode (unit KB)                                                                     |
| taos_dnodes_info_mem_total     | counter | Total memory of this dnode (unit KB)                                                                                  |
| taos_dnodes_info_net_in        | gauge   | Network incoming rate of this dnode (unit Byte/s)                                                                     |
| taos_dnodes_info_net_out       | gauge   | Network outgoing rate of this dnode (unit Byte/s)                                                                     |
| taos_dnodes_info_uptime        | gauge   | Uptime of this dnode (unit seconds)                                                                                   |
| taos_dnodes_info_vnodes_num    | counter | Number of vnodes on this dnode                                                                                        |

#### Data Directory

##### Supported Tags for Monitoring Information

- `cluster_id`: Cluster id
- `dnode_ep`: dnode endpoint
- `dnode_id`: dnode id
- `data_dir_name`: Data directory name
- `data_dir_level`: Data directory level

##### Related Metrics and Their Meanings

| Metric Name                       | Type  | Meaning                   |
| --------------------------------- | ----- | ------------------------- |
| taos_taosd_dnodes_data_dirs_avail | gauge | Available space (in Byte) |
| taos_taosd_dnodes_data_dirs_total | gauge | Total space (in Byte)     |
| taos_taosd_dnodes_data_dirs_used  | gauge | Used space (in Byte)      |

#### Log Directory

##### Supported Tags for Monitoring Information

- `cluster_id`: Cluster id
- `dnode_ep`: dnode endpoint
- `dnode_id`: dnode id
- `log_dir_name`: Log directory name

##### Related Metrics and Their Meanings

| Metric Name                      | Type  | Meaning                   |
| -------------------------------- | ----- | ------------------------- |
| taos_taosd_dnodes_log_dirs_avail | gauge | Available space (in Byte) |
| taos_taosd_dnodes_log_dirs_total | gauge | Total space (in Byte)     |
| taos_taosd_dnodes_log_dirs_used  | gauge | Used space (in Byte)      |

#### Log Count

##### Supported Tags for Monitoring Information

- `cluster_id`: Cluster id
- `dnode_ep`: dnode endpoint
- `dnode_id`: dnode id

##### Related Metrics and Their Meanings

| Metric Name            | Type    | Meaning              |
| ---------------------- | ------- | -------------------- |
| taos_log_summary_debug | counter | Number of debug logs |
| taos_log_summary_error | counter | Number of error logs |
| taos_log_summary_info  | counter | Number of info logs  |
| taos_log_summary_trace | counter | Number of trace logs |

#### taosadapter

Same as /metrics/v2 endpoint. See [taosadapter](#taosadapter) section above.

#### taoskeeper

Same as /metrics/v2 endpoint. See [taoskeeper](#taoskeeper-1) section above.

#### Other taosd Cluster Monitoring Items

##### taos_m_info_role

- **Tags**:
  - `cluster_id`: Cluster id
  - `mnode_ep`: mnode endpoint
  - `mnode_id`: mnode id
  - `value`: Role value (the state of this mnode, range: offline, follower, candidate, leader, error, learner)
- **Type**: gauge
- **Meaning**: mnode role

##### taos_taos_sql_req_count

- **Tags**:
  - `cluster_id`: Cluster id
  - `result`: Request result (range: Success, Failed)
  - `sql_type`: SQL type (range: select, insert, inserted_rows, delete)
  - `username`: Username
- **Type**: gauge
- **Meaning**: Number of SQL requests

##### taos_taosd_sql_req_count

- **Tags**:
  - `cluster_id`: Cluster id
  - `dnode_ep`: dnode endpoint
  - `dnode_id`: dnode id
  - `result`: Request result (range: Success, Failed)
  - `sql_type`: SQL type (range: select, insert, inserted_rows, delete)
  - `username`: Username
  - `vgroup_id`: Virtual group id
- **Type**: gauge
- **Meaning**: Number of SQL requests

##### taos_taosd_vgroups_info_status

- **Tags**:
  - `cluster_id`: Cluster id
  - `database_name`: Database name
  - `vgroup_id`: Virtual group id
- **Type**: gauge
- **Meaning**: Virtual group status. 0 for unsynced, indicating no leader elected; 1 for ready.

##### taos_taosd_vgroups_info_tables_num

- **Tags**:
  - `cluster_id`: Cluster id
  - `database_name`: Database name
  - `vgroup_id`: Virtual group id
- **Type**: gauge
- **Meaning**: Number of tables in the virtual group

##### taos_taosd_vnodes_info_role

- **Tags**:
  - `cluster_id`: Cluster id
  - `database_name`: Database name
  - `dnode_id`: dnode id
  - `value`: Role value (range: offline, follower, candidate, leader, error, learner)
  - `vgroup_id`: Virtual group id
- **Type**: gauge
- **Meaning**: Virtual node role

### Dashboard

We provide the `TaosKeeper Prometheus Dashboard for 3.x` dashboard, which offers a monitoring dashboard similar to TDinsight.

In the Grafana Dashboard menu, click `import`, enter dashboard ID `18587`, and click the `Load` button to import the `TaosKeeper Prometheus Dashboard for 3.x` dashboard.

## taosKeeper Monitoring Metrics

taosKeeper also writes the monitoring data it collects into a monitoring database, which by default is the `log` database. This can be modified in the taoskeeper configuration file.

### keeper_monitor table

The `keeper_monitor` table records taoskeeper monitoring data.

| field    | type      | is_tag | comment              |
| :------- | :-------- | :----- | :------------------- |
| ts       | TIMESTAMP |        | timestamp            |
| cpu      | DOUBLE    |        | CPU usage rate       |
| mem      | DOUBLE    |        | Memory usage rate    |
| identify | NCHAR     | TAG    | Identity information |
