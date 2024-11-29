---
sidebar_label: taosKeeper
title: taosKeeper Reference
slug: /tdengine-reference/components/taoskeeper
---

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";

taosKeeper is a tool for exporting monitoring metrics in TDengine version 3.0, allowing you to obtain the operational status of TDengine with just a few simple configurations. taosKeeper uses the TDengine RESTful interface, so there's no need to install the TDengine client.

## Installation

taosKeeper can be installed in two ways:

- It is automatically installed when you install the official TDengine installation package. For details, please refer to [TDengine Installation](../../../get-started/).
- You can also compile and install taosKeeper separately. For more details, please refer to the [taosKeeper](https://github.com/taosdata/taoskeeper) repository.

## Configuration

taosKeeper needs to be executed in the operating system terminal. This tool supports three configuration methods: command line parameters, environment variables, and configuration files. The priority order is: command line parameters, environment variables, configuration file parameters. Generally, we recommend using a configuration file.

### Command Line Parameters and Environment Variables

The command line parameters and environment variable descriptions can be referenced from the output of the command `taoskeeper --help`. Here’s an example:

```shell
Usage of taosKeeper v3.3.2.0:
      --debug                          enable debug mode. Env "TAOS_KEEPER_DEBUG"
  -P, --port int                       http port. Env "TAOS_KEEPER_PORT" (default 6043)
      --logLevel string                log level (panic fatal error warn warning info debug trace). Env "TAOS_KEEPER_LOG_LEVEL" (default "info")
      --gopoolsize int                 coroutine size. Env "TAOS_KEEPER_POOL_SIZE" (default 50000)
  -R, --RotationInterval string        interval for refresh metrics, such as "300ms", Valid time units are "ns", "us" (or "µs"), "ms", "s", "m", "h". Env "TAOS_KEEPER_ROTATION_INTERVAL" (default "15s")
      --tdengine.host string           TDengine server's ip. Env "TAOS_KEEPER_TDENGINE_HOST" (default "127.0.0.1")
      --tdengine.port int              TDengine REST server(taosAdapter)'s port. Env "TAOS_KEEPER_TDENGINE_PORT" (default 6041)
      --tdengine.username string       TDengine server's username. Env "TAOS_KEEPER_TDENGINE_USERNAME" (default "root")
      --tdengine.password string       TDengine server's password. Env "TAOS_KEEPER_TDENGINE_PASSWORD" (default "taosdata")
      --tdengine.usessl                TDengine server use ssl or not. Env "TAOS_KEEPER_TDENGINE_USESSL"
      --metrics.prefix string          prefix in metrics names. Env "TAOS_KEEPER_METRICS_PREFIX"
      --metrics.database.name string   database for storing metrics data. Env "TAOS_KEEPER_METRICS_DATABASE" (default "log")
      --metrics.tables stringArray     export some tables that are not super table, multiple values split with white space. Env "TAOS_KEEPER_METRICS_TABLES"
      --environment.incgroup           whether running in cgroup. Env "TAOS_KEEPER_ENVIRONMENT_INCGROUP"
      --log.path string                log path. Env "TAOS_KEEPER_LOG_PATH" (default "/var/log/taos")
      --log.rotationCount uint         log rotation count. Env "TAOS_KEEPER_LOG_ROTATION_COUNT" (default 5)
      --log.rotationTime duration      log rotation time. Env "TAOS_KEEPER_LOG_ROTATION_TIME" (default 24h0m0s)
      --log.rotationSize string        log rotation size(KB MB GB), must be a positive integer. Env "TAOS_KEEPER_LOG_ROTATION_SIZE" (default "100000000")
  -c, --config string                  config path default /etc/taos/taoskeeper.toml
  -V, --version                        Print the version and exit
  -h, --help                           Print this help message and exit
```

### Configuration File

taosKeeper supports specifying a configuration file using the command `taoskeeper -c <keeper config file>`.

If you do not specify a configuration file, taosKeeper will use the default configuration file, which is located at `/etc/taos/taoskeeper.toml`.

If neither the taosKeeper configuration file is specified, nor does `/etc/taos/taoskeeper.toml` exist, the default configuration will be used.

**Here is an example configuration file:**

```toml
# Start with debug middleware for gin
debug = false

# Listen port, default is 6043
port = 6043

# log level
loglevel = "info"

# go pool size
gopoolsize = 50000

# interval for metrics
RotationInterval = "15s"

[tdengine]
host = "127.0.0.1"
port = 6041
username = "root"
password = "taosdata"
usessl = false

[metrics]
# metrics prefix in metrics names.
prefix = "taos"

# export some tables that are not super table
tables = []

# database for storing metrics data
[metrics.database]
name = "log"
# database options for db storing metrics data
[metrics.database.options]
vgroups = 1
buffer = 64
KEEP = 90
cachemodel = "both"

[environment]
# Whether running in cgroup.
incgroup = false

[log]
rotationCount = 5
rotationTime = "24h"
rotationSize = 100000000
```

## Start

**Before running taosKeeper, ensure that the TDengine cluster and taosAdapter are running correctly.** Additionally, monitoring services must be enabled in the TDengine configuration file `taos.cfg`, requiring at least the configuration of `monitor` and `monitorFqdn`.

```shell
monitor 1
monitorFqdn localhost # FQDN of the taoskeeper service
```

For more details on TDengine monitoring configuration, please refer to: [Monitor Your Cluster](../../../operations-and-maintenance/monitor-your-cluster/).

<Tabs>
<TabItem label="Linux" value="linux">

After installation, use the `systemctl` command to start the taoskeeper service process.

```bash
systemctl start taoskeeper
```

Check if the service is working properly:

```bash
systemctl status taoskeeper
```

If the service process is active, the status command will display the following related information:

```text
Active: active (running)
```

If the background service process is stopped, the status command will display the following related information:

```text
Active: inactive (dead)
```

The following `systemctl` commands can help you manage the taoskeeper service:

- Start the service process: `systemctl start taoskeeper`
- Stop the service process: `systemctl stop taoskeeper`
- Restart the service process: `systemctl restart taoskeeper`
- View service status: `systemctl status taoskeeper`

:::info

- The `systemctl` command requires _root_ permissions to run. If you are not a _root_ user, please add `sudo` before the command.
- If your system does not support `systemd`, you can manually run `/usr/local/taos/bin/taoskeeper` to start the taoskeeper service.
- Troubleshooting: If the service is abnormal, please check the logs for more information. The log files are stored by default in `/var/log/taos`.

:::

</TabItem>

<TabItem label="macOS" value="macOS">

After installation, you can run `sudo launchctl start com.tdengine.taoskeeper` to start the taoskeeper service process.

The following `launchctl` commands are used to manage the taoskeeper service:

- Start the service process: `sudo launchctl start com.tdengine.taoskeeper`
- Stop the service process: `sudo launchctl stop com.tdengine.taoskeeper`
- View service status: `sudo launchctl list | grep taoskeeper`

:::info

- The `launchctl` command requires administrator privileges to manage `com.tdengine.taoskeeper`, so make sure to add `sudo` before it for security.
- The first column returned by `sudo launchctl list | grep taoskeeper` is the PID of the `taoskeeper` program. If it shows `-`, it means the taoskeeper service is not running.
- Troubleshooting: If the service is abnormal, please check the logs for more information. The log files are stored by default in `/var/log/taos`.

:::

</TabItem>
</Tabs>

## Health Check

You can access the taosKeeper `check_health` interface to determine if the service is alive. If the service is normal, it will return an HTTP 200 status code:

```shell
curl -i http://127.0.0.1:6043/check_health
```

Return result:

```text
HTTP/1.1 200 OK
Content-Type: application/json; charset=utf-8
Date: Wed, 07 Aug 2024 06:19:50 GMT
Content-Length: 21

{"version":"3.3.2.3"}
```

## Data Collection and Monitoring

taosKeeper, as a tool for exporting TDengine monitoring metrics, can record the monitoring data generated by TDengine in a specified database (default monitoring data is `log`). This monitoring data can be used to configure TDengine monitoring.

### Viewing Monitoring Data

You can view the supertables in the `log` database, with each supertable corresponding to a set of monitoring metrics, which will not be detailed further.

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

You can check the most recent report record of a supertable, such as:

``` shell
taos> select last_row(*) from taosd_dnodes_info;
      last_row(_ts)      |   last_row(disk_engine)   |  last_row(system_net_in)  |   last_row(vnodes_num)    | last_row(system_net_out)  |     last_row(uptime)      |    last_row(has_mnode)    |  last_row(io_read_disk)   | last_row(error_log_count) |     last_row(io_read)     |    last_row(cpu_cores)    |    last_row(has_qnode)    |    last_row(has_snode)    |   last_row(disk_total)    |   last_row(mem_engine)    | last_row(info_log_count)  |   last_row(cpu_engine)    |  last_row(io_write_disk)  | last_row(debug_log_count) |    last_row(disk_used)    |    last_row(mem_total)    |    last_row(io_write)     |     last_row(masters)     |   last_row(cpu_system)    | last_row(trace_log_count) |    last_row(mem_free)     |
======================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================
 2024-08-07 14:54:09.174 |         0.000000000000000 |      3379.093240947399863 |        37.000000000000000 |      5265.998201139278535 |     64402.000000000000000 |         1.000000000000000 |      8323.261934108399146 |         6.000000000000000 |     40547.386655118425551 |        16.000000000000000 |         0.000000000000000 |         0.000000000000000 |     5.272955781120000e+11 |   2443032.000000000000000 |       423.000000000000000 |         0.556269622200215 |    677731.836503547732718 |    356380.000000000000000 |     4.997186764800000e+10 |  65557284.000000000000000 |    714177.054532129666768 |        37.000000000000000 |         2.642280705451021 |         0.000000000000000 |  11604276.000000000000000 |
Query OK, 1 row(s) in set (0.003168s)
```

### Using TDInsight to Configure Monitoring

Once monitoring data has been collected, you can use TDInsight to configure monitoring for TDengine. For specific details, please refer to the [TDInsight Reference Manual](../tdinsight/).

## Integrating Prometheus

taosKeeper provides the `/metrics` interface, which returns monitoring data in Prometheus format. Prometheus can extract monitoring data from taosKeeper to monitor TDengine.

### Exporting Monitoring Metrics

The following curl command shows the data format returned by the `/metrics` interface:

```shell
curl http://127.0.0.1:6043/metrics
```

Partial result set:

```shell
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

### Monitoring Metrics Details

#### taosd Cluster

##### Supported Labels for Monitoring Information

- `cluster_id`: Cluster ID

##### Related Metrics and Their Meaning

| Metric Name                             | Type    | Meaning                              |
| --------------------------------------- | ------- | ------------------------------------ |
| taos_cluster_info_connections_total     | counter | Total number of connections           |
| taos_cluster_info_dbs_total             | counter | Total number of databases             |
| taos_cluster_info_dnodes_alive          | counter | Number of alive dnodes               |
| taos_cluster_info_dnodes_total          | counter | Total number of dnodes                |
| taos_cluster_info_first_ep              | gauge   | First endpoint; the label value indicates the endpoint value |
| taos_cluster_info_first_ep_dnode_id     | counter | The dnode ID of the first endpoint   |
| taos_cluster_info_master_uptime         | gauge   | Uptime of the master node (in days) |
| taos_cluster_info_mnodes_alive          | counter | Number of alive mnodes               |
| taos_cluster_info_mnodes_total          | counter | Total number of mnodes               |
| taos_cluster_info_stbs_total            | counter | Total number of supertables          |
| taos_cluster_info_streams_total         | counter | Total number of streams               |
| taos_cluster_info_tbs_total             | counter | Total number of tables                |
| taos_cluster_info_topics_total          | counter | Total number of topics                |
| taos_cluster_info_version               | gauge   | Version information; the label value indicates the version number |
| taos_cluster_info_vgroups_alive         | counter | Number of alive virtual groups        |
| taos_cluster_info_vgroups_total         | counter | Total number of virtual groups        |
| taos_cluster_info_vnodes_alive          | counter | Number of alive virtual nodes         |
| taos_cluster_info_vnodes_total          | counter | Total number of virtual nodes         |
| taos_grants_info_expire_time            | counter | Remaining time until cluster authorization expires (in seconds) |
| taos_grants_info_timeseries_total       | counter | Total number of time series allowed for cluster authorization |
| taos_grants_info_timeseries_used        | counter | Number of time series already owned by the cluster |

#### dnode

##### Supported Labels for Monitoring Information

- `cluster_id`: Cluster ID
- `dnode_ep`: dnode endpoint
- `dnode_id`: dnode ID

##### Related Metrics and Their Meaning

| Metric Name                          | Type    | Meaning                                                                                  |
| ------------------------------------- | ------- | ---------------------------------------------------------------------------------------- |
| taos_d_info_status                    | gauge   | dnode status; the label value indicates the status: ready (normal), offline (down), unknown (unknown). |
| taos_dnodes_info_cpu_cores            | gauge   | Number of CPU cores                                                                     |
| taos_dnodes_info_cpu_engine           | gauge   | CPU percentage used by this dnode's process (range: 0-100)                             |
| taos_dnodes_info_cpu_system           | gauge   | CPU percentage used by the system on which this dnode resides (range: 0-100)           |
| taos_dnodes_info_disk_engine          | counter | Disk capacity used by this dnode's process (in Bytes)                                   |
| taos_dnodes_info_disk_total           | counter | Total disk capacity of the node where this dnode resides (in Bytes)                     |
| taos_dnodes_info_disk_used            | counter | Disk capacity used by the node where this dnode resides (in Bytes)                      |
| taos_dnodes_info_has_mnode            | counter | Whether there is an mnode                                                              |
| taos_dnodes_info_has_qnode            | counter | Whether there is a qnode                                                              |
| taos_dnodes_info_has_snode            | counter | Whether there is an snode                                                              |
| taos_dnodes_info_io_read              | gauge   | I/O read rate of the node where this dnode resides (in Bytes/s)                         |
| taos_dnodes_info_io_read_disk         | gauge   | Disk I/O write rate of the node where this dnode resides (in Bytes/s)                   |
| taos_dnodes_info_io_write             | gauge   | I/O write rate of the node where this dnode resides (in Bytes/s)                        |
| taos_dnodes_info_io_write_disk        | gauge   | Disk I/O write rate of the node where this dnode resides (in Bytes/s)                   |
| taos_dnodes_info_masters              | counter | Number of master nodes                                                                  |
| taos_dnodes_info_mem_engine           | counter | Memory used by this dnode's process (in KB)                                            |
| taos_dnodes_info_mem_system           | counter | Memory used by the system where this dnode resides (in KB)                             |
| taos_dnodes_info_mem_total            | counter | Total memory of the node where this dnode resides (in KB)                              |
| taos_dnodes_info_net_in               | gauge   | Network inbound rate of the node where this dnode resides (in Bytes/s)                  |
| taos_dnodes_info_net_out              | gauge   | Network outbound rate of the node where this dnode resides (in Bytes/s)                 |
| taos_dnodes_info_uptime               | gauge   | Uptime of this dnode (in seconds)                                                      |
| taos_dnodes_info_vnodes_num           | counter | Number of vnodes on the node where this dnode resides                                   |

#### Data Directory

##### Supported Labels for Monitoring Information

- `cluster_id`: Cluster ID
- `dnode_ep`: dnode endpoint
- `dnode_id`: dnode ID
- `data_dir_name`: Data directory name
- `data_dir_level`: Data directory level

##### Related Metrics and Their Meaning

| Metric Name                          | Type  | Meaning                     |
| ------------------------------------- | ----- | --------------------------- |
| taos_taosd_dnodes_data_dirs_avail    | gauge | Available space (in Bytes)  |
| taos_taosd_dnodes_data_dirs_total    | gauge | Total space (in Bytes)      |
| taos_taosd_dnodes_data_dirs_used     | gauge | Used space (in Bytes)       |

#### Log Directory

##### Supported Labels for Monitoring Information

- `cluster_id`: Cluster ID
- `dnode_ep`: dnode endpoint
- `dnode_id`: dnode ID
- `log_dir_name`: Log directory name

##### Related Metrics and Their Meaning

| Metric Name                          | Type  | Meaning                     |
| ------------------------------------- | ----- | --------------------------- |
| taos_taosd_dnodes_log_dirs_avail     | gauge | Available space (in Bytes)  |
| taos_taosd_dnodes_log_dirs_total     | gauge | Total space (in Bytes)      |
| taos_taosd_dnodes_log_dirs_used      | gauge | Used space (in Bytes)       |

#### Log Count

##### Supported Labels for Monitoring Information

- `cluster_id`: Cluster ID
- `dnode_ep`: dnode endpoint
- `dnode_id`: dnode ID

##### Related Metrics and Their Meaning

| Metric Name                          | Type    | Meaning                   |
| ------------------------------------- | ------- | ------------------------- |
| taos_log_summary_debug                | counter | Debug log count           |
| taos_log_summary_error                | counter | Error log count           |
| taos_log_summary_info                 | counter | Info log count            |
| taos_log_summary_trace                | counter | Trace log count           |

#### taosadapter

##### Supported Labels for Monitoring Information

- `endpoint`: Endpoint
- `req_type`: Request type, where 0 indicates REST, and 1 indicates WebSocket

##### Related Metrics and Their Meaning

| Metric Name                               | Type    | Meaning                      |
| ------------------------------------------ | ------- | ----------------------------- |
| taos_adapter_requests_fail                 | counter | Number of failed requests     |
| taos_adapter_requests_in_process           | counter | Number of requests in process |
| taos_adapter_requests_other                | counter | Number of other types of requests |
| taos_adapter_requests_other_fail           | counter | Number of other types of failed requests |
| taos_adapter_requests_other_success        | counter | Number of other types of successful requests |
| taos_adapter_requests_query                | counter | Number of query requests       |
| taos_adapter_requests_query_fail           | counter | Number of query failed requests |
| taos_adapter_requests_query_in_process     | counter | Number of queries in process   |
| taos_adapter_requests_query_success        | counter | Number of successful query requests |
| taos_adapter_requests_success              | counter | Number of successful requests   |
| taos_adapter_requests_total                | counter | Total number of requests       |
| taos_adapter_requests_write                | counter | Number of write requests       |
| taos_adapter_requests_write_fail           | counter | Number of write failed requests |
| taos_adapter_requests_write_in_process     | counter | Number of write requests in process |
| taos_adapter_requests_write_success        | counter | Number of successful write requests |

#### taoskeeper

##### Supported Labels for Monitoring Information

- `identify`: Node endpoint

##### Related Metrics and Their Meaning

| Metric Name                | Type  | Meaning                             |
| --------------------------- | ----- | ----------------------------------- |
| taos_keeper_monitor_cpu     | gauge | taoskeeper CPU usage (range: 0~1)  |
| taos_keeper_monitor_mem     | gauge | taoskeeper memory usage (range: 0~1) |

#### Other taosd Cluster Monitoring Items

##### taos_m_info_role

- **Labels**:
  - `cluster_id`: Cluster ID
  - `mnode_ep`: mnode endpoint
  - `mnode_id`: mnode ID
  - `value`: Role value (the status of this mnode; range: offline, follower, candidate, leader, error, learner)
- **Type**: gauge
- **Meaning**: mnode role

##### taos_taos_sql_req_count

- **Labels**:
  - `cluster_id`: Cluster ID
  - `result`: Request result (range: Success, Failed)
  - `sql_type`: SQL type (range: select, insert, inserted_rows, delete)
  - `username`: Username
- **Type**: gauge
- **Meaning**: SQL request count

##### taos_taosd_sql_req_count

- **Labels**:
  - `cluster_id`: Cluster ID
  - `dnode_ep`: dnode endpoint
  - `dnode_id`: dnode ID
  - `result`: Request result (range: Success, Failed)
  - `sql_type`: SQL type (range: select, insert, inserted_rows, delete)
  - `username`: Username
  - `vgroup_id`: Virtual group ID
- **Type**: gauge
- **Meaning**: SQL request count

##### taos_taosd_vgroups_info_status

- **Labels**:
  - `cluster_id`: Cluster ID
  - `database_name`: Database name
  - `vgroup_id`: Virtual group ID
- **Type**: gauge
- **Meaning**: Virtual group status. 0 means unsynced, indicating no leader has been elected; 1 means ready.

##### taos_taosd_vgroups_info_tables_num

- **Labels**:
  - `cluster_id`: Cluster ID
  - `database_name`: Database name
  - `vgroup_id`: Virtual group ID
- **Type**: gauge
- **Meaning**: Number of tables in the virtual group.

##### taos_taosd_vnodes_info_role

- **Labels**:
  - `cluster_id`: Cluster ID
  - `database_name`: Database name
  - `dnode_id`: dnode ID
  - `value`: Role value (range: offline, follower, candidate, leader, error, learner)
  - `vgroup_id`: Virtual group ID
- **Type**: gauge
- **Meaning**: Virtual node role

### Scrape Configuration

Prometheus provides the `scrape_configs` configuration to extract monitoring data from an endpoint. Typically, you only need to modify the targets configuration in `static_configs` to the endpoint address of taoskeeper. For more configuration information, please refer to the [Prometheus Configuration Documentation](https://prometheus.io/docs/prometheus/latest/configuration/configuration/#scrape_config).

```text
# A scrape configuration containing exactly one endpoint to scrape:
# Here it's Prometheus itself.
scrape_configs:
  - job_name: "taoskeeper"
    # metrics_path defaults to '/metrics'
    # scheme defaults to 'http'.
    static_configs:
      - targets: ["localhost:6043"]
```

### Dashboard

We provide the `TaosKeeper Prometheus Dashboard for 3.x`, which offers a monitoring dashboard similar to TDInsight.

In the Grafana Dashboard menu, click `import`, enter the dashboard ID `18587`, and click the `Load` button to import the `TaosKeeper Prometheus Dashboard for 3.x`.

## taosKeeper Monitoring Metrics

taosKeeper will also write its collected monitoring data to the monitoring database, with the default being the `log` database, which can be modified in the taoskeeper configuration file.

### keeper_monitor Table

The `keeper_monitor` table records monitoring data from taoskeeper.

| field    | type      | is_tag | comment         |
| :------- | :-------- | :----- | :-------------- |
| ts       | TIMESTAMP |        | timestamp       |
| cpu      | DOUBLE    |        | CPU usage rate  |
| mem      | DOUBLE    |        | Memory usage rate|
| identify | NCHAR     | TAG    | Identity information |
