---
sidebar_label: taosKeeper
title: taosKeeper Reference Guide
description: This document describes how to use taosKeeper, a tool for exporting TDengine monitoring metrics.
---

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";

## Introduction

taosKeeper is a tool for TDengine that exports monitoring metrics. With taosKeeper, you can easily monitor the operational status of your TDengine deployment. taosKeeper uses the TDengine REST API. It is not necessary to install TDengine Client to use taosKeeper.

## Installation

There are two ways to install taosKeeper:
Methods of installing taosKeeper:

- Installing the official TDengine installer will automatically install taosKeeper. 

- You can compile taosKeeper separately and install it. Please refer to the [taosKeeper](https://github.com/taosdata/taoskeeper) repository for details.
## Configuration and Launch

### Configuration

taosKeeper needs to be executed on the terminal of the operating system, it supports three configuration methods: Command-line arguments, environment variable and configuration file. The precedence of those is Command-line, environment variable and configuration file.

**Make sure that the TDengine cluster is running correctly before running taosKeeper.** Ensure that the monitoring service in TDengine has been started. At least the values of `monitor` and `monitorFqdn` need to be set in `taos.cfg`.

```shell
monitor 1
monitorFqdn localhost # taoskeeper's FQDN
```

For more information, see [TDengine Monitoring Configuration](../../config/#monitoring-parameters).

### Quick Launch

<Tabs>
<TabItem label="Linux" value="linux">

After the installation is complete, run the following command to start the taoskeeper service:

```bash
systemctl start taoskeeper
```

Run the following command to confirm that taoskeeper is running normally:

```bash
systemctl status taoskeeper
```

Output similar to the following indicates that taoskeeper is running normally:

```
Active: active (running)
```

Output similar to the following indicates that taoskeeper has not started successfully:

```
Active: inactive (dead)
```

The following `systemctl` commands can help you manage taoskeeper service:

- Start taoskeeper Server: `systemctl start taoskeeper`

- Stop taoskeeper Server: `systemctl stop taoskeeper`

- Restart taoskeeper Server: `systemctl restart taoskeeper`

- Check taoskeeper Server status: `systemctl status taoskeeper`

:::info

- The `systemctl` command requires _root_ privileges. If you are not logged in as the _root_ user, use the `sudo` command.
- The `systemctl stop taoskeeper` command will instantly stop taoskeeper Server.
- If your system does not include `systemd`, you can run `/usr/local/taos/bin/taoskeeper` to start taoskeeper manually.

:::
</TabItem>

<TabItem label="macOS" value="macos">

After the installation is complete, run `launchctl start com.tdengine.taoskeeper` to start taoskeeper Server.

The following `launchctl` commands can help you manage taoskeeper service:

- Start taoskeeper Server: `sudo launchctl start com.tdengine.taoskeeper`

- Stop taoskeeper Server: `sudo launchctl stop com.tdengine.taoskeeper`

- Check taoskeeper Server status: `sudo launchctl list | grep taoskeeper`

:::info
- Please use `sudo` to run `launchctl` to manage _com.tdengine.taoskeeper_ with administrator privileges.
- The administrator privilege is required for service management to enhance security.
- Troubleshooting:
- The first column returned by the command `launchctl list | grep taoskeeper` is the PID of the program. If it's `-`, that means the taoskeeper service is not running.
- If the service is abnormal, please check the `launchd.log` file from the system log.

:::

</TabItem>
</Tabs>

#### Launch With Configuration File

You can quickly launch taosKeeper with the following commands. If you do not specify a configuration file, `/etc/taos/taoskeeper.toml` is used by default. If this file does not specify configurations, the default values are used.

```shell
$ taoskeeper -c <keeper config file>
```

**Sample configuration files**
```toml
# enable debug in gin framework
debug = false

# listen to server port, default 6043
port = 6043

# set log level to panic, error, info, debug, or trace
loglevel = "info"

# set pool size
gopoolsize = 50000

# query rotation period for TDengine monitoring data
RotationInterval = "15s"

[tdengine]
host = "127.0.0.1"
port = 6041
username = "root"
password = "taosdata"

[metrics]
# monitoring metric prefix
prefix = "taos"

# database to store monitoring data
database = "log"

# standard tables to monitor
tables = ["normal_table"]

# database options for db storing metrics data
[metrics.databaseoptions]
cachemodel = "none"

[environment]
# Whether running in cgroup.
incgroup = false

[log]
# rotation file num
rotationCount = 5
# rotation on time
rotationTime = "24h"
# rotation on file size (bytes)
rotationSize = 100000000

```

### Obtain Monitoring Metrics

taosKeeper records monitoring metrics generated by TDengine in a specified database and provides an interface through which you can export the data.

#### View Monitoring Results

```shell
$ taos
# the log database is used in this example
> use log;
> select * from taosd_cluster_info limit 1;
```

Example result set:

```shell
           _ts           |      cluster_uptime       |         dbs_total         |         tbs_total         |        stbs_total         |       vgroups_total       |       vgroups_alive       |       vnodes_total        |       vnodes_alive        |       mnodes_total        |       mnodes_alive        |     connections_total     |       topics_total        |       streams_total       |       dnodes_total        |       dnodes_alive        |    grants_expire_time     |  grants_timeseries_used   |  grants_timeseries_total  |           cluster_id           |
===================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================
 2024-06-04 03:03:34.341 |         0.000000000000000 |         2.000000000000000 |         1.000000000000000 |         4.000000000000000 |         4.000000000000000 |         4.000000000000000 |         4.000000000000000 |         4.000000000000000 |         1.000000000000000 |         1.000000000000000 |         2.000000000000000 |         0.000000000000000 |         0.000000000000000 |         1.000000000000000 |         1.000000000000000 |         0.000000000000000 |         3.000000000000000 |         0.000000000000000 | 554014120921134497             |
Query OK, 1 row(s) in set (0.001652s)
```

#### Export Monitoring Metrics

```shell
$ curl http://127.0.0.1:6043/metrics
```

Sample result set (excerpt):

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

### check\_health 

```
$ curl -i http://127.0.0.1:6043/check_health
```

Response:

```
HTTP/1.1 200 OK
Content-Type: application/json; charset=utf-8
Date: Mon, 03 Apr 2023 07:20:38 GMT
Content-Length: 19

{"version":"1.0.0"}
```

### taoskeeper with Prometheus

There is `/metrics` api in taoskeeper provide TDengine metric data for Prometheus. 

#### scrape config

Scrape config in Prometheus specifies a set of targets and parameters describing how to scrape metric data from endpoint. For more information, please reference to [Prometheus documents](https://prometheus.io/docs/prometheus/latest/configuration/configuration/#scrape_config).

```
# A scrape configuration containing exactly one endpoint to scrape:
# Here it's Prometheus itself.
scrape_configs:
  - job_name: "taoskeeper"
    # metrics_path defaults to '/metrics'
    # scheme defaults to 'http'.
    static_configs:
      - targets: ["localhost:6043"]
```

#### Dashboard

There is a dashboard named `TaosKeeper Prometheus Dashboard for 3.x`, which provides a monitoring dashboard similar to TInsight.

In Grafana, click the Dashboard menu and click `import`, enter the dashboard ID `18587` and click the `Load` button. Then finished importing `TaosKeeper Prometheus Dashboard for 3.x` dashboard.

