---
sidebar_label: taosKeeper
title: taosKeeper
description: TDengine 3.0 版本监控指标的导出工具
---

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";

## 简介

taosKeeper 是 TDengine 3.0 版本监控指标的导出工具，通过简单的几项配置即可获取 TDengine 的运行状态。taosKeeper 使用 TDengine RESTful 接口，所以不需要安装 TDengine 客户端即可使用。

## 安装

taosKeeper 有两种安装方式：
taosKeeper 安装方式：

- 安装 TDengine 官方安装包的同时会自动安装 taosKeeper, 详情请参考[ TDengine 安装](../../get-started/)。

- 单独编译 taosKeeper 并安装，详情请参考 [taosKeeper](https://github.com/taosdata/taoskeeper) 仓库。

## 配置和运行方式

### 配置

taosKeeper 需要在操作系统终端执行，该工具支持三种配置方式：[命令行参数](#命令行参数启动)、[环境变量](#环境变量启动) 和 [配置文件](#配置文件启动)。优先级为：命令行参数、环境变量、配置文件参数。

**在运行 taosKeeper 之前要确保 TDengine 集群与 taosAdapter 已经在正确运行。** 并且 TDengine 已经开启监控服务，TDengine 配置文件 `taos.cfg` 中至少需要配置 `monitor` 和 `monitorFqdn`。

```shell
monitor 1
monitorFqdn localhost # taoskeeper 服务的 FQDN
```

TDengine 监控配置相关，具体请参考：[TDengine 监控配置](../config/#监控相关)。


### 启动

<Tabs>
<TabItem label="Linux" value="linux">

安装后，请使用 `systemctl` 命令来启动 taoskeeper 的服务进程。

```bash
systemctl start taoskeeper
```

检查服务是否正常工作：

```bash
systemctl status taoskeeper
```

如果服务进程处于活动状态，则 status 指令会显示如下的相关信息：

```
Active: active (running)
```

如果后台服务进程处于停止状态，则 status 指令会显示如下的相关信息：

```
Active: inactive (dead)
```

如下 `systemctl` 命令可以帮助你管理 taoskeeper 服务：

- 启动服务进程：`systemctl start taoskeeper`

- 停止服务进程：`systemctl stop taoskeeper`

- 重启服务进程：`systemctl restart taoskeeper`

- 查看服务状态：`systemctl status taoskeeper`

:::info

- `systemctl` 命令需要 _root_ 权限来运行，如果您非 _root_ 用户，请在命令前添加 `sudo`。
- 如果系统中不支持 `systemd`，也可以用手动运行 `/usr/local/taos/bin/taoskeeper` 方式启动 taoskeeper 服务。
- 故障排查：
- 如果服务异常请查看系统日志获取更多信息。
:::
</TabItem>

<TabItem label="macOS" value="macOS">

安装后，可以运行 `sudo launchctl start com.tdengine.taoskeeper` 来启动 taoskeeper 服务进程。

如下 `launchctl` 命令用于管理 taoskeeper 服务：

- 启动服务进程：`sudo launchctl start com.tdengine.taoskeeper`

- 停止服务进程：`sudo launchctl stop com.tdengine.taoskeeper`

- 查看服务状态：`sudo launchctl list | grep taoskeeper`

:::info

- `launchctl` 命令管理`com.tdengine.taoskeeper`需要管理员权限，务必在前面加 `sudo` 来增强安全性。
- `sudo launchctl list | grep taoskeeper` 指令返回的第一列是 `taoskeeper` 程序的 PID，若为 `-` 则说明 taoskeeper 服务未运行。
- 故障排查：
- 如果服务异常请查看系统日志获取更多信息。

:::

</TabItem>
</Tabs>


#### 配置文件启动

执行以下命令即可快速体验 taosKeeper。当不指定 taosKeeper 配置文件时，优先使用 `/etc/taos/taoskeeper.toml` 配置，否则将使用默认配置。

```shell
$ taoskeeper -c <keeper config file>
```

**下面是配置文件的示例：**
```toml
# gin 框架是否启用 debug
debug = false

# 服务监听端口, 默认为 6043
port = 6043

# 日志级别，包含 panic、error、info、debug、trace等
loglevel = "info"

# 程序中使用协程池的大小
gopoolsize = 50000

# 查询 TDengine 监控数据轮询间隔
RotationInterval = "15s"

[tdengine]
host = "127.0.0.1"
port = 6041
username = "root"
password = "taosdata"

# 需要被监控的 taosAdapter
[taosAdapter]
address = ["127.0.0.1:6041"]

[metrics]
# 监控指标前缀
prefix = "taos"

# 集群数据的标识符
cluster = "production"

# 存放监控数据的数据库
database = "log"

# 指定需要监控的普通表
tables = []

# database options for db storing metrics data
[metrics.databaseoptions]
cachemodel = "none"
```

### 获取监控指标

taosKeeper 作为 TDengine 监控指标的导出工具，可以将 TDengine 产生的监控数据记录在指定数据库中，并提供导出接口。

#### 查看监控结果集

```shell
$ taos
# 如上示例，使用 log 库作为监控日志存储位置
> use log;
> select * from cluster_info limit 1;
```

结果示例：

```shell
           ts            |            first_ep            | first_ep_dnode_id |   version    |    master_uptime     | monitor_interval |  dbs_total  |  tbs_total  | stbs_total  | dnodes_total | dnodes_alive | mnodes_total | mnodes_alive | vgroups_total | vgroups_alive | vnodes_total | vnodes_alive | connections_total |  protocol   |           cluster_id           |
===============================================================================================================================================================================================================================================================================================================================================================================
 2022-08-16 17:37:01.629 | hlb:6030                       |                 1 | 3.0.0.0      |              0.27250 |               15 |           2 |          27 |          38 |            1 |            1 |            1 |            1 |             4 |             4 |            4 |            4 |                14 |           1 | 5981392874047724755            |
Query OK, 1 rows in database (0.036162s)
```

#### 导出监控指标

```shell
$ curl http://127.0.0.1:6043/metrics
```

部分结果集：

```shell
# HELP taos_cluster_info_connections_total
# TYPE taos_cluster_info_connections_total counter
taos_cluster_info_connections_total{cluster_id="5981392874047724755"} 16
# HELP taos_cluster_info_dbs_total
# TYPE taos_cluster_info_dbs_total counter
taos_cluster_info_dbs_total{cluster_id="5981392874047724755"} 2
# HELP taos_cluster_info_dnodes_alive
# TYPE taos_cluster_info_dnodes_alive counter
taos_cluster_info_dnodes_alive{cluster_id="5981392874047724755"} 1
# HELP taos_cluster_info_dnodes_total
# TYPE taos_cluster_info_dnodes_total counter
taos_cluster_info_dnodes_total{cluster_id="5981392874047724755"} 1
# HELP taos_cluster_info_first_ep
# TYPE taos_cluster_info_first_ep gauge
taos_cluster_info_first_ep{cluster_id="5981392874047724755",value="hlb:6030"} 1
```

### check\_health 

```
$ curl -i http://127.0.0.1:6043/check_health
```

返回结果：

```
HTTP/1.1 200 OK
Content-Type: application/json; charset=utf-8
Date: Mon, 03 Apr 2023 07:20:38 GMT
Content-Length: 19

{"version":"1.0.0"}
```

### 集成 Prometheus

taoskeeper 提供了 `/metrics` 接口，返回了 Prometheus 格式的监控数据，Prometheus 可以从 taoskeeper 抽取监控数据，实现通过 Prometheus 监控 TDengine 的目的。

#### 抽取配置

Prometheus 提供了 `scrape_configs` 配置如何从 endpoint 抽取监控数据，通常只需要修改 `static_configs` 中的 targets 配置为 taoskeeper 的 endpoint 地址，更多配置信息请参考 [Prometheus 配置文档](https://prometheus.io/docs/prometheus/latest/configuration/configuration/#scrape_config)。

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

我们提供了 `TaosKeeper Prometheus Dashboard for 3.x` dashboard，提供了和 TDinsight 类似的监控 dashboard。

在 Grafana Dashboard 菜单点击 `import`，dashboard ID 填写 `18587`，点击 `Load` 按钮即可导入 `TaosKeeper Prometheus Dashboard for 3.x` dashboard。

