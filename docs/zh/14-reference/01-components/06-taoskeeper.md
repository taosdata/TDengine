---
sidebar_label: taosKeeper
title: taosKeeper 参考手册
toc_max_heading_level: 4
---

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";

taosKeeper 是 TDengine 3.0 版本监控指标的导出工具，通过简单的几项配置即可获取 TDengine 的运行状态。taosKeeper 使用 TDengine RESTful 接口，所以不需要安装 TDengine 客户端即可使用。

## 安装

taosKeeper 有两种安装方式：

- 安装 TDengine 官方安装包的同时会自动安装 taosKeeper, 详情请参考[ TDengine 安装](../../../get-started/)。

- 单独编译 taosKeeper 并安装，详情请参考 [taosKeeper](https://github.com/taosdata/taoskeeper) 仓库。

## 配置

taosKeeper 需要在操作系统终端执行，该工具支持三种配置方式：命令行参数、环境变量 和 配置文件。优先级为：命令行参数、环境变量、配置文件参数。 一般我们推荐使用配置文件。

### 命令行参数和环境变量
命令行参数 和 环境变量说明可以参考命令 `taoskeeper --help` 的输出。下面是一个例子：
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



### 配置文件

taosKeeper 支持用 `taoskeeper -c <keeper config file>` 命令来指定配置文件。   
若不指定配置文件，taosKeeper 会使用默认配置文件，其路径为： `/etc/taos/taoskeeper.toml` 。   
若既不指定 taosKeeper 配置文件，且 `/etc/taos/taoskeeper.toml` 也不存在，将使用默认配置。  

**下面是配置文件的示例：**
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

## 启动

**在运行 taosKeeper 之前要确保 TDengine 集群与 taosAdapter 已经在正确运行。** 并且 TDengine 已经开启监控服务，TDengine 配置文件 `taos.cfg` 中至少需要配置 `monitor` 和 `monitorFqdn`。

```shell
monitor 1
monitorFqdn localhost # taoskeeper 服务的 FQDN
```

TDengine 监控配置相关，具体请参考：[TDengine 监控配置](../../../operation/monitor)。


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
- 故障排查：如果服务异常请查看日志获取更多信息。日志文件默认放在 `/var/log/taos` 下。

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
- 故障排查：如果服务异常请查看日志获取更多信息。日志文件默认放在 `/var/log/taos` 下。

:::

</TabItem>
</Tabs>


## 健康检查 

可以访问 taosKeeper 的 `check_health` 接口来判断服务是否存活，如果服务正常则会返回 HTTP 200 状态码：

```
$ curl -i http://127.0.0.1:6043/check_health
```

返回结果：

```
HTTP/1.1 200 OK
Content-Type: application/json; charset=utf-8
Date: Wed, 07 Aug 2024 06:19:50 GMT
Content-Length: 21

{"version":"3.3.2.3"}
```


## 数据收集与监控

taosKeeper 作为 TDengine 监控指标的导出工具，可以将 TDengine 产生的监控数据记录在指定数据库中（默认的监控数据是 `log`），这些监控数据可以用来配置 TDengine 监控。

### 查看监控数据

可以查看 `log` 库下的超级表，每个超级表都对应一组监控指标，具体指标不再赘述。
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

可以查看一个超级表的最近一条上报记录，如：

``` shell
taos> select last_row(*) from taosd_dnodes_info;
      last_row(_ts)      |   last_row(disk_engine)   |  last_row(system_net_in)  |   last_row(vnodes_num)    | last_row(system_net_out)  |     last_row(uptime)      |    last_row(has_mnode)    |  last_row(io_read_disk)   | last_row(error_log_count) |     last_row(io_read)     |    last_row(cpu_cores)    |    last_row(has_qnode)    |    last_row(has_snode)    |   last_row(disk_total)    |   last_row(mem_engine)    | last_row(info_log_count)  |   last_row(cpu_engine)    |  last_row(io_write_disk)  | last_row(debug_log_count) |    last_row(disk_used)    |    last_row(mem_total)    |    last_row(io_write)     |     last_row(masters)     |   last_row(cpu_system)    | last_row(trace_log_count) |    last_row(mem_free)     |
======================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================
 2024-08-07 14:54:09.174 |         0.000000000000000 |      3379.093240947399863 |        37.000000000000000 |      5265.998201139278535 |     64402.000000000000000 |         1.000000000000000 |      8323.261934108399146 |         6.000000000000000 |     40547.386655118425551 |        16.000000000000000 |         0.000000000000000 |         0.000000000000000 |     5.272955781120000e+11 |   2443032.000000000000000 |       423.000000000000000 |         0.556269622200215 |    677731.836503547732718 |    356380.000000000000000 |     4.997186764800000e+10 |  65557284.000000000000000 |    714177.054532129666768 |        37.000000000000000 |         2.642280705451021 |         0.000000000000000 |  11604276.000000000000000 |
Query OK, 1 row(s) in set (0.003168s)
```


### 使用 TDInsight 配置监控

收集到监控数据以后，就可以使用 TDInsight 来配置 TDengine 的监控，具体请参考 [TDinsight 参考手册](../tdinsight/) 


## 集成 Prometheus

taoskeeper 提供了 `/metrics` 接口，返回了 Prometheus 格式的监控数据，Prometheus 可以从 taoskeeper 抽取监控数据，实现通过 Prometheus 监控 TDengine 的目的。


### 导出监控指标

下面通过 `curl` 命令展示 `/metrics` 接口返回的数据格式：

```shell
$ curl http://127.0.0.1:6043/metrics
```

部分结果集：

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

### 监控指标详情

#### taosd 集群

##### 监控信息支持的标签
- `cluster_id`： 集群 id

##### 相关指标及其含义
| 指标名称                            | 类型    | 含义                                  |
| ----------------------------------- | ------- | ------------------------------------- |
| taos_cluster_info_connections_total | counter | 总连接数                              |
| taos_cluster_info_dbs_total         | counter | 数据库总数                            |
| taos_cluster_info_dnodes_alive      | counter | 存活的 dnode 数量                     |
| taos_cluster_info_dnodes_total      | counter | dnode 总数                            |
| taos_cluster_info_first_ep          | gauge   | 第一个端点，标签 value 表示端点值     |
| taos_cluster_info_first_ep_dnode_id | counter | 第一个端点的 dnode id                 |
| taos_cluster_info_master_uptime     | gauge   | 主节点运行时间，单位天                |
| taos_cluster_info_mnodes_alive      | counter | 存活的 mnode 数量                     |
| taos_cluster_info_mnodes_total      | counter | mnode 总数                            |
| taos_cluster_info_stbs_total        | counter | 超级表总数                            |
| taos_cluster_info_streams_total     | counter | 流总数                                |
| taos_cluster_info_tbs_total         | counter | 表总数                                |
| taos_cluster_info_topics_total      | counter | 主题总数                              |
| taos_cluster_info_version           | gauge   | 版本信息，标签 value 表示版本号       |
| taos_cluster_info_vgroups_alive     | counter | 存活的虚拟组数量                      |
| taos_cluster_info_vgroups_total     | counter | 虚拟组总数                            |
| taos_cluster_info_vnodes_alive      | counter | 存活的虚拟节点数量                    |
| taos_cluster_info_vnodes_total      | counter | 虚拟节点总数                          |
| taos_grants_info_expire_time        | counter | 集群授权过期剩余时间（单位 秒）       |
| taos_grants_info_timeseries_total   | counter | 集群授权允许使用 time series 的总数量 |
| taos_grants_info_timeseries_used    | counter | 集群已拥有的 time series 的数量       |

#### dnode

##### 监控信息支持的标签
- `cluster_id`： 集群 id
- `dnode_ep`： dnode 端点
- `dnode_id`：dnode id

##### 相关指标及其含义
| 指标名称                       | 类型    | 含义                                                                                     |
| ------------------------------ | ------- | ---------------------------------------------------------------------------------------- |
| taos_d_info_status             | gauge   | dnode 状态，标签 value 表示状态， ready 表示正常， offline 表示下线， unknown 表示未知。 |
| taos_dnodes_info_cpu_cores     | gauge   | CPU 核心数                                                                               |
| taos_dnodes_info_cpu_engine    | gauge   | 该 dnode 的进程所使用的 CPU 百分比（取值范围 0~100）                                     |
| taos_dnodes_info_cpu_system    | gauge   | 该 dnode 所在节点的系统使用的 CPU 百分比（取值范围 0~100）                               |
| taos_dnodes_info_disk_engine   | counter | 该 dnode 的进程使用的磁盘容量（单位 Byte)                                                |
| taos_dnodes_info_disk_total    | counter | 该 dnode 所在节点的磁盘总容量（单位 Byte)                                                |
| taos_dnodes_info_disk_used     | counter | 该 dnode 所在节点的磁盘已使用的容量（单位 Byte)                                          |
| taos_dnodes_info_has_mnode     | counter | 是否有 mnode                                                                             |
| taos_dnodes_info_has_qnode     | counter | 是否有 qnode                                                                             |
| taos_dnodes_info_has_snode     | counter | 是否有 snode                                                                             |
| taos_dnodes_info_io_read       | gauge   | 该 dnode 所在节点的 io 读取速率（单位 Byte/s)                                            |
| taos_dnodes_info_io_read_disk  | gauge   | 该 dnode 所在节点的磁盘 io 写入取速率（单位 Byte/s)                                      |
| taos_dnodes_info_io_write      | gauge   | 该 dnode 所在节点的 io 写入取速率（单位 Byte/s)                                          |
| taos_dnodes_info_io_write_disk | gauge   | 该 dnode 所在节点的磁盘 io 写入取速率（单位 Byte/s)                                      |
| taos_dnodes_info_masters       | counter | 主节点数量                                                                               |
| taos_dnodes_info_mem_engine    | counter | 该 dnode 的进程所使用的内存（单位 KB)                                                    |
| taos_dnodes_info_mem_system    | counter | 该 dnode 所在节的系统所使用的内存（单位 KB)                                              |
| taos_dnodes_info_mem_total     | counter | 该 dnode 所在节点的总内存（单位 KB)                                                      |
| taos_dnodes_info_net_in        | gauge   | 该 dnode 所在节点的网络传入速率（单位 Byte/s)                                            |
| taos_dnodes_info_net_out       | gauge   | 该 dnode 所在节点的网络传出速率（单位 Byte/s)                                            |
| taos_dnodes_info_uptime        | gauge   | 该 dnode 的启动时间(单位 秒)                                                             |
| taos_dnodes_info_vnodes_num    | counter | 该 dnode 所在节点的 vnode 数量                                                           |

#### 数据目录

##### 监控信息支持的标签
- `cluster_id`： 集群 id
- `dnode_ep`： dnode 端点
- `dnode_id`：dnode id
- `data_dir_name`：数据目录名
- `data_dir_level`：数据目录级别 

##### 相关指标及其含义
| 指标名称                          | 类型  | 含义                 |
| --------------------------------- | ----- | -------------------- |
| taos_taosd_dnodes_data_dirs_avail | gauge | 可用空间（单位 Byte) |
| taos_taosd_dnodes_data_dirs_total | gauge | 总空间（单位 Byte)   |
| taos_taosd_dnodes_data_dirs_used  | gauge | 已用空间（单位 Byte) |

#### 日志目录

##### 监控信息支持的标签
- `cluster_id`： 集群 id
- `dnode_ep`： dnode 端点
- `dnode_id`：dnode id
- `log_dir_name`：日志目录名

##### 相关指标及其含义
| 指标名称                         | 类型  | 含义                 |
| -------------------------------- | ----- | -------------------- |
| taos_taosd_dnodes_log_dirs_avail | gauge | 可用空间（单位 Byte) |
| taos_taosd_dnodes_log_dirs_total | gauge | 总空间（单位 Byte)   |
| taos_taosd_dnodes_log_dirs_used  | gauge | 已用空间（单位 Byte) |

#### 日志数量

##### 监控信息支持的标签
- `cluster_id`： 集群 id
- `dnode_ep`： dnode 端点
- `dnode_id`：dnode id

##### 相关指标及其含义
| 指标名称               | 类型    | 含义         |
| ---------------------- | ------- | ------------ |
| taos_log_summary_debug | counter | 调试日志数量 |
| taos_log_summary_error | counter | 错误日志数量 |
| taos_log_summary_info  | counter | 信息日志数量 |
| taos_log_summary_trace | counter | 跟踪日志数量 |


#### taosadapter

##### 监控信息支持的标签
- `endpoint`：端点
- `req_type`：请求类型，0 表示 rest，1 表示 websocket

##### 相关指标及其含义
| 指标名称                               | 类型    | 含义                 |
| -------------------------------------- | ------- | -------------------- |
| taos_adapter_requests_fail             | counter | 失败的请求数         |
| taos_adapter_requests_in_process       | counter | 正在处理的请求数     |
| taos_adapter_requests_other            | counter | 其他类型的请求数     |
| taos_adapter_requests_other_fail       | counter | 其他类型的失败请求数 |
| taos_adapter_requests_other_success    | counter | 其他类型的成功请求数 |
| taos_adapter_requests_query            | counter | 查询请求数           |
| taos_adapter_requests_query_fail       | counter | 查询失败请求数       |
| taos_adapter_requests_query_in_process | counter | 正在处理的查询请求数 |
| taos_adapter_requests_query_success    | counter | 查询成功请求数       |
| taos_adapter_requests_success          | counter | 成功的请求数         |
| taos_adapter_requests_total            | counter | 总请求数             |
| taos_adapter_requests_write            | counter | 写请求数             |
| taos_adapter_requests_write_fail       | counter | 写失败请求数         |
| taos_adapter_requests_write_in_process | counter | 正在处理的写请求数   |
| taos_adapter_requests_write_success    | counter | 写成功请求数         |

#### taoskeeper

##### 监控信息支持的标签
- `identify`： 节点 endpoint

##### 相关指标及其含义
| 指标名称                | 类型  | 含义                                  |
| ----------------------- | ----- | ------------------------------------- |
| taos_keeper_monitor_cpu | gauge | taoskeeper CPU 使用率（取值范围 0~1） |
| taos_keeper_monitor_mem | gauge | taoskeeper 内存使用率（取值范围 0~1） |

#### 其他 taosd 集群监控项

##### taos_m_info_role
- **标签**:
  - `cluster_id`: 集群 id
  - `mnode_ep`: mnode 端点
  - `mnode_id`: mnode id
  - `value`: 角色值（该 mnode 的状态，取值范围：offline, follower, candidate, leader, error, learner）
- **类型**: gauge
- **含义**: mnode 角色

##### taos_taos_sql_req_count
- **标签**:
  - `cluster_id`: 集群 id
  - `result`: 请求结果（取值范围： Success, Failed）
  - `sql_type`: SQL 类型（取值范围：select, insert，inserted_rows, delete）
  - `username`: 用户名
- **类型**: gauge
- **含义**: SQL 请求数量

##### taos_taosd_sql_req_count
- **标签**:
  - `cluster_id`: 集群 id
  - `dnode_ep`: dnode 端点
  - `dnode_id`: dnode id
  - `result`: 请求结果（取值范围： Success, Failed）
  - `sql_type`: SQL 类型（取值范围：select, insert，inserted_rows, delete）
  - `username`: 用户名
  - `vgroup_id`: 虚拟组 id
- **类型**: gauge
- **含义**: SQL 请求数量

##### taos_taosd_vgroups_info_status
- **标签**:
  - `cluster_id`: 集群 id
  - `database_name`: 数据库名称
  - `vgroup_id`: 虚拟组 id
- **类型**: gauge
- **含义**: 虚拟组状态。 0 为 unsynced，表示没有leader选出；1 为 ready。

##### taos_taosd_vgroups_info_tables_num
- **标签**:
  - `cluster_id`: 集群 id
  - `database_name`: 数据库名称
  - `vgroup_id`: 虚拟组 id
- **类型**: gauge
- **含义**: 虚拟组表数量

##### taos_taosd_vnodes_info_role
- **标签**:
  - `cluster_id`: 集群 id
  - `database_name`: 数据库名称
  - `dnode_id`: dnode id
  - `value`: 角色值（取值范围：offline, follower, candidate, leader, error, learner）
  - `vgroup_id`: 虚拟组 id
- **类型**: gauge
- **含义**: 虚拟节点角色
  

### 抽取配置

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

### Dashboard

我们提供了 `TaosKeeper Prometheus Dashboard for 3.x` dashboard，提供了和 TDinsight 类似的监控 dashboard。

在 Grafana Dashboard 菜单点击 `import`，dashboard ID 填写 `18587`，点击 `Load` 按钮即可导入 `TaosKeeper Prometheus Dashboard for 3.x` dashboard。



## taosKeeper 监控指标

taosKeeper 也会将自己采集的监控数据写入监控数据库，默认是 `log` 库，可以在 taoskeeper 配置文件中修改。

### keeper\_monitor 表

`keeper_monitor` 记录 taoskeeper 监控数据。

| field    | type      | is\_tag | comment      |
| :------- | :-------- | :------ | :----------- |
| ts       | TIMESTAMP |         | timestamp    |
| cpu      | DOUBLE    |         | cpu 使用率   |
| mem      | DOUBLE    |         | 内存使用率   |
| identify | NCHAR     | TAG     | 身份标识信息 |
