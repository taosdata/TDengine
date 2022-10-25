---
sidebar_label: taosKeeper
title: taosKeeper
description: TDengine 3.0 版本监控指标的导出工具
---

## 简介

TaosKeeper 是 TDengine 3.0 版本监控指标的导出工具，通过简单的几项配置即可获取 TDengine 的运行状态。taosKeeper 使用 TDengine RESTful 接口，所以不需要安装 TDengine 客户端即可使用。

## 安装

<!-- taosKeeper 有两种安装方式: -->
taosKeeper 安装方式：

<!-- - 安装 TDengine 官方安装包的同时会自动安装 taosKeeper, 详情请参考[ TDengine 安装](/operation/pkg-install)。-->

<!-- - 单独编译 taosKeeper 并安装，详情请参考 [taosKeeper](https://github.com/taosdata/taoskeeper) 仓库。-->
- 单独编译 taosKeeper 并安装，详情请参考 [taosKeeper](https://github.com/taosdata/taoskeeper) 仓库。

## 运行

### 配置和运行方式

taosKeeper 需要在操作系统终端执行，该工具支持三种配置方式：[命令行参数](#命令行参数启动)、[环境变量](#环境变量启动) 和 [配置文件](#配置文件启动)。优先级为：命令行参数、环境变量、配置文件参数。

**在运行 taosKeeper 之前要确保 TDengine 集群与 taosAdapter 已经在正确运行。** 并且 TDengine 已经开启监控服务，具体请参考：[TDengine 监控配置](../config/#监控相关)。

### 命令行参数启动

在使用命令行参数运行 taosKeeper 并控制其行为。

```shell
$ taosKeeper
```

### 环境变量启动

通过设置环境变量达到控制启动参数的目的，通常在容器中运行时使用。

```shell
$ export TAOS_KEEPER_TDENGINE_HOST=192.168.64.3
 
$ taoskeeper
```

具体参数列表请参照 `taoskeeper -h` 输入结果。

### 配置文件启动

执行以下命令即可快速体验 taosKeeper。当不指定 taosKeeper 配置文件时，优先使用 `/etc/taos/keeper.toml` 配置，否则将使用默认配置。 

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
