---
sidebar_label: Prometheus
title: Prometheus 
---

import Prometheus from "../14-reference/_prometheus.mdx"

Prometheus 是一款流行的开源监控告警系统。Prometheus 于2016年加入了 Cloud Native Computing Foundation （云原生云计算基金会，简称 CNCF），成为继 Kubernetes 之后的第二个托管项目，该项目拥有非常活跃的开发人员和用户社区。

Prometheus 提供了 `remote_write` 和 `remote_read` 接口来利用其它数据库产品作为它的存储引擎。为了让 Prometheus 生态圈的用户能够利用 TDengine 的高效写入和查询，TDengine 也提供了对这两个接口的支持。

通过适当的配置， Prometheus 的数据可以通过 `remote_write` 接口存储到 TDengine 中，也可以通过 `remote_read` 接口来查询存储在 TDengine 中的数据，充分利用 TDengine 对时序数据的高效存储查询性能和集群处理能力。

## 前置条件

要将 Prometheus 数据写入 TDengine 需要以下几方面的准备工作。
- TDengine 集群已经部署并正常运行
- taosAdapter 已经安装并正常运行。具体细节请参考 [taosAdapter 的使用手册](/reference/taosadapter)
- Prometheus 已经安装。安装 Prometheus 请参考[官方文档](https://prometheus.io/docs/prometheus/latest/installation/)

## 配置步骤
<Prometheus />

## 验证方法

重启 Prometheus 后可参考以下示例验证从 Prometheus 向 TDengine 写入数据并能够正确读出。

### 使用 TDengine CLI 查询写入数据
```
taos> show databases;
              name              |      created_time       |   ntables   |   vgroups   | replica | quorum |  days  |           keep           |  cache(MB)  |   blocks    |   minrows   |   maxrows   | wallevel |    fsync    | comp | cachelast | precision | update |   status   |
====================================================================================================================================================================================================================================================================================
 test                           | 2022-04-12 08:07:58.756 |           1 |           1 |       1 |      1 |     10 | 3650                     |          16 |           6 |         100 |        4096 |        1 |        3000 |    2 |         0 | ms        |      0 | ready      |
 log                            | 2022-04-20 07:19:50.260 |           2 |           1 |       1 |      1 |     10 | 3650                     |          16 |           6 |         100 |        4096 |        1 |        3000 |    2 |         0 | ms        |      0 | ready      |
 prometheus_data                | 2022-04-20 07:21:09.202 |         158 |           1 |       1 |      1 |     10 | 3650                     |          16 |           6 |         100 |        4096 |        1 |        3000 |    2 |         0 | ns        |      2 | ready      |
 db                             | 2022-04-15 06:37:08.512 |           1 |           1 |       1 |      1 |     10 | 3650                     |          16 |           6 |         100 |        4096 |        1 |        3000 |    2 |         0 | ms        |      0 | ready      |
Query OK, 4 row(s) in set (0.000585s)

taos> use prometheus_data;
Database changed.

taos> show stables;
              name              |      created_time       | columns |  tags  |   tables    |
============================================================================================
 metrics                        | 2022-04-20 07:21:09.209 |       2 |      1 |        1389 |
Query OK, 1 row(s) in set (0.000487s)

taos> select * from metrics limit 10;
              ts               |           value           |             labels             |
=============================================================================================
 2022-04-20 07:21:09.193000000 |               0.000024996 | {"__name__":"go_gc_duration... |
 2022-04-20 07:21:14.193000000 |               0.000024996 | {"__name__":"go_gc_duration... |
 2022-04-20 07:21:19.193000000 |               0.000024996 | {"__name__":"go_gc_duration... |
 2022-04-20 07:21:24.193000000 |               0.000024996 | {"__name__":"go_gc_duration... |
 2022-04-20 07:21:29.193000000 |               0.000024996 | {"__name__":"go_gc_duration... |
 2022-04-20 07:21:09.193000000 |               0.000054249 | {"__name__":"go_gc_duration... |
 2022-04-20 07:21:14.193000000 |               0.000054249 | {"__name__":"go_gc_duration... |
 2022-04-20 07:21:19.193000000 |               0.000054249 | {"__name__":"go_gc_duration... |
 2022-04-20 07:21:24.193000000 |               0.000054249 | {"__name__":"go_gc_duration... |
 2022-04-20 07:21:29.193000000 |               0.000054249 | {"__name__":"go_gc_duration... |
Query OK, 10 row(s) in set (0.011146s)
```

### 使用 promql-cli 通过 remote_read 从 TDengine 读取数据

安装 promql-cli

```
 go install github.com/nalbury/promql-cli@latest
```

在 TDengine 和 taosAdapter 服务运行状态对 Prometheus 数据进行查询

```
ubuntu@shuduo-1804 ~ $ promql-cli --host "http://127.0.0.1:9090" "sum(up) by (job)"
JOB           VALUE    TIMESTAMP
prometheus    1        2022-04-20T08:05:26Z
node          1        2022-04-20T08:05:26Z
```

暂停 taosAdapter 服务后对 Prometheus 数据进行查询

```
ubuntu@shuduo-1804 ~ $ sudo systemctl stop taosadapter.service
ubuntu@shuduo-1804 ~ $ promql-cli --host "http://127.0.0.1:9090" "sum(up) by (job)"
VALUE    TIMESTAMP

```

