---
sidebar_label: StatsD
title: StatsD 直接写入
description: 使用 StatsD 向 TDengine 写入
---

import StatsD from "../14-reference/_statsd.mdx"

StatsD 是汇总和总结应用指标的一个简单的守护进程，近些年来发展迅速，已经变成了一个用于收集应用性能指标的统一的协议。

只需要在 StatsD 的配置文件中填写运行 taosAdapter 的服务器域名（或 IP 地址）和相应端口即可将 StatsD 的数据写入到 TDengine 中，可以充分利用 TDengine 对时序数据的高效存储查询性能和集群处理能力。

## 前置条件

要将 StatsD 数据写入 TDengine 需要以下几方面的准备工作。
- TDengine 集群已经部署并正常运行
- taosAdapter 已经安装并正常运行。具体细节请参考 [taosAdapter 的使用手册](/reference/taosadapter)
- StatsD 已经安装。安装 StatsD 请参考[官方文档](https://github.com/statsd/statsd)

## 配置步骤
<StatsD />

## 验证方法

运行 StatsD：

```
$ node stats.js config.js &
[1] 8546
$ 20 Apr 09:54:41 - [8546] reading config file: config.js
20 Apr 09:54:41 - server is up INFO
```

使用 nc 写入测试数据：

```
$ echo "foo:1|c" | nc -u -w0 127.0.0.1 8125
```

使用 TDengine CLI 验证从 StatsD 向 TDengine 写入数据并能够正确读出：

```
taos> show databases;
              name              |
=================================
 information_schema             |
 performance_schema             |
 statsd                         |
Query OK, 3 row(s) in set (0.003142s)

taos> use statsd;
Database changed.

taos> show stables;
              name              |
=================================
 foo                            |
Query OK, 1 row(s) in set (0.002161s)

taos> select * from foo;
              ts               |         value         |         metric_type          |
=======================================================================================
 2022-04-20 09:54:51.219614235 |                     1 | counter                      |
Query OK, 1 row(s) in set (0.004179s)

taos>
```

:::note

- TDengine will automatically create unique IDs for sub-table names by the rule.
:::
