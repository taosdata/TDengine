---
sidebar_label: Telegraf
title: Telegraf 写入
---

import Telegraf from "../14-reference/_telegraf.mdx"

Telegraf 是一款十分流行的指标采集开源软件。在数据采集和平台监控系统中，Telegraf 可以采集多种组件的运行信息，而不需要自己手写脚本定时采集，降低数据获取的难度。

只需要将 Telegraf 的输出配置增加指向 taosAdapter 对应的 url 并修改若干配置项即可将 Telegraf 的数据写入到 TDengine 中。将 Telegraf 的数据存在到 TDengine 中可以充分利用 TDengine 对时序数据的高效存储查询性能和集群处理能力。

## 前置条件

要将 Telegraf 数据写入 TDengine 需要以下几方面的准备工作。
- TDengine 集群已经部署并正常运行
- taosAdapter 已经安装并正常运行。具体细节请参考 [taosAdapter 的使用手册](/reference/taosadapter)
- Telegraf 已经安装。安装 Telegraf 请参考[官方文档](https://docs.influxdata.com/telegraf/v1.22/install/)

## 配置步骤
<Telegraf />

## 验证方法

重启 Telegraf 服务：

```
sudo systemctl restart telegraf
```

使用 TDengine CLI 验证从 Telegraf 向 TDengine 写入数据并能够正确读出：

```
taos> show databases;
              name              |      created_time       |   ntables   |   vgroups   | replica | quorum |  days  |           keep           |  cache(MB)  |   blocks    |   minrows   |   maxrows   | wallevel |    fsync    | comp | cachelast | precision | update |   status   |
====================================================================================================================================================================================================================================================================================
 telegraf                       | 2022-04-20 08:47:53.488 |          22 |           1 |       1 |      1 |     10 | 3650                     |          16 |           6 |         100 |        4096 |        1 |        3000 |    2 |         0 | ns        |      2 | ready      |
 log                            | 2022-04-20 07:19:50.260 |           9 |           1 |       1 |      1 |     10 | 3650                     |          16 |           6 |         100 |        4096 |        1 |        3000 |    2 |         0 | ms        |      0 | ready      |
Query OK, 2 row(s) in set (0.002401s)

taos> use telegraf;
Database changed.

taos> show stables;
              name              |      created_time       | columns |  tags  |   tables    |
============================================================================================
 swap                           | 2022-04-20 08:47:53.532 |       7 |      1 |           1 |
 cpu                            | 2022-04-20 08:48:03.488 |      11 |      2 |           5 |
 system                         | 2022-04-20 08:47:53.512 |       8 |      1 |           1 |
 diskio                         | 2022-04-20 08:47:53.550 |      12 |      2 |          15 |
 kernel                         | 2022-04-20 08:47:53.503 |       6 |      1 |           1 |
 mem                            | 2022-04-20 08:47:53.521 |      35 |      1 |           1 |
 processes                      | 2022-04-20 08:47:53.555 |      12 |      1 |           1 |
 disk                           | 2022-04-20 08:47:53.541 |       8 |      5 |           2 |
Query OK, 8 row(s) in set (0.000521s)

taos> select * from telegraf.system limit 10;
              ts               |           load1           |           load5           |          load15           |        n_cpus         |        n_users        |        uptime         | uptime_format |              host
|
=============================================================================================================================================================================================================================================
 2022-04-20 08:47:50.000000000 |               0.000000000 |               0.050000000 |               0.070000000 |                     4 |                     1 |                  5533 |  1:32         | shuduo-1804
|
 2022-04-20 08:48:00.000000000 |               0.000000000 |               0.050000000 |               0.070000000 |                     4 |                     1 |                  5543 |  1:32         | shuduo-1804
|
 2022-04-20 08:48:10.000000000 |               0.000000000 |               0.040000000 |               0.070000000 |                     4 |                     1 |                  5553 |  1:32         | shuduo-1804
|
Query OK, 3 row(s) in set (0.013269s)
```
