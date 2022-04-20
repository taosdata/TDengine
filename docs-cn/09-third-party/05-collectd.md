---
sidebar_label: collectd
title: collectd 写入
---

import CollectD from "../14-reference/_collectd.mdx"

collectd 是一款插件式架构的开源监控软件，它可以收集各种来源的指标，如操作系统，应用程序，日志文件和外部设备，并存储此信息或通过网络提供。

只需要将 collectd 的配置指向 taosAdapter 对应的服务器和端口即可将 collectd 采集的数据写入到 TDengine，可以充分利用 TDengine 对时序数据的高效存储查询性能和集群处理能力。

安装 collectd 请参考[官方文档](https://collectd.org/download.shtml)。

## 前置条件

要将 collectd 数据写入 TDengine, 需要几方面的准备工作。
- TDengine 集群已经部署并正在运行
- taosAdapter 已经安装并正在运行, 具体细节请参考 [taosAdapter 的使用手册](/reference/taosadapter)
- collectd 已经安装

## 配置 collectd
<CollectD />

## 验证方法

重启 collectd 后使用 TDengine CLI 验证从 collectd 向 TDengine 写入数据并能够正确读出：

```
taos> show databases;
              name              |      created_time       |   ntables   |   vgroups   | replica | quorum |  days  |           keep           |  cache(MB)  |   blocks    |   minrows   |   maxrows   | wallevel |    fsync    | comp | cachelast | precision | update |   status   |
====================================================================================================================================================================================================================================================================================
 collectd                       | 2022-04-20 09:27:45.460 |          95 |           1 |       1 |      1 |     10 | 3650                     |          16 |           6 |         100 |        4096 |        1 |        3000 |    2 |         0 | ns        |      2 | ready      |
 log                            | 2022-04-20 07:19:50.260 |          11 |           1 |       1 |      1 |     10 | 3650                     |          16 |           6 |         100 |        4096 |        1 |        3000 |    2 |         0 | ms        |      0 | ready      |
Query OK, 2 row(s) in set (0.003266s)

taos> use collectd;
Database changed.

taos> show stables;
              name              |      created_time       | columns |  tags  |   tables    |
============================================================================================
 load_1                         | 2022-04-20 09:27:45.492 |       2 |      2 |           1 |
 memory_value                   | 2022-04-20 09:27:45.463 |       2 |      3 |           6 |
 df_value                       | 2022-04-20 09:27:45.463 |       2 |      4 |          25 |
 load_2                         | 2022-04-20 09:27:45.501 |       2 |      2 |           1 |
 load_0                         | 2022-04-20 09:27:45.485 |       2 |      2 |           1 |
 interface_1                    | 2022-04-20 09:27:45.488 |       2 |      3 |          12 |
 irq_value                      | 2022-04-20 09:27:45.476 |       2 |      3 |          31 |
 interface_0                    | 2022-04-20 09:27:45.480 |       2 |      3 |          12 |
 entropy_value                  | 2022-04-20 09:27:45.473 |       2 |      2 |           1 |
 swap_value                     | 2022-04-20 09:27:45.477 |       2 |      3 |           5 |
Query OK, 10 row(s) in set (0.002236s)

taos> select * from collectd.memory_value limit 10;
              ts               |           value           |              host              |         type_instance          |           type           |
=========================================================================================================================================================
 2022-04-20 09:27:45.459653462 |        54689792.000000000 | shuduo-1804                    | buffered                       | memory                   |
 2022-04-20 09:27:55.453168283 |        57212928.000000000 | shuduo-1804                    | buffered                       | memory                   |
 2022-04-20 09:28:05.453004291 |        57942016.000000000 | shuduo-1804                    | buffered                       | memory                   |
 2022-04-20 09:27:45.459653462 |      6381330432.000000000 | shuduo-1804                    | free                           | memory                   |
 2022-04-20 09:27:55.453168283 |      6357643264.000000000 | shuduo-1804                    | free                           | memory                   |
 2022-04-20 09:28:05.453004291 |      6349987840.000000000 | shuduo-1804                    | free                           | memory                   |
 2022-04-20 09:27:45.459653462 |       107040768.000000000 | shuduo-1804                    | slab_recl                      | memory                   |
 2022-04-20 09:27:55.453168283 |       107536384.000000000 | shuduo-1804                    | slab_recl                      | memory                   |
 2022-04-20 09:28:05.453004291 |       107634688.000000000 | shuduo-1804                    | slab_recl                      | memory                   |
 2022-04-20 09:27:45.459653462 |       309137408.000000000 | shuduo-1804                    | used                           | memory                   |
Query OK, 10 row(s) in set (0.010348s)
```

