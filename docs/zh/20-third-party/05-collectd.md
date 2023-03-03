---
sidebar_label: collectd
title: collectd 写入
description: 使用 collected 向 TDengine 写入数据
---

import CollectD from "../14-reference/_collectd.mdx"

collectd 是一个用来收集系统性能的守护进程。collectd 提供各种存储方式来存储不同值的机制。它会在系统运行和存储信息时周期性的统计系统的相关统计信息。利用这些信息有助于查找当前系统性能瓶颈和预测系统未来的负载等。

只需要将 collectd 的配置指向运行 taosAdapter 的服务器域名（或 IP 地址）和相应端口即可将 collectd 采集的数据写入到 TDengine，可以充分利用 TDengine 对时序数据的高效存储查询性能和集群处理能力。

## 前置条件

要将 collectd 数据写入 TDengine，需要几方面的准备工作。
- TDengine 集群已经部署并正常运行
- taosAdapter 已经安装并正常运行，具体细节请参考[ taosAdapter 的使用手册](/reference/taosadapter)
- collectd 已经安装。安装 collectd 请参考[官方文档](https://collectd.org/download.shtml)

## 配置步骤
<CollectD />

## 验证方法

重启 collectd 

```
sudo systemctl restart collectd
```

使用 TDengine CLI 验证从 collectd 向 TDengine 写入数据并能够正确读出：

```
taos> show databases;
              name              |
=================================
 information_schema             |
 performance_schema             |
 collectd                       |
Query OK, 3 row(s) in set (0.003266s)

taos> use collectd;
Database changed.

taos> show stables;
              name              |
=================================
 load_1                         |
 memory_value                   |
 df_value                       |
 load_2                         |
 load_0                         |
 interface_1                    |
 irq_value                      |
 interface_0                    |
 entropy_value                  |
 swap_value                     |
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

:::note

- TDengine 默认生成的子表名是根据规则生成的唯一 ID 值。
:::
