---
sidebar_label: Telegraf
title: Telegraf 写入
description: 使用 Telegraf 向 TDengine 写入数据
---

import Telegraf from "../14-reference/_telegraf.mdx"

Telegraf 是一款十分流行的指标采集开源软件。在数据采集和平台监控系统中，Telegraf 可以采集多种组件的运行信息，而不需要自己手写脚本定时采集，降低数据获取的难度。

只需要将 Telegraf 的输出配置增加指向 taosAdapter 对应的 url 并修改若干配置项即可将 Telegraf 的数据写入到 TDengine 中。将 Telegraf 的数据存在到 TDengine 中可以充分利用 TDengine 对时序数据的高效存储查询性能和集群处理能力。

## 前置条件

要将 Telegraf 数据写入 TDengine 需要以下几方面的准备工作。
- TDengine 集群已经部署并正常运行
- taosAdapter 已经安装并正常运行。具体细节请参考 [taosAdapter 的使用手册](/reference/taosadapter)
- Telegraf 已经安装。安装 Telegraf 请参考[官方文档](https://docs.influxdata.com/telegraf/v1.22/install/)
- Telegraf 默认采集系统运行状态数据。通过使能[输入插件](https://docs.influxdata.com/telegraf/v1.22/plugins/)方式可以输出[其他格式](https://docs.influxdata.com/telegraf/v1.24/data_formats/input/)的数据到 Telegraf 再写入到 TDengine中。

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
              name              |
=================================
 information_schema             |
 performance_schema             |
 telegraf                       |
Query OK, 3 rows in database (0.010568s)

taos> use telegraf;
Database changed.

taos> show stables;
              name              |
=================================
 swap                           |
 cpu                            |
 system                         |
 diskio                         |
 kernel                         |
 mem                            |
 processes                      |
 disk                           |
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

:::note

- TDengine 接收 influxdb 格式数据默认生成的子表名是根据规则生成的唯一 ID 值。
用户如需指定生成的表名，可以通过在 taos.cfg 里配置 smlChildTableName 参数来指定。如果通过控制输入数据格式，即可利用 TDengine 这个功能指定生成的表名。
举例如下：配置 smlChildTableName=tname 插入数据为 st,tname=cpu1,t1=4 c1=3 1626006833639000000 则创建的表名为 cpu1。如果多行数据 tname 相同，但是后面的 tag_set 不同，则使用第一行自动建表时指定的 tag_set，其他的行会忽略）。[TDengine 无模式写入参考指南](/reference/schemaless/#无模式写入行协议)
:::

