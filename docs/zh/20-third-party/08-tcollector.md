---
sidebar_label: TCollector
title: TCollector 写入
---

import TCollector from "../14-reference/_tcollector.mdx"

TCollector 是 openTSDB 的一部分，它用来采集客户端日志发送给数据库。

只需要将 TCollector 的配置修改指向运行 taosAdapter 的服务器域名（或 IP 地址）和相应端口即可将 TCollector 采集的数据存在到 TDengine 中，可以充分利用 TDengine 对时序数据的高效存储查询性能和集群处理能力。

## 前置条件

要将 TCollector 数据写入 TDengine 需要以下几方面的准备工作。
- TDengine 集群已经部署并正常运行
- taosAdapter 已经安装并正常运行。具体细节请参考 [taosAdapter 的使用手册](/reference/taosadapter)
- TCollector 已经安装。安装 TCollector 请参考[官方文档](http://opentsdb.net/docs/build/html/user_guide/utilities/tcollector.html#installation-of-tcollector)

## 配置步骤
<TCollector />

## 验证方法

重启 taosAdapter：

```
sudo systemctl restart taosadapter
```

手动执行 `sudo ./tcollector.py` 

等待数秒后使用 TDengine CLI 查询 TDengine 是否创建相应数据库并写入数据。

```
taos> show databases;
              name              |      created_time       |   ntables   |   vgroups   | replica | quorum |  days  |           keep           |  cache(MB)  |   blocks    |   minrows   |   maxrows   | wallevel |    fsync    | comp | cachelast | precision | update |   status   |
====================================================================================================================================================================================================================================================================================
 tcollector                     | 2022-04-20 12:44:49.604 |          88 |           1 |       1 |      1 |     10 | 3650                     |          16 |           6 |         100 |        4096 |        1 |        3000 |    2 |         0 | ns        |      2 | ready      |
 log                            | 2022-04-20 07:19:50.260 |          11 |           1 |       1 |      1 |     10 | 3650                     |          16 |           6 |         100 |        4096 |        1 |        3000 |    2 |         0 | ms        |      0 | ready      |
Query OK, 2 row(s) in set (0.002679s)

taos> use tcollector;
Database changed.

taos> show stables;
              name              |      created_time       | columns |  tags  |   tables    |
============================================================================================
 proc.meminfo.hugepages_rsvd    | 2022-04-20 12:44:53.945 |       2 |      1 |           1 |
 proc.meminfo.directmap1g       | 2022-04-20 12:44:54.110 |       2 |      1 |           1 |
 proc.meminfo.vmallocchunk      | 2022-04-20 12:44:53.724 |       2 |      1 |           1 |
 proc.meminfo.hugepagesize      | 2022-04-20 12:44:54.004 |       2 |      1 |           1 |
 tcollector.reader.lines_dro... | 2022-04-20 12:44:49.675 |       2 |      1 |           1 |
 proc.meminfo.sunreclaim        | 2022-04-20 12:44:53.437 |       2 |      1 |           1 |
 proc.stat.ctxt                 | 2022-04-20 12:44:55.363 |       2 |      1 |           1 |
 proc.meminfo.swaptotal         | 2022-04-20 12:44:53.158 |       2 |      1 |           1 |
 proc.uptime.total              | 2022-04-20 12:44:52.813 |       2 |      1 |           1 |
 tcollector.collector.lines_... | 2022-04-20 12:44:49.895 |       2 |      2 |          51 |
 proc.meminfo.vmallocused       | 2022-04-20 12:44:53.704 |       2 |      1 |           1 |
 proc.meminfo.memavailable      | 2022-04-20 12:44:52.939 |       2 |      1 |           1 |
 sys.numa.foreign_allocs        | 2022-04-20 12:44:57.929 |       2 |      2 |           1 |
 proc.meminfo.committed_as      | 2022-04-20 12:44:53.639 |       2 |      1 |           1 |
 proc.vmstat.pswpin             | 2022-04-20 12:44:54.177 |       2 |      1 |           1 |
 proc.meminfo.cmafree           | 2022-04-20 12:44:53.865 |       2 |      1 |           1 |
 proc.meminfo.mapped            | 2022-04-20 12:44:53.349 |       2 |      1 |           1 |
 proc.vmstat.pgmajfault         | 2022-04-20 12:44:54.251 |       2 |      1 |           1 |
...
```
