---
sidebar_label: TCollector
title: TCollector 写入
description: 使用 TCollector 写入 TDengine
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
              name              |
=================================
 information_schema             |
 performance_schema             |
 tcollector                     |
Query OK, 3 rows in database (0.001647s)


taos> use tcollector;
Database changed.

taos> show stables;
              name              |
=================================
 proc.meminfo.hugepages_rsvd    |
 proc.meminfo.directmap1g       |
 proc.meminfo.vmallocchunk      |
 proc.meminfo.hugepagesize      |
 tcollector.reader.lines_dro... |
 proc.meminfo.sunreclaim        |
 proc.stat.ctxt                 |
 proc.meminfo.swaptotal         |
 proc.uptime.total              |
 tcollector.collector.lines_... |
 proc.meminfo.vmallocused       |
 proc.meminfo.memavailable      |
 sys.numa.foreign_allocs        |
 proc.meminfo.committed_as      |
 proc.vmstat.pswpin             |
 proc.meminfo.cmafree           |
 proc.meminfo.mapped            |
 proc.vmstat.pgmajfault         |
...
```

:::note

- TDengine 默认生成的子表名是根据规则生成的唯一 ID 值。
:::
