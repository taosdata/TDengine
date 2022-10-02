---
sidebar_label: icinga2
title: icinga2 写入
description: 使用 icinga2 写入 TDengine
---

import Icinga2 from "../14-reference/_icinga2.mdx"

icinga2 是一款开源主机、网络监控软件，最初由 Nagios 网络监控应用发展而来。目前，icinga2 遵从 GNU GPL v2 许可协议发行。

只需要将 icinga2 的配置修改指向 taosAdapter 对应的服务器和相应端口即可将 icinga2 采集的数据存在到 TDengine 中，可以充分利用 TDengine 对时序数据的高效存储查询性能和集群处理能力。

## 前置条件

要将 icinga2 数据写入 TDengine 需要以下几方面的准备工作。
- TDengine 集群已经部署并正常运行
- taosAdapter 已经安装并正常运行。具体细节请参考[ taosAdapter 的使用手册](/reference/taosadapter)
- icinga2 已经安装。安装 icinga2 请参考[官方文档](https://icinga.com/docs/icinga-2/latest/doc/02-installation/)

## 配置步骤
<Icinga2 />

## 验证方法

重启 taosAdapter：
```
sudo systemctl restart taosadapter
```

重启 icinga2：

```
sudo systemctl restart icinga2
```

等待 10 秒左右后，使用 TDengine CLI 查询 TDengine 验证是否创建相应数据库并写入数据：

```
taos> show databases;
              name              |
=================================
 information_schema             |
 performance_schema             |
 icinga2                        |
Query OK, 3 row(s) in set (0.001867s)

taos> use icinga2;
Database changed.

taos> show stables;
              name              |
=================================
 icinga.service.users.state_... |
 icinga.service.users.acknow... |
 icinga.service.procs.downti... |
 icinga.service.users.users     |
 icinga.service.procs.procs_min |
 icinga.service.users.users_min |
 icinga.check.max_check_atte... |
 icinga.service.procs.state_... |
 icinga.service.procs.procs_... |
 icinga.service.users.users_... |
 icinga.check.latency           |
 icinga.service.procs.procs_... |
 icinga.service.users.downti... |
 icinga.service.users.users_... |
 icinga.service.users.reachable |
 icinga.service.procs.procs     |
 icinga.service.procs.acknow... |
 icinga.service.procs.state     |
 icinga.service.procs.reachable |
 icinga.check.current_attempt   |
 icinga.check.execution_time    |
 icinga.service.users.state     |
Query OK, 22 row(s) in set (0.002317s)
```


:::note

- TDengine 默认生成的子表名是根据规则生成的唯一 ID 值。
:::
