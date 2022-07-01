---
sidebar_label: icinga2
title: icinga2 写入
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
              name              |      created_time       |   ntables   |   vgroups   | replica | quorum |  days  |           keep           |  cache(MB)  |   blocks    |   minrows   |   maxrows   | wallevel |    fsync    | comp | cachelast | precision | update |   status   |
====================================================================================================================================================================================================================================================================================
 log                            | 2022-04-20 07:19:50.260 |          11 |           1 |       1 |      1 |     10 | 3650                     |          16 |           6 |         100 |        4096 |        1 |        3000 |    2 |         0 | ms        |      0 | ready      |
 icinga2                        | 2022-04-20 12:11:39.697 |          13 |           1 |       1 |      1 |     10 | 3650                     |          16 |           6 |         100 |        4096 |        1 |        3000 |    2 |         0 | ns        |      2 | ready      |
Query OK, 2 row(s) in set (0.001867s)

taos> use icinga2;
Database changed.

taos> show stables;
              name              |      created_time       | columns |  tags  |   tables    |
============================================================================================
 icinga.service.users.state_... | 2022-04-20 12:11:39.726 |       2 |      1 |           1 |
 icinga.service.users.acknow... | 2022-04-20 12:11:39.756 |       2 |      1 |           1 |
 icinga.service.procs.downti... | 2022-04-20 12:11:44.541 |       2 |      1 |           1 |
 icinga.service.users.users     | 2022-04-20 12:11:39.770 |       2 |      1 |           1 |
 icinga.service.procs.procs_min | 2022-04-20 12:11:44.599 |       2 |      1 |           1 |
 icinga.service.users.users_min | 2022-04-20 12:11:39.809 |       2 |      1 |           1 |
 icinga.check.max_check_atte... | 2022-04-20 12:11:39.847 |       2 |      3 |           2 |
 icinga.service.procs.state_... | 2022-04-20 12:11:44.522 |       2 |      1 |           1 |
 icinga.service.procs.procs_... | 2022-04-20 12:11:44.576 |       2 |      1 |           1 |
 icinga.service.users.users_... | 2022-04-20 12:11:39.796 |       2 |      1 |           1 |
 icinga.check.latency           | 2022-04-20 12:11:39.869 |       2 |      3 |           2 |
 icinga.service.procs.procs_... | 2022-04-20 12:11:44.588 |       2 |      1 |           1 |
 icinga.service.users.downti... | 2022-04-20 12:11:39.746 |       2 |      1 |           1 |
 icinga.service.users.users_... | 2022-04-20 12:11:39.783 |       2 |      1 |           1 |
 icinga.service.users.reachable | 2022-04-20 12:11:39.736 |       2 |      1 |           1 |
 icinga.service.procs.procs     | 2022-04-20 12:11:44.565 |       2 |      1 |           1 |
 icinga.service.procs.acknow... | 2022-04-20 12:11:44.554 |       2 |      1 |           1 |
 icinga.service.procs.state     | 2022-04-20 12:11:44.509 |       2 |      1 |           1 |
 icinga.service.procs.reachable | 2022-04-20 12:11:44.532 |       2 |      1 |           1 |
 icinga.check.current_attempt   | 2022-04-20 12:11:39.825 |       2 |      3 |           2 |
 icinga.check.execution_time    | 2022-04-20 12:11:39.898 |       2 |      3 |           2 |
 icinga.service.users.state     | 2022-04-20 12:11:39.704 |       2 |      1 |           1 |
Query OK, 22 row(s) in set (0.002317s)
```
