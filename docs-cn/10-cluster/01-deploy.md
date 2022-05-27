---
title: 集群部署
---

## 准备工作

### 第零步

规划集群所有物理节点的 FQDN，将规划好的 FQDN 分别添加到每个物理节点的 /etc/hosts；修改每个物理节点的 /etc/hosts，将所有集群物理节点的 IP 与 FQDN 的对应添加好。【如部署了 DNS，请联系网络管理员在 DNS 上做好相关配置】

### 第一步

如果搭建集群的物理节点中，存有之前的测试数据、装过 1.X 的版本，或者装过其他版本的 TDengine，请先将其删除，并清空所有数据（如果需要保留原有数据，请联系涛思交付团队进行旧版本升级、数据迁移），具体步骤请参考博客[《TDengine 多种安装包的安装和卸载》](https://www.taosdata.com/blog/2019/08/09/566.html)。

:::note
因为 FQDN 的信息会写进文件，如果之前没有配置或者更改 FQDN，且启动了 TDengine。请一定在确保数据无用或者备份的前提下，清理一下之前的数据（rm -rf /var/lib/taos/\*）；
:::

:::note
客户端所在服务器也需要配置，确保它可以正确解析每个节点的 FQDN 配置，不管是通过 DNS 服务，还是修改 hosts 文件。
:::

### 第二步

确保集群中所有主机在端口 6030-6042 上的 TCP/UDP 协议能够互通。

### 第三步

在所有物理节点安装 TDengine，且版本必须是一致的，但不要启动 taosd。安装时，提示输入是否要加入一个已经存在的 TDengine 集群时，第一个物理节点直接回车创建新集群，后续物理节点则输入该集群任何一个在线的物理节点的 FQDN:端口号（默认 6030）；

### 第四步

检查所有数据节点，以及应用程序所在物理节点的网络设置：

每个物理节点上执行命令 `hostname -f`，查看和确认所有节点的 hostname 是不相同的（应用驱动所在节点无需做此项检查）；

每个物理节点上执行 ping host，其中 host 是其他物理节点的 hostname，看能否 ping 通其它物理节点；如果不能 ping 通，需要检查网络设置，或 /etc/hosts 文件（Windows 系统默认路径为 C:\Windows\system32\drivers\etc\hosts），或 DNS 的配置。如果无法 ping 通，是无法组成集群的；

从应用运行的物理节点，ping taosd 运行的数据节点，如果无法 ping 通，应用是无法连接 taosd 的，请检查应用所在物理节点的 DNS 设置或 hosts 文件；

每个数据节点的 End Point 就是输出的 hostname 外加端口号，比如 h1.taosdata.com:6030。

### 第五步

修改 TDengine 的配置文件（所有节点的文件 /etc/taos/taos.cfg 都需要修改）。假设准备启动的第一个数据节点 End Point 为 h1.taosdata.com:6030，其与集群配置相关参数如下：

```c
// firstEp 是每个数据节点首次启动后连接的第一个数据节点
firstEp               h1.taosdata.com:6030

// 必须配置为本数据节点的 FQDN，如果本机只有一个 hostname，可注释掉本项
fqdn                  h1.taosdata.com

// 配置本数据节点的端口号，缺省是 6030
serverPort            6030

// 副本数为偶数的时候，需要配置，请参考《Arbitrator 的使用》的部分
arbitrator            ha.taosdata.com:6042
```

一定要修改的参数是 firstEp 和 fqdn。在每个数据节点，firstEp 需全部配置成一样，但 fqdn 一定要配置成其所在数据节点的值。其他参数可不做任何修改，除非你很清楚为什么要修改。

加入到集群中的数据节点 dnode，涉及集群相关的下表 9 项参数必须完全相同，否则不能成功加入到集群中。

| **#** | **配置参数名称**   | **含义**                                    |
| ----- | ------------------ | ------------------------------------------- |
| 1     | numOfMnodes        | 系统中管理节点个数                          |
| 2     | mnodeEqualVnodeNum | 一个 mnode 等同于 vnode 消耗的个数          |
| 3     | offlineThreshold   | dnode 离线阈值，超过该时间将导致 Dnode 离线 |
| 4     | statusInterval     | dnode 向 mnode 报告状态时长                 |
| 5     | arbitrator         | 系统中裁决器的 End Point                    |
| 6     | timezone           | 时区                                        |
| 7     | balance            | 是否启动负载均衡                            |
| 8     | maxTablesPerVnode  | 每个 vnode 中能够创建的最大表个数           |
| 9     | maxVgroupsPerDb    | 每个 DB 中能够使用的最大 vgroup 个数        |

:::note
在 2.0.19.0 及更早的版本中，除以上 9 项参数外，dnode 加入集群时，还会要求 locale 和 charset 参数的取值也一致。

:::

## 启动集群

### 启动第一个数据节点

按照《立即开始》里的步骤，启动第一个数据节点，例如 h1.taosdata.com，然后执行 taos，启动 taos shell，从 shell 里执行命令“SHOW DNODES”，如下所示：

```
Welcome to the TDengine shell from Linux, Client Version:2.0.0.0


Copyright (c) 2017 by TAOS Data, Inc. All rights reserved.

taos> show dnodes;
 id |       end_point    | vnodes | cores | status | role |      create_time        |
=====================================================================================
  1 |  h1.taos.com:6030  |      0 |     2 |  ready |  any | 2020-07-31 03:49:29.202 |
Query OK, 1 row(s) in set (0.006385s)

taos>
```

上述命令里，可以看到刚启动的数据节点的 End Point 是：h1.taos.com:6030，就是这个新集群的 firstEp。

### 启动后续数据节点

将后续的数据节点添加到现有集群，具体有以下几步：

按照《立即开始》一章的方法在每个物理节点启动 taosd；（注意：每个物理节点都需要在 taos.cfg 文件中将 firstEp 参数配置为新集群首个节点的 End Point——在本例中是 h1.taos.com:6030）

在第一个数据节点，使用 CLI 程序 taos，登录进 TDengine 系统，执行命令：

```sql
CREATE DNODE "h2.taos.com:6030";
```

将新数据节点的 End Point（准备工作中第四步获知的）添加进集群的 EP 列表。“fqdn:port”需要用双引号引起来，否则出错。请注意将示例的“h2.taos.com:6030” 替换为这个新数据节点的 End Point。

然后执行命令

```sql
SHOW DNODES;
```

查看新节点是否被成功加入。如果该被加入的数据节点处于离线状态，请做两个检查：

查看该数据节点的 taosd 是否正常工作，如果没有正常运行，需要先检查为什么?
查看该数据节点 taosd 日志文件 taosdlog.0 里前面几行日志（一般在 /var/log/taos 目录），看日志里输出的该数据节点 fqdn 以及端口号是否为刚添加的 End Point。如果不一致，需要将正确的 End Point 添加进去。
按照上述步骤可以源源不断的将新的数据节点加入到集群。

:::tip

任何已经加入集群在线的数据节点，都可以作为后续待加入节点的 firstEp。
firstEp 这个参数仅仅在该数据节点首次加入集群时有作用，加入集群后，该数据节点会保存最新的 mnode 的 End Point 列表，不再依赖这个参数。
接下来，配置文件中的 firstEp 参数就主要在客户端连接的时候使用了，例如 taos shell 如果不加参数，会默认连接由 firstEp 指定的节点。
两个没有配置 firstEp 参数的数据节点 dnode 启动后，会独立运行起来。这个时候，无法将其中一个数据节点加入到另外一个数据节点，形成集群。无法将两个独立的集群合并成为新的集群。

:::
