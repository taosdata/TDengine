---
sidebar_label: 手动部署
title: 集群部署和管理
description: 使用命令行工具手动部署 TDengine 集群
---

## 准备工作

### 第零步

规划集群所有物理节点的 FQDN，将规划好的 FQDN 分别添加到每个物理节点的 /etc/hosts；修改每个物理节点的 /etc/hosts，将所有集群物理节点的 IP 与 FQDN 的对应添加好。【如部署了 DNS，请联系网络管理员在 DNS 上做好相关配置】

### 第一步

如果搭建集群的物理节点中，存有之前的测试数据，或者装过其他版本的 TDengine，请先将其删除，并清空所有数据（如果需要保留原有数据，请联系涛思交付团队进行旧版本升级、数据迁移），具体步骤请参考博客[《TDengine 多种安装包的安装和卸载》](https://www.taosdata.com/blog/2019/08/09/566.html)。

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

```

一定要修改的参数是 firstEp 和 fqdn。在每个数据节点，firstEp 需全部配置成一样，但 fqdn 一定要配置成其所在数据节点的值。其他参数可不做任何修改，除非你很清楚为什么要修改。

加入到集群中的数据节点 dnode，下表中涉及集群相关的参数必须完全相同，否则不能成功加入到集群中。

| **#** | **配置参数名称** | **含义**                             |
| ----- | ---------------- | ------------------------------------ |
| 1     | statusInterval   | dnode 向 mnode 报告状态时长          |
| 2     | timezone         | 时区                                 |
| 3     | locale           | 系统区位信息及编码格式               |
| 4     | charset          | 字符集编码                           |
| 5     | ttlChangeOnWrite | ttl 到期时间是否伴随表的修改操作改变 |

## 启动集群

按照《立即开始》里的步骤，启动第一个数据节点，例如 h1.taosdata.com，然后执行 taos，启动 TDengine CLI，在其中执行命令 “SHOW DNODES”，如下所示：

```
taos> show dnodes;
id | endpoint | vnodes | support_vnodes | status | create_time | note |
============================================================================================================================================
1 | h1.taosdata.com:6030 | 0 | 1024 | ready | 2022-07-16 10:50:42.673 | |
Query OK, 1 rows affected (0.007984s)


```

上述命令里，可以看到刚启动的数据节点的 End Point 是：h1.taos.com:6030，就是这个新集群的 firstEp。

## 添加数据节点

将后续的数据节点添加到现有集群，具体有以下几步：

按照《立即开始》一章的方法在每个物理节点启动 taosd；（注意：每个物理节点都需要在 taos.cfg 文件中将 firstEp 参数配置为新集群首个节点的 End Point——在本例中是 h1.taos.com:6030）

在第一个数据节点，使用 CLI 程序 taos，登录进 TDengine 系统，执行命令：

```sql
CREATE DNODE "h2.taos.com:6030";
````

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
接下来，配置文件中的 firstEp 参数就主要在客户端连接的时候使用了，例如 TDengine CLI 如果不加参数，会默认连接由 firstEp 指定的节点。
两个没有配置 firstEp 参数的数据节点 dnode 启动后，会独立运行起来。这个时候，无法将其中一个数据节点加入到另外一个数据节点，形成集群。无法将两个独立的集群合并成为新的集群。

:::

## 查看数据节点

启动 TDengine CLI 程序 taos，然后执行：

```sql
SHOW DNODES;
```

它将列出集群中所有的 dnode，每个 dnode 的 ID，end_point（fqdn:port），状态（ready，offline 等），vnode 数目，还未使用的 vnode 数目等信息。在添加或删除一个数据节点后，可以使用该命令查看。

输出如下（具体内容仅供参考，取决于实际的集群配置）

```
taos> show dnodes;
   id   |            endpoint            | vnodes | support_vnodes |   status   |       create_time       |              note              |
============================================================================================================================================
      1 | trd01:6030                     |    100 |           1024 | ready      | 2022-07-15 16:47:47.726 |                                |
Query OK, 1 rows affected (0.006684s)
```

## 查看虚拟节点组

为充分利用多核技术，并提供横向扩展能力，数据需要分片处理。因此 TDengine 会将一个 DB 的数据切分成多份，存放在多个 vnode 里。这些 vnode 可能分布在多个数据节点 dnode 里，这样就实现了水平扩展。一个 vnode 仅仅属于一个 DB，但一个 DB 可以有多个 vnode。vnode 所在的数据节点是 mnode 根据当前系统资源的情况，自动进行分配的，无需任何人工干预。

启动 CLI 程序 taos，然后执行：

```sql
USE SOME_DATABASE;
SHOW VGROUPS;
```

输出如下（具体内容仅供参考，取决于实际的集群配置）

```
taos> use db;
Database changed.

taos> show vgroups;
  vgroup_id  |            db_name             |   tables    |  v1_dnode   | v1_status  |  v2_dnode   | v2_status  |  v3_dnode   | v3_status  |    status    |   nfiles    |  file_size  | tsma |
================================================================================================================================================================================================
           2 | db                             |           0 |           1 | leader     |        NULL | NULL       |        NULL | NULL       | NULL         |        NULL |        NULL |    0 |
           3 | db                             |           0 |           1 | leader     |        NULL | NULL       |        NULL | NULL       | NULL         |        NULL |        NULL |    0 |
           4 | db                             |           0 |           1 | leader     |        NULL | NULL       |        NULL | NULL       | NULL         |        NULL |        NULL |    0 |
Query OK, 8 row(s) in set (0.001154s)
```

## 删除数据节点

启动 CLI 程序 taos，执行：

```sql
DROP DNODE dnodeId;
```

dnodeId 可以通过 SHOW DNODES 获得。

:::warning

数据节点一旦被 drop 之后，不能重新加入集群。需要将此节点重新部署（清空数据文件夹）。集群在完成 `drop dnode` 操作之前，会将该 dnode 的数据迁移走。
请注意 `drop dnode` 和 停止 taosd 进程是两个不同的概念，不要混淆：因为删除 dnode 之前要执行迁移数据的操作，因此被删除的 dnode 必须保持在线状态。待删除操作结束之后，才能停止 taosd 进程。
一个数据节点被 drop 之后，其他节点都会感知到这个 dnodeID 的删除操作，任何集群中的节点都不会再接收此 dnodeID 的请求。
dnodeID 是集群自动分配的，不得人工指定。它在生成时是递增的，不会重复。

:::

## 常见问题

1、建立集群时使用 CREATE DNODE 增加新节点后，新节点始终显示 offline 状态？
```sql
  1）首先要检查增加的新节点上的 taosd 服务是否已经正常启动

  2）如果已经启动，再检查到新节点的网络是否通畅，可以使用 ping fqdn 验证下

  3）如果前面两步都没有问题，这一步要检查新节点做为独立集群在运行了，可以使用 taos -h fqdn 连接上后，show dnodes; 命令查看.
    如果显示的列表与你主节点上显示的不一致，说明此节点自己单独成立了一个集群，解决的方法是停止新节点上的服务，然后清空新节点上
    taos.cfg 中配置的 dataDir 目录下的所有文件，重新启动新节点服务即可解决。
```
