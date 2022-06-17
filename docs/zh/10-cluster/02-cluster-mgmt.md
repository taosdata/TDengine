---
title: 数据节点管理
---

上面已经介绍如何从零开始搭建集群。集群组建完成后，可以随时查看集群中当前的数据节点的状态，还可以添加新的数据节点进行扩容，删除数据节点，甚至手动进行数据节点之间的负载均衡操作。

:::note

以下所有执行命令的操作需要先登陆进 TDengine 系统，必要时请使用 root 权限。

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
   id   |           end_point            | vnodes | cores  |   status   | role  |       create_time       |      offline reason      |
======================================================================================================================================
      1 | localhost:6030                 |      9 |      8 | ready      | any   | 2022-04-15 08:27:09.359 |                          |
Query OK, 1 row(s) in set (0.008298s)
```

## 查看虚拟节点组

为充分利用多核技术，并提供 scalability，数据需要分片处理。因此 TDengine 会将一个 DB 的数据切分成多份，存放在多个 vnode 里。这些 vnode 可能分布在多个数据节点 dnode 里，这样就实现了水平扩展。一个 vnode 仅仅属于一个 DB，但一个 DB 可以有多个 vnode。vnode 所在的数据节点是 mnode 根据当前系统资源的情况，自动进行分配的，无需任何人工干预。

启动 CLI 程序 taos，然后执行：

```sql
USE SOME_DATABASE;
SHOW VGROUPS;
```

输出如下（具体内容仅供参考，取决于实际的集群配置）

```
taos> show dnodes;
   id   |           end_point            | vnodes | cores  |   status   | role  |       create_time       |      offline reason      |
======================================================================================================================================
      1 | localhost:6030                 |      9 |      8 | ready      | any   | 2022-04-15 08:27:09.359 |                          |
Query OK, 1 row(s) in set (0.008298s)

taos> use db;
Database changed.

taos> show vgroups;
    vgId     |   tables    |  status  |   onlines   | v1_dnode | v1_status | compacting  |
==========================================================================================
          14 |       38000 | ready    |           1 |        1 | master    |           0 |
          15 |       38000 | ready    |           1 |        1 | master    |           0 |
          16 |       38000 | ready    |           1 |        1 | master    |           0 |
          17 |       38000 | ready    |           1 |        1 | master    |           0 |
          18 |       37001 | ready    |           1 |        1 | master    |           0 |
          19 |       37000 | ready    |           1 |        1 | master    |           0 |
          20 |       37000 | ready    |           1 |        1 | master    |           0 |
          21 |       37000 | ready    |           1 |        1 | master    |           0 |
Query OK, 8 row(s) in set (0.001154s)
```

## 添加数据节点

启动 CLI 程序 taos，然后执行：

```sql
CREATE DNODE "fqdn:port";
```

将新数据节点的 End Point 添加进集群的 EP 列表。“fqdn:port“需要用双引号引起来，否则出错。一个数据节点对外服务的 fqdn 和 port 可以通过配置文件 taos.cfg 进行配置，缺省是自动获取。【强烈不建议用自动获取方式来配置 FQDN，可能导致生成的数据节点的 End Point 不是所期望的】

示例如下：
```
taos> create dnode "localhost:7030";
Query OK, 0 of 0 row(s) in database (0.008203s)

taos> show dnodes;
   id   |           end_point            | vnodes | cores  |   status   | role  |       create_time       |      offline reason      |
======================================================================================================================================
      1 | localhost:6030                 |      9 |      8 | ready      | any   | 2022-04-15 08:27:09.359 |                          |
      2 | localhost:7030                 |      0 |      0 | offline    | any   | 2022-04-19 08:11:42.158 | status not received      |
Query OK, 2 row(s) in set (0.001017s)
```

在上面的示例中可以看到新创建的 dnode 的状态为 offline，待该 dnode 被启动并连接上配置文件中指定的 firstEp后再次查看，得到如下结果（示例）

```
taos> show dnodes;
   id   |           end_point            | vnodes | cores  |   status   | role  |       create_time       |      offline reason      |
======================================================================================================================================
      1 | localhost:6030                 |      3 |      8 | ready      | any   | 2022-04-15 08:27:09.359 |                          |
      2 | localhost:7030                 |      6 |      8 | ready      | any   | 2022-04-19 08:14:59.165 |                          |
Query OK, 2 row(s) in set (0.001316s)
```
从中可以看到两个 dnode 状态都为 ready


## 删除数据节点

启动 CLI 程序 taos，然后执行：

```sql
DROP DNODE "fqdn:port";
```
或者
```sql
DROP DNODE dnodeId;
```

通过 “fqdn:port” 或 dnodeID 来指定一个具体的节点都是可以的。其中 fqdn 是被删除的节点的 FQDN，port 是其对外服务器的端口号；dnodeID 可以通过 SHOW DNODES 获得。

示例如下：
```
taos> show dnodes;
   id   |           end_point            | vnodes | cores  |   status   | role  |       create_time       |      offline reason      |
======================================================================================================================================
      1 | localhost:6030                 |      9 |      8 | ready      | any   | 2022-04-15 08:27:09.359 |                          |
      2 | localhost:7030                 |      0 |      0 | offline    | any   | 2022-04-19 08:11:42.158 | status not received      |
Query OK, 2 row(s) in set (0.001017s)

taos> drop dnode 2;
Query OK, 0 of 0 row(s) in database (0.000518s)

taos> show dnodes;
   id   |           end_point            | vnodes | cores  |   status   | role  |       create_time       |      offline reason      |
======================================================================================================================================
      1 | localhost:6030                 |      9 |      8 | ready      | any   | 2022-04-15 08:27:09.359 |                          |
Query OK, 1 row(s) in set (0.001137s)
```

上面的示例中，初次执行 `show dnodes` 列出了两个 dnode, 执行 `drop dnode 2` 删除其中 ID 为 2 的 dnode 之后再次执行 `show dnodes`，可以看到只剩下 ID 为 1 的 dnode 。

:::warning

数据节点一旦被 drop 之后，不能重新加入集群。需要将此节点重新部署（清空数据文件夹）。集群在完成 `drop dnode` 操作之前，会将该 dnode 的数据迁移走。
请注意 `drop dnode` 和 停止 taosd 进程是两个不同的概念，不要混淆：因为删除 dnode 之前要执行迁移数据的操作，因此被删除的 dnode 必须保持在线状态。待删除操作结束之后，才能停止 taosd 进程。
一个数据节点被 drop 之后，其他节点都会感知到这个 dnodeID 的删除操作，任何集群中的节点都不会再接收此 dnodeID 的请求。
dnodeID 是集群自动分配的，不得人工指定。它在生成时是递增的，不会重复。

:::

## 手动迁移数据节点

手动将某个 vnode 迁移到指定的 dnode。

启动 CLI 程序 taos，然后执行：

```sql
ALTER DNODE <source-dnodeId> BALANCE "VNODE:<vgId>-DNODE:<dest-dnodeId>";
```

其中：source-dnodeId 是源 dnodeId，也就是待迁移的 vnode 所在的 dnodeID；vgId 可以通过 SHOW VGROUPS 获得，列表的第一列；dest-dnodeId 是目标 dnodeId。

首先执行 `show vgroups` 查看 vgroup 的分布情况 
```
taos> show vgroups;
    vgId     |   tables    |  status  |   onlines   | v1_dnode | v1_status | compacting  |
==========================================================================================
          14 |       38000 | ready    |           1 |        3 | master    |           0 |
          15 |       38000 | ready    |           1 |        3 | master    |           0 |
          16 |       38000 | ready    |           1 |        3 | master    |           0 |
          17 |       38000 | ready    |           1 |        3 | master    |           0 |
          18 |       37001 | ready    |           1 |        3 | master    |           0 |
          19 |       37000 | ready    |           1 |        1 | master    |           0 |
          20 |       37000 | ready    |           1 |        1 | master    |           0 |
          21 |       37000 | ready    |           1 |        1 | master    |           0 |
Query OK, 8 row(s) in set (0.001314s)
```

从中可以看到在 dnode 3 中有5个 vgroup，而 dnode 1 有 3 个 vgroup，假定我们想将其中 vgId 为18 的 vgroup 从 dnode 3 迁移到 dnode 1

```
taos> alter dnode 3 balance "vnode:18-dnode:1";

DB error: Balance already enabled (0.00755
```

上面的结果表明目前所在数据库已经启动了 balance 选项，所以无法进行手动迁移。

停止整个集群，将两个 dnode 的配置文件中的 balance 都设置为 0 （默认为1）之后，重新启动集群，再次执行 ` alter dnode` 和 `show vgroups` 命令如下
```
taos> alter dnode 3 balance "vnode:18-dnode:1";
Query OK, 0 row(s) in set (0.000575s)

taos> show vgroups;
    vgId     |   tables    |  status  |   onlines   | v1_dnode | v1_status | v2_dnode | v2_status | compacting  |
=================================================================================================================
          14 |       38000 | ready    |           1 |        3 | master    |        0 | NULL      |           0 |
          15 |       38000 | ready    |           1 |        3 | master    |        0 | NULL      |           0 |
          16 |       38000 | ready    |           1 |        3 | master    |        0 | NULL      |           0 |
          17 |       38000 | ready    |           1 |        3 | master    |        0 | NULL      |           0 |
          18 |       37001 | ready    |           2 |        1 | slave     |        3 | master    |           0 |
          19 |       37000 | ready    |           1 |        1 | master    |        0 | NULL      |           0 |
          20 |       37000 | ready    |           1 |        1 | master    |        0 | NULL      |           0 |
          21 |       37000 | ready    |           1 |        1 | master    |        0 | NULL      |           0 |
Query OK, 8 row(s) in set (0.001242s)
```

从上面的输出可以看到 vgId 为 18 的 vnode 被从 dnode 3 迁移到了 dnode 1。

:::warning

只有在集群的自动负载均衡选项关闭时（balance 设置为 0），才允许手动迁移。
只有处于正常工作状态的 vnode 才能被迁移：master/slave；当处于 offline/unsynced/syncing 状态时，是不能迁移的。
迁移前，务必核实目标 dnode 的资源足够：CPU、内存、硬盘。

:::

