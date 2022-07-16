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

## 添加数据节点

启动 CLI 程序 taos，然后执行：

```sql
CREATE DNODE "fqdn:port";
```

将新数据节点的 End Point 添加进集群的 EP 列表。“fqdn:port“需要用双引号引起来，否则出错。一个数据节点对外服务的 fqdn 和 port 可以通过配置文件 taos.cfg 进行配置，缺省是自动获取。【强烈不建议用自动获取方式来配置 FQDN，可能导致生成的数据节点的 End Point 不是所期望的】

然后启动新加入的数据节点的 taosd 进程，再通过 taos 查看数据节点状态：

```
taos> show dnodes;
   id   |            endpoint            | vnodes | support_vnodes |   status   |       create_time       |              note              |
============================================================================================================================================
      1 | trd01:6030                     |    100 |           1024 | ready      | 2022-07-15 16:47:47.726 |                                |
      2 | trd04:6030                     |      0 |           1024 | ready      | 2022-07-15 16:56:13.670 |                                |
Query OK, 2 rows affected (0.007031s)
```
从中可以看到两个 dnode 状态都为 ready

## 删除数据节点

先停止要删除的数据节点的 taosd 进程，然后启动 CLI 程序 taos，执行：

```sql
DROP DNODE "fqdn:port";
```
或者
```sql
DROP DNODE dnodeId;
```

通过 “fqdn:port” 或 dnodeID 来指定一个具体的节点都是可以的。其中 fqdn 是被删除的节点的 FQDN，port 是其对外服务器的端口号；dnodeID 可以通过 SHOW DNODES 获得。


:::warning

数据节点一旦被 drop 之后，不能重新加入集群。需要将此节点重新部署（清空数据文件夹）。集群在完成 `drop dnode` 操作之前，会将该 dnode 的数据迁移走。
请注意 `drop dnode` 和 停止 taosd 进程是两个不同的概念，不要混淆：因为删除 dnode 之前要执行迁移数据的操作，因此被删除的 dnode 必须保持在线状态。待删除操作结束之后，才能停止 taosd 进程。
一个数据节点被 drop 之后，其他节点都会感知到这个 dnodeID 的删除操作，任何集群中的节点都不会再接收此 dnodeID 的请求。
dnodeID 是集群自动分配的，不得人工指定。它在生成时是递增的，不会重复。

:::


