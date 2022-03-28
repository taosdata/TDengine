# 集群部署
## 启动第一个数据节点
按照《立即开始》里的指示，启动第一个数据节点，例如 h1.taosdata.com，然后执行 taos, 启动 taos shell，从 shell 里执行命令"show dnodes;"，如下所示：
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
上述命令里，可以看到这个刚启动的这个数据节点的 End Point 是：h1.taos.com:6030，就是这个新集群的 firstEp。

## 启动后续数据节点
将后续的数据节点添加到现有集群，具体有以下几步：

按照《立即开始》一章的方法在每个物理节点启动 taosd；（注意：每个物理节点都需要在 taos.cfg 文件中将 firstEp 参数配置为新集群首个节点的 End Point——在本例中是 h1.taos.com:6030）

在第一个数据节点，使用 CLI 程序 taos，登录进 TDengine 系统，执行命令：

```mysql
CREATE DNODE "h2.taos.com:6030";
```
将新数据节点的 End Point (准备工作中第四步获知的) 添加进集群的 EP 列表。"fqdn:port"需要用双引号引起来，否则出错。请注意将示例的“h2.taos.com:6030" 替换为这个新数据节点的 End Point。

然后执行命令
```mysql
SHOW DNODES;
```
查看新节点是否被成功加入。如果该被加入的数据节点处于离线状态，请做两个检查：

查看该数据节点的 taosd 是否正常工作，如果没有正常运行，需要先检查为什么
查看该数据节点 taosd 日志文件 taosdlog.0 里前面几行日志(一般在/var/log/taos 目录)，看日志里输出的该数据节点 fqdn 以及端口号是否为刚添加的 End Point。如果不一致，需要将正确的 End Point 添加进去。
按照上述步骤可以源源不断的将新的数据节点加入到集群。

:::tip

任何已经加入集群在线的数据节点，都可以作为后续待加入节点的 firstEp。
firstEp 这个参数仅仅在该数据节点首次加入集群时有作用，加入集群后，该数据节点会保存最新的 mnode 的 End Point 列表，不再依赖这个参数。
接下来，配置文件中的 firstEp 参数就主要在客户端连接的时候使用了，例如 taos shell 如果不加参数，会默认连接由 firstEp 指定的节点。
两个没有配置 firstEp 参数的数据节点 dnode 启动后，会独立运行起来。这个时候，无法将其中一个数据节点加入到另外一个数据节点，形成集群。无法将两个独立的集群合并成为新的集群。

:::

## 数据节点管理
上面已经介绍如何从零开始搭建集群。集群组建完后，还可以随时添加新的数据节点进行扩容，或删除数据节点，并检查集群当前状态。

提示：

以下所有执行命令的操作需要先登陆进 TDengine 系统，必要时请使用 root 权限。
添加数据节点
执行 CLI 程序 taos，执行：

```mysql
CREATE DNODE "fqdn:port";
```
将新数据节点的 End Point 添加进集群的 EP 列表。"fqdn:port"需要用双引号引起来，否则出错。一个数据节点对外服务的 fqdn 和 port 可以通过配置文件 taos.cfg 进行配置，缺省是自动获取。【强烈不建议用自动获取方式来配置 FQDN，可能导致生成的数据节点的 End Point 不是所期望的】

## 删除数据节点
执行 CLI 程序 taos，执行：

```mysql
DROP DNODE "fqdn:port | dnodeID";
```
通过"fqdn:port"或"dnodeID"来指定一个具体的节点都是可以的。其中 fqdn 是被删除的节点的 FQDN，port 是其对外服务器的端口号；dnodeID 可以通过 SHOW DNODES 获得。

:::warning

一个数据节点一旦被 drop 之后，不能重新加入集群。需要将此节点重新部署（清空数据文件夹）。集群在完成 drop dnode 操作之前，会将该 dnode 的数据迁移走。
请注意 drop dnode 和 停止 taosd 进程是两个不同的概念，不要混淆：因为删除 dnode 之前要执行迁移数据的操作，因此被删除的 dnode 必须保持在线状态。待删除操作结束之后，才能停止 taosd 进程。
一个数据节点被 drop 之后，其他节点都会感知到这个 dnodeID 的删除操作，任何集群中的节点都不会再接收此 dnodeID 的请求。
dnodeID 是集群自动分配的，不得人工指定。它在生成时是递增的，不会重复。

:::

## 手动迁移数据节点
手动将某个 vnode 迁移到指定的 dnode。

执行 CLI 程序 taos，执行：

```mysql
ALTER DNODE <source-dnodeId> BALANCE "VNODE:<vgId>-DNODE:<dest-dnodeId>";
```
其中：source-dnodeId 是源 dnodeId，也就是待迁移的 vnode 所在的 dnodeID；vgId 可以通过 SHOW VGROUPS 获得，列表的第一列；dest-dnodeId 是目标 dnodeId。

:::warning

只有在集群的自动负载均衡选项关闭时(balance 设置为 0)，才允许手动迁移。
只有处于正常工作状态的 vnode 才能被迁移：master/slave，当处于 offline/unsynced/syncing 状态时，是不能迁移的。
迁移前，务必核实目标 dnode 的资源足够：CPU、内存、硬盘。

:::

## 查看数据节点
执行 CLI 程序 taos，执行：

```mysql
SHOW DNODES;
```
它将列出集群中所有的 dnode，每个 dnode 的 ID，end_point(fqdn:port)，状态(ready, offline 等），vnode 数目，还未使用的 vnode 数目等信息。在添加或删除一个数据节点后，可以使用该命令查看。

## 查看虚拟节点组
为充分利用多核技术，并提供 scalability，数据需要分片处理。因此 TDengine 会将一个 DB 的数据切分成多份，存放在多个 vnode 里。这些 vnode 可能分布在多个数据节点 dnode 里，这样就实现了水平扩展。一个 vnode 仅仅属于一个 DB，但一个 DB 可以有多个 vnode。vnode 的是 mnode 根据当前系统资源的情况，自动进行分配的，无需任何人工干预。

执行 CLI 程序 taos，执行：

 ```mysql
USE SOME_DATABASE;
SHOW VGROUPS;
 ```
