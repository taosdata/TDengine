# 集群管理

## 数据节点管理

上面已经介绍如何从零开始搭建集群。集群组建完后，还可以随时添加新的数据节点进行扩容，或删除数据节点，并检查集群当前状态。

提示：

以下所有执行命令的操作需要先登陆进 TDengine 系统，必要时请使用 root 权限。
添加数据节点
执行 CLI 程序 taos，执行：

```sql
CREATE DNODE "fqdn:port";
```

将新数据节点的 End Point 添加进集群的 EP 列表。"fqdn:port"需要用双引号引起来，否则出错。一个数据节点对外服务的 fqdn 和 port 可以通过配置文件 taos.cfg 进行配置，缺省是自动获取。【强烈不建议用自动获取方式来配置 FQDN，可能导致生成的数据节点的 End Point 不是所期望的】

## 删除数据节点

执行 CLI 程序 taos，执行：

```sql
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

```sql
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

```sql
SHOW DNODES;
```

它将列出集群中所有的 dnode，每个 dnode 的 ID，end_point(fqdn:port)，状态(ready, offline 等），vnode 数目，还未使用的 vnode 数目等信息。在添加或删除一个数据节点后，可以使用该命令查看。

## 查看虚拟节点组

为充分利用多核技术，并提供 scalability，数据需要分片处理。因此 TDengine 会将一个 DB 的数据切分成多份，存放在多个 vnode 里。这些 vnode 可能分布在多个数据节点 dnode 里，这样就实现了水平扩展。一个 vnode 仅仅属于一个 DB，但一个 DB 可以有多个 vnode。vnode 的是 mnode 根据当前系统资源的情况，自动进行分配的，无需任何人工干预。

执行 CLI 程序 taos，执行：

```sql
USE SOME_DATABASE;
SHOW VGROUPS;
```
