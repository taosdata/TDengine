---
title: 高可用
description: TDengine 的高可用设计 
---

## Vnode 的高可用性

TDengine 通过多副本的机制来提供系统的高可用性，包括 vnode 和 mnode 的高可用性。

vnode 的副本数是与 DB 关联的，一个集群里可以有多个 DB，根据运营的需求，每个 DB 可以配置不同的副本数。创建数据库时，通过参数 replica 指定副本数（缺省为 1）。如果副本数为 1，系统的可靠性无法保证，只要数据所在的节点宕机，就将无法提供服务。集群的节点数必须大于等于副本数，否则创建表时将返回错误“more dnodes are needed”。比如下面的命令将创建副本数为 3 的数据库 demo：

```sql
CREATE DATABASE demo replica 3;
```

一个 DB 里的数据会被切片分到多个 vnode group，vnode group 里的 vnode 数目就是 DB 的副本数，同一个 vnode group 里各 vnode 的数据是完全一致的。为保证高可用性，vnode group 里的 vnode 一定要分布在不同的数据节点 dnode 里（实际部署时，需要在不同的物理机上），只要一个 vnode group 里超过半数的 vnode 处于工作状态，这个 vnode group 就能正常的对外服务。

一个数据节点 dnode 里可能有多个 DB 的数据，因此一个 dnode 离线时，可能会影响到多个 DB。如果一个 vnode group 里的一半或一半以上的 vnode 不工作，那么该 vnode group 就无法对外服务，无法插入或读取数据，这样会影响到它所属的 DB 的一部分表的读写操作。

因为 vnode 的引入，无法简单地给出结论：“集群中过半数据节点 dnode 工作，集群就应该工作”。但是对于简单的情形，很好下结论。比如副本数为 3，只有三个 dnode，那如果仅有一个节点不工作，整个集群还是可以正常工作的，但如果有两个数据节点不工作，那整个集群就无法正常工作了。

## Mnode 的高可用性

TDengine 集群是由 mnode（taosd 的一个模块，管理节点）负责管理的，为保证 mnode 的高可用，可以配置多个 mnode 副本，在集群启动时只有一个 mnode，用户可以通过 `create mnode` 来增加新的 mnode。用户可以通过该命令自主决定哪几个 dnode 会承担 mnode 的角色。为保证元数据的强一致性，在有多个 mnode 时，mnode 副本之间是通过同步的方式进行数据复制的。

一个集群有多个数据节点 dnode，但一个 dnode 至多运行一个 mnode 实例。用户可通过 CLI 程序 taos，在 TDengine 的 console 里，执行如下命令：

```sql
SHOW MNODES;
```

来查看 mnode 列表，该列表将列出 mnode 所处的 dnode 的 End Point 和角色（leader, follower, candidate, offline）。当集群中第一个数据节点启动时，该数据节点一定会运行一个 mnode 实例，否则该数据节点 dnode 无法正常工作，因为一个系统是必须有至少一个 mnode 的。

在 TDengine 3.0 及以后的版本中，数据同步采用 RAFT 协议，所以 mnode 的数量应该被设置为 1 个或者 3 个。
