---
sidebar_label: 缓存
title: 缓存
description: "TDengine 内部的缓存设计"
---

为了实现高效的写入和查询，TDengine 充分利用了各种缓存技术，本节将对 TDengine 中对缓存的使用做详细的说明。

## 写缓存

TDengine 采用时间驱动缓存管理策略（First-In-First-Out，FIFO），又称为写驱动的缓存管理机制。这种策略有别于读驱动的数据缓存模式（Least-Recent-Used，LRU），直接将最近写入的数据保存在系统的缓存中。当缓存达到临界值的时候，将最早的数据批量写入磁盘。一般意义上来说，对于物联网数据的使用，用户最为关心最近产生的数据，即当前状态。TDengine 充分利用了这一特性，将最近到达的（当前状态）数据保存在缓存中。

每个 vnode 的写入缓存大小在创建数据库时决定，创建数据库时的两个关键参数 vgroups 和 buffer 分别决定了该数据库中的数据由多少个 vgroup 处理，以及向其中的每个 vnode 分配多少写入缓存。buffer 的单位是MB。

```sql
create database db0 vgroups 100 buffer 16
```

理论上缓存越大越好，但超过一定阈值后再增加缓存对写入性能提升并无帮助，一般情况下使用默认值即可。

## 读缓存

在创建数据库时可以选择是否缓存该数据库中每个子表的最新数据。由参数 cachemodel 设置，分为四种情况：
- none: 不缓存
- last_row: 缓存子表最近一行数据，这将显著改善 last_row 函数的性能
- last_value: 缓存子表每一列最近的非 NULL 值，这将显著改善无特殊影响（比如 WHERE, ORDER BY, GROUP BY, INTERVAL）时的 last 函数的性能
- both: 同时缓存最近的行和列，即等同于上述 cachemodel 值为 last_row 和 last_value 的行为同时生效

## 元数据缓存

为了更高效地处理查询和写入，每个 vnode 都会缓存自己曾经获取到的元数据。元数据缓存由创建数据库时的两个参数 pages 和 pagesize 决定。pagesize 的单位是 kb。

```sql
create database db0 pages 128 pagesize 16
```

上述语句会为数据库 db0 的每个 vnode 创建 128 个 page，每个 page 16kb 的元数据缓存。

## 文件系统缓存

TDengine 利用 WAL 技术来提供基本的数据可靠性。写入 WAL 本质上是以顺序追加的方式写入磁盘文件。此时文件系统缓存在写入性能中也会扮演关键角色。在创建数据库时可以利用 wal 参数来选择性能优先或者可靠性优先。
- 1: 写 WAL 但不执行 fsync ，新写入 WAL 的数据保存在文件系统缓存中但并未写入磁盘，这种方式性能优先
- 2: 写 WAL 且执行 fsync，新写入 WAL 的数据被立即同步到磁盘上，可靠性更高

## 客户端缓存

为了进一步提升整个系统的处理效率，除了以上提到的服务端缓存技术之外，在 TDengine 的所有客户端都要调用的核心库 libtaos.so （也称为 taosc ）中也充分利用了缓存技术。在 taosc 中会缓存所访问过的各个数据库、超级表以及子表的元数据，集群的拓扑结构等关键元数据。

当有多个客户端同时访问 TDengine 集群，且其中一个客户端对某些元数据进行了修改的情况下，有可能会出现其它客户端所缓存的元数据不同步或失效的情况，此时需要在客户端执行 "reset query cache" 以让整个缓存失效从而强制重新拉取最新的元数据重新建立缓存。
