---
title: 三副本方案
sidebar_label: 三副本方案
toc_max_heading_level: 4
---


本节介绍 TDengine 三副本方案的配置与使用。

TDengine 的三副本方案采用 RAFT 算法来实现数据的一致性，包括元数据和时序数据。一个虚拟节点组（VGroup）构成了一个 RAFT 组；虚拟节点组的虚拟数据节点（Vnode），便是该 RAFT 组的成员节点，也称之为副本。

1. 每个 Vnode 都有自己的角色，它们可以是 Follower（跟随者）、Candidate（候选人）、Leader（领导者）。
2. 每个 Vnode 都维护了一份连续的日志（Log），用于记录数据写入、变更、或删除等操作的所有指令。日志是由一系列有序的日志条目 (Log Entry) 组成，每个 Log Entry 都有唯一的编号（Index），用于标识日志协商或执行的进度。
3. Leader 角色的 Vnode 提供读写服务，在故障节点不超过半数的情况下保证集群的高可用性。此外，即使发生了节点重启及 Leader 重新选举等事件后，RAFT 也能够始终保证新产生的 Leader 可以提供已经写入成功的全部完整数据的读写服务。
4. 每一次对数据库的变更请求（比如 insert），都对应一个 Log Entry。在持续写入数据的过程中，会按照协议机制在每个成员节点上产生完全相同的日志记录，并且以相同的顺序执行数据变更操作，以 WAL 的形式存储在数据文件目录中。
5. 每一个 Log Entry 携带的 Index，就代表数据变更的版本号；当一个数据写入请求发出后，必定至少过半数节点上完成写入才会把“写入成功”返回给客户端；这部分涉及 Log entry 的两种重要的状态，committed 和 applied。
6. 只有当过半数的节点把该条 SQL 的写入信息追加到文件系统上的 WAL，并且收到确认消息之后，这条 Log entry 才会被 Leader 认为是安全的；此时该日志进入 committed 状态，完成数据的插入，随后该 Log Entry 便被标记为 applied 的状态。

<img src={replica3} width="560" alt="多副本工作原理图" />


## 方案特点

1. 最小配置的服务器节点数为 3 个
2. 三副本为数据库建库参数，不同数据库可按需选择副本数
3. 支持单副本与三副本之间切换(前提是节点数量满足需求、各节点可用 vnode 数量/内存/存储空间足够)
4. 支持 TDengine 集群的完整特性，包括：读缓存、数据订阅、流计算等
5. 支持 TDengine 所有语言连接器以及连接方式
6. 不支持三副本与双副本之间的切换
7. 不支持三副本切换为双活，除非另外部署一套实例与当前实例组成双活方案

## 集群配置

三副本要求集群至少配置三个服务器节点，基本部署与配置步骤如下
1. 确定服务器节点数量、主机名或域名，配置好所有节点的域名解析：DNS 或 /etc/hosts
2. 各节点分别安装 TDengine 企业版服务端安装包，按需编辑好各节点 taos.cfg
3. 启动各节点 taosd 服务 (其他服务可按需启动：taosadapter/taosx/taoskeeper/taos-explorer)
4. 登入taos CLI，将所有节点添加入集群 create dnode xxxx
5. 创建三个 mnode create mnode on dnode nn

## 数据库创建

创建三副本数据库。DBA 按需创建指定的三副本数据库

```sql
create database <dbname> replica 3 vgroups xx buffer xx ...
```

## 修改数据库副本数

创建了一个单副本数据库后，希望改为三副本时，可通过 alter 命令来实现，反之亦然

```sql
alter database <dbname> replica 3|1
```

## 常见问题

### 1. 创建三副本数据库或修改为三副本时，报错：DB error: Out of dnodes
- 服务器节点数不足：原因是服务器节点数少于三个。
- 解决方案：增加服务器节点数量，满足最低要求。

### 2. 创建三副本数据库或 split vgroup 时，报错：DB error: Vnodes exhausted
- 服务器可用 Vnodes 不足：原因是某些服务器节点可用 Vnodes 数少于建库或 split vgroup 的需求数。
- 解决方案：调整服务器 CPU 数量、SupportVnodes 数量，满足建库要求。