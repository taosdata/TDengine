---
title: 三副本方案
sidebar_label: 三副本方案
toc_max_heading_level: 4
---

TDengine TSDB 的三副本方案采用 RAFT 算法来实现数据的一致性，包括元数据和时序数据。一个虚拟节点组（VGroup）构成了一个 RAFT 组；VGroup 中的虚拟节点（Vnode），便是该 RAFT 组的成员节点，也称之为副本。

1. 每个 Vnode 都有自己的角色，可以是 Leader（领导者）、Follower（跟随者）、Candidate（候选人）。
2. 每个 Vnode 都维护了一份连续的日志，用于记录数据写入、变更、或删除等操作的所有指令。日志是由一系列有序的日志条目组成，每条日志都有唯一的编号，用于标识日志协商或执行的进度。
3. Leader 角色的 Vnode 提供读写服务，在故障节点不超过半数的情况下保证集群的高可用性。此外，即使发生了节点重启及 Leader 重新选举等事件后，RAFT 协议也能够始终保证新产生的 Leader 可以提供已经写入成功的全部完整数据的读写服务。
4. 每一次对数据库的变更请求（比如数据写入），都对应一条日志。在持续写入数据的过程中，会按照协议机制在每个成员节点上产生完全相同的日志记录，并且以相同的顺序执行数据变更操作，以 WAL 文件的形式存储在数据文件目录中。
5. 只有当过半数的节点把该条日志追加到 WAL 文件，并且收到确认消息之后，这条日志才会被 Leader 认为是安全的；此时该日志进入 committed 状态，完成数据的插入，随后该日志被标记为 applied 的状态。

多副本工作原理参见 [数据写入与复制流程](../../26-tdinternal/01-arch.md#数据写入与复制流程)   

## 集群配置

三副本要求集群至少配置三个服务器节点，基本部署与配置步骤如下：
1. 确定服务器节点数量、主机名或域名，配置好所有节点的域名解析：DNS 或 /etc/hosts
2. 各节点分别安装 TDengine TSDB 服务端安装包，按需编辑好各节点 taos.cfg
3. 启动各节点 taosd 服务，其他服务可按需启动（taosadapter/taosx/taoskeeper/taos-explorer)

## 运维命令

### 创建集群

创建三节点的集群

```sql
CREATE dnode <dnode_ep> port <dnode_port>;
CREATE dnode <dnode_ep> port <dnode_port>;
```

创建三副本的 Mnode，保证 Mnode 高可用

```sql
CREATE mnode on dnode <dnode_id>;
CREATE mnode on dnode <dnode_id>;
```

### 数据库创建

创建三副本的数据库

```sql
create database <dbname> replica 3 vgroups xx buffer xx ...
```

### 修改数据库副本数

创建了单副本数据库后，如果希望改为三副本时，可通过 alter 命令来实现，反之亦然

```sql
alter database <dbname> replica 3|1
```

## 常见问题

### 1. 创建三副本数据库或修改为三副本时，报错：DB error: Out of dnodes
- 服务器节点数不足：原因是服务器节点数少于三个。
- 解决方案：增加服务器节点数量，满足最低要求。

### 2. 创建三副本数据库或 split vgroup 时，报错：DB error: Vnodes exhausted
- 服务器可用 Vnodes 不足：原因是某些服务器节点可用 Vnodes 数少于建库或 split vgroup 的需求数。
- 解决方案：调整服务器 CPU 数量、SupportVnodes 配置参数，满足建库要求。