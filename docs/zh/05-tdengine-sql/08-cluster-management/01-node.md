---
sidebar_label: 节点管理
title: 节点管理
description: 管理 dnode、mnode、qnode、bnode 与 vnode 的 SQL 命令
---

组成 TDengine 集群的物理实体是 `dnode`（data node 的缩写），即运行在操作系统之上的进程。在 `dnode` 中可创建负责时序数据存储的 `vnode`（virtual node）。在多节点集群中，当某个数据库的 `replica` 为 `3` 时，该库中每个 `vgroup` 由 3 个 `vnode` 组成；当 `replica` 为 `1` 时，每个 `vgroup` 由 1 个 `vnode` 组成。若要将某数据库配置为多副本，集群中的 `dnode` 数量至少为 3。

在 `dnode` 上还可创建 `mnode`（management node），单个集群最多可创建三个 `mnode`。自 `v3.0.0.0` 起，为支持存算分离引入逻辑节点 `qnode`（query node）。`qnode` 与 `vnode` 既可共存于同一 `dnode`，也可分别部署在不同 `dnode` 上。

## 创建数据节点

```sql
CREATE DNODE {dnode_endpoint | dnode_host_name PORT port_val}
```

其中 `dnode_endpoint` 为 `hostname:port` 格式；也可分别指定 hostname 与 port。

实际操作中建议先创建 `dnode`，再启动对应进程，以便该 `dnode` 按配置文件中的 `firstEP` 立即加入集群。加入成功后，每个 `dnode` 都会被分配一个 ID。

## 查看数据节点

```sql
SHOW DNODES;
```

列出集群中所有数据节点，字段包括 `dnode` 的 ID、endpoint 与 status。

## 删除数据节点

```sql
DROP DNODE {dnode_id | dnode_endpoint} [FORCE | UNSAFE]
```

删除 `dnode` 并不等于停止对应进程。建议先删除 `dnode`，再停止其所对应的进程。

只有在线节点可被删除。若要强制删除离线节点，需指定 `FORCE`。

当节点上存在单副本且节点离线时，若要强制删除该节点，需指定 `UNSAFE`；此时数据不可再恢复。`FORCE` 与 `UNSAFE` 二者择一，不可同时指定。

## 修改数据节点配置

```sql
ALTER DNODE dnode_id dnode_option

ALTER ALL DNODES dnode_option
```

对支持动态修改的配置参数，可使用 `ALTER DNODE` 或 `ALTER ALL DNODES` 修改 `dnode` 中的参数值。自 `v3.3.4.0` 起，修改后的配置参数会自动持久化，数据库服务重启后仍然生效。

某参数是否支持动态修改，请参阅 [taosd 参考手册](../../12-operations-and-tooling/03-components/01-taosd.md)。

参数值需为字符格式。例如将 `dnode` 1 的日志输出级别改为 debug：

```sql
ALTER DNODE 1 'debugFlag' '143';
```

### 补充说明

配置参数在 `dnode` 中分为全局配置参数与局部配置参数，可通过 `SHOW VARIABLES` 或 `SHOW DNODE dnode_id VARIABLES` 中的 `category` 字段确认。

1. 局部配置参数：可使用 `ALTER DNODE` 或 `ALTER ALL DNODES` 更新某一个或全部 `dnode` 的局部配置参数。
2. 全局配置参数：要求各个 `dnode` 保持一致，因此只能使用 `ALTER ALL DNODES` 更新全部 `dnode` 的全局配置参数。

配置参数是否可动态修改，有以下三种情况：

1. 支持动态修改，立即生效
2. 支持动态修改，重启后生效
3. 不支持动态修改

对重启后生效的参数，可通过 `SHOW VARIABLES` 或 `SHOW DNODE dnode_id VARIABLES` 看到修改后的值，但需重启数据库服务后才真正生效。

## 创建管理节点

```sql
CREATE MNODE ON DNODE dnode_id
```

系统启动时默认在 `firstEP` 节点上创建一个 `mnode`。可使用本语句创建更多 `mnode` 以提高可用性。一个集群最多存在三个 `mnode`，一个 `dnode` 上只能创建一个 `mnode`。

## 查看管理节点

```sql
SHOW MNODES;
```

列出集群中所有管理节点，包括其 ID、所在 `dnode` 以及状态。

## 删除管理节点

```sql
DROP MNODE ON DNODE dnode_id;
```

删除 `dnode_id` 所指定的 `dnode` 上的 `mnode`。

## 创建查询节点

```sql
CREATE QNODE ON DNODE dnode_id;
```

系统启动时默认没有 `qnode`，可创建 `qnode` 以实现计算与存储分离。一个 `dnode` 上只能创建一个 `qnode`。

若某 `dnode` 的 `supportVnodes` 不为 `0`，同时又在其上创建了 `qnode`，则该 `dnode` 中既有负责存储的 `vnode`，又有负责查询计算的 `qnode`；若还创建了 `mnode`，则同一 `dnode` 上最多可同时存在这三种逻辑节点。也可通过配置实现物理分离：将 `supportVnodes` 设为 `0`，再仅创建 `mnode` 或 `qnode` 之一。

## 查看查询节点

```sql
SHOW QNODES;
```

列出集群中所有查询节点，包括 ID 及所在 `dnode`。

## 删除查询节点

```sql
DROP QNODE ON DNODE dnode_id;
```

删除 ID 为 `dnode_id` 的 `dnode` 上的 `qnode`，不影响该 `dnode` 本身的状态。

## 创建订阅节点

```sql
CREATE BNODE ON DNODE dnode_id [PROTOCOL protocol];
```

系统启动时默认没有 `bnode`，可创建 `bnode` 以启动订阅服务。一个 `dnode` 上只能创建一个 `bnode`。`PROTOCOL` 为可选配置项，未指定时默认为 `mqtt`，后续会扩展其它协议。`bnode` 创建成功后，`dnode` 会启动子进程 `taosmqtt`，对外提供订阅服务。

## 查看订阅节点

```sql
SHOW BNODES;
```

列出集群中所有订阅节点，包括 ID、protocol、创建时间及所在 `dnode`。

## 删除订阅节点

```sql
DROP BNODE ON DNODE dnode_id;
```

删除 ID 为 `dnode_id` 的 `dnode` 上的 `bnode`；该 `dnode` 上的 `taosmqtt` 子进程会退出，订阅服务停止。

## 关闭虚拟节点

```sql
CLOSE VNODE vgroup_id ON DNODE dnode_id;
```

关闭指定 `dnode` 上指定 `vgroup` 的 `vnode`。关闭操作会优雅刷写数据并停止该 `vnode` 运行，但不删除任何数据文件。被关闭的 `vnode` 在 `SHOW VNODES` 中将显示为 `offline`。

此命令需要超级用户权限。

**使用说明**

- 被关闭的 `vnode` 不再参与数据读写和 Raft 复制，有效副本数减少
- 若关闭的是 leader 副本，将触发 Raft 重新选举（写入短暂不可用，通常小于 1 秒）
- 关闭状态不跨 `taosd` 重启保留——重启后 `vnode` 自动恢复打开
- 建议在操作后通过 `SHOW VNODES` 确认状态变更

**示例**

```sql
-- 关闭 dnode 1 上 vgroup 2 的 vnode
CLOSE VNODE 2 ON DNODE 1;

-- 确认状态
SHOW VNODES;
```

## 打开虚拟节点

```sql
OPEN VNODE vgroup_id ON DNODE dnode_id;
```

重新打开指定 `dnode` 上此前通过 `CLOSE VNODE` 关闭的 `vnode`，恢复其正常运行。打开后 `vnode` 将重新加入 Raft 复制组并参与数据读写。

此命令需要超级用户权限。

**使用说明**

- 仅可对之前通过 `CLOSE VNODE` 关闭的 `vnode` 执行
- 打开操作涉及 `vnode` 初始化与 WAL 回放，耗时取决于 WAL 大小
- 打开后 `vnode` 需要追赶复制进度，期间可能暂时无法提供读服务

**示例**

```sql
-- 重新打开 dnode 1 上 vgroup 2 的 vnode
OPEN VNODE 2 ON DNODE 1;

-- 确认恢复
SHOW VNODES;
```

**常见错误**

| 错误信息 | 原因 | 解决方法 |
| ------------------------------------- | --- | --- |
| `Vgroup does not exist`               | `vgroupId` 不存在 | 执行 `SHOW VGROUPS` 确认 |
| `Vgroup not found on specified dnode` | 该 `vgroup` 在指定 `dnode` 上没有副本 | 执行 `SHOW VGROUPS` 确认副本分布 |
| `Vnode is already closed`             | `vnode` 已处于关闭状态 | 无需重复关闭 |
| `Vnode is not in closed state`        | `vnode` 未关闭，无法执行 `OPEN` | 确认是否已被 `CLOSE`，或是否已因重启自动恢复 |

## 查询集群状态

```sql
SHOW CLUSTER ALIVE;
```

查询当前集群是否可用，返回值含义如下：

- `0`：不可用
- `1`：完全可用
- `2`：部分可用（集群中部分节点下线，但其它节点仍可正常使用）

## 修改客户端配置

若将客户端也视为广义集群的一部分，可通过如下命令动态修改客户端配置参数：

```sql
ALTER LOCAL local_option
```

可使用以上语法更改客户端配置参数，无需重启客户端，修改后立即生效。

某参数是否支持动态修改，请参阅 [taosc 参考手册](../../12-operations-and-tooling/03-components/02-taosc.md)。

连接级参数修改命令 `SET TIMEZONE`、`SET FIRST_DAY_OF_WEEK` 请参见 [时区与自然时间单位](../10-time/01-timezone.md)。

## 查看客户端配置

```sql
SHOW LOCAL VARIABLES;
```
