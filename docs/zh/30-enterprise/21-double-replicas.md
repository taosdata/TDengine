---
toc_max_heading_level: 4
title: "双副本"
sidebar_label: "双副本"
---

## 概述

本节简要介绍双副本功能，该功能从 3.3.0.0 版本的 TDengine 企业版中开始提供。相较于三副本数据库，双副本数据库可以在降低硬件成本同时保证一定的高可用能力。双副本数据库中每个 Vgroup 仅有两个成员。在其中一个 Vnode 故障时，Mnode 可根据数据同步状态状态，裁定另一 Vnode 是否可独自对外提供服务。

## 创建双副本数据库

通过以下 SQL 命令创建双副本数据库：

```sql
CREATE DATABASE db REPLICA 2;
```

## 查看双副本 Vgroups 的状态

通过以下 SQL 命令参看双副本数据库中各 Vgroup 的状态：

```sql
show arbgroups;

select * from information_schema.ins_arbgroups;
            db_name             |  vgroup_id  | v1_dnode | v2_dnode | is_sync | assigned_dnode |         assigned_token         |
=================================================================================================================================
 db                             |           2 |        2 |        3 |       0 | NULL           | NULL                           |
 db                             |           3 |        1 |        2 |       0 |              1 | d1#g3#1714119404630#663        |
 db                             |           4 |        1 |        3 |       1 | NULL           | NULL                           |

```
is_sync 有以下两种取值：
- 0: Vgroup 数据未达成同步。在此状态下，如果 Vgroup 中的某一 Vnode 不可访问，另一个 Vnode 无法被指定为 `AssignedLeader` role，该 Vgroup 将无法提供服务。
- 1: Vgroup 数据达成同步。在此状态下，如果 Vgroup 中的某一 Vnode 不可访问，另一个 Vnode 可以被指定为 `AssignedLeader` role，该 Vgroup 可以继续提供服务。

assigned_dnode：
- 标识被指定为 AssignedLeader 的 Vnode 的 DnodeId
- 未指定 AssignedLeader时，该列显示 NULL

assigned_token：
- 标识被指定为 AssignedLeader 的 Vnode 的 Token
- 未指定 AssignedLeader时，该列显示 NULL


## 删除双副本数据库

通过以下 SQL 命令删除双副本数据库：

```sql
DROP DATABASE db;
```

## 约束与限制
1. 暂不支持对双副本数据库相关 Vgroup 进行 SPLITE VGROUP 或 REDISTRIBUTE VGROUP 操作
2. 单副本数据库可变更为双副本数据库，但不支持从双副本变更为其它副本数，也不支持从三副本变更为双副本

## 最佳实践

1. 全新部署

双副本的主要价值在于节省存储成本的同时能够有一定的高可用和高可靠能力。在实践中，推荐配置为：
- N 节点集群 （其中 N>=3）
- 其中 N-1 个 dnode 负责存储时序数据
- 第 N 个 dnode 不参与时序数据的存储和读取，即其上不保存副本；可以通过 `supportVnodes` 这个参数为 0 来实现这个目标
- 不存储数据副本的 dnode 对 CPU/Memory 资源的占用也较低，可以使用较低配置服务器

2. 从单副本升级

假定已经有一个单副本集群，其结点数为 N (N>=1)，欲将其升级为双副本集群，升级后需要保证 N>=3，且新加入的某个节点的 `supportVnodes` 参数配置为 0。在集群升级完成后使用  `alter database replica 2` 的命令修改某个特定数据库的副本数。
