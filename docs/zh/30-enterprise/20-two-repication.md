---
toc_max_heading_level: 4
title: "双副本"
sidebar_label: "双副本"
---

## 概述

本节简要介绍双副本功能，该功能从 3.3.0.0 版本的 TDengine 企业版中开始提供。

从属于双副本数据库的 Vgroup 中某一 Vnode 故障时，Mnode Leader 可根据同步情况指定该 Vgroup 中另一 Vnode 成为 AssignedLeader。AssignedLeader 无需其他 Vnode 确认即可响应外部请求。

## 创建双副本数据库
通过以下 SQL 命令创建双副本数据库：

```sql
CREATE DATABASE db REPLICA 2;
```

## 查看双副本 Vgroups 的状态

通过以下 SQL 命令参看各双副本 Vgroup 状态：

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
- 0: 当 Vgroup 数据未达成同步时
- 1: 当 Vgroup 数据达成同步时。

仅达成同步的 Vgroup 中的 Vnode 可被指定为 AssignedLeader

assigned_dnode：
- 标识被指定为 AssignedLeader 的 Vnode 的 DnodeId
- 未指定 AssignedLeader时，状态列显示 NULL

assigned_token：
- 标识被指定为 AssignedLeader 的 Vnode 的 Token
- 未指定 AssignedLeader时，状态列显示 NULL


## 删除双副本数据库
通过以下 SQL 命令删除双副本数据库：

```sql
DROP DATABASE db;
```

## 约束与限制
1. 暂不支持对双副本数据库相关 Vgroup 进行 Splite 或 Redistribute 操作
2. 除单副本数据库可变更为双副本外，暂不支持双副本与其他副本数相互变更
