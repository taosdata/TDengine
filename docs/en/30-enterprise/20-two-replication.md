---
toc_max_heading_level: 4
title: "two-replication"
sidebar_label: "two-replication"
---

## Introduction

This section describes database two-replication feature, which is available from TDengine Enterprise 3.3.0.0.

When one of the Vnodes in the Vgroup that belongs to the two-replication database fails, the Mnode Leader specifies the other Vnode in the Vgroup to be the AssignedLeader according to synchronization. AssignedLeader can respond to external requests without confirmation from other Vnodes.

## Create Database with two-replication
You can create a database with two-replication using the SQL commmand as below:

```sql
CREATE DATABASE db REPLICA 2;
```

## View two-replication Vgroup Details

The state of two-replication Vgroup can be viewed as follow:

```sql
show arbgroups;

select * from information_schema.ins_arbgroups;
            db_name             |  vgroup_id  | v1_dnode | v2_dnode | is_sync | assigned_dnode |         assigned_token         |
=================================================================================================================================
 db                             |           2 |        2 |        3 |       0 | NULL           | NULL                           |
 db                             |           3 |        1 |        2 |       0 |              1 | d1#g3#1714119404630#663        |
 db                             |           4 |        1 |        3 |       1 | NULL           | NULL                           |

```
is_sync has the following two values:
- 0: indicates that Vgroup data is not synchronized
- 1: indicates that Vgroup data is synchronized.

Only Vnodes in the synchronized Vgroup can be designated as AssignedLeader

assigned_dnode：
- Identifies the DnodeId of the Vnode designated as AssignedLeader
- If AssignedLeader is not specified, the status column displays NULL

assigned_token：
- Identifies the Token of the Vnode designated as AssignedLeader
- If AssignedLeader is not specified, the status column displays NULL


## Drop Database with two-replication
You can drop a database with two-replication using the SQL commmand as below:
```sql
DROP DATABASE db;
```

## Restriction and Limitation
1. Splite or Redistribute the Vgroup associated with a two-replication database is not supported
2. The replication of a two-replication database cannot be altered.
3. One-replication database can be altered to two-repication.
