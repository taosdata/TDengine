---
toc_max_heading_level: 4
title: "Double Replicas with High Availability"
sidebar_label: "Double Replicas"
---

## Introduction

This section describes the double replicas feature, which is available from TDengine Enterprise 3.3.0.0. With this feature, you can create a database with 2 replicas instead of 3, but can still achieve high availability with lower hardware cost. The replica number is specified when creating a database. If one vnode of a vgroup is down over a period, the remaining vnode will take a special role "AssignedLeader" to continue to provide service. A key difference between replica 2 and replica 3 is the number of vnodes: 2 versus 3.

## Create Database with 2 Replicas

You can create a database with 2 replicas using the SQL commmand as below:

```sql
CREATE DATABASE <db_name> REPLICA 2;
```

## View Vgroup Details

The state of the vgroups of a database with 2 replicas can be viewed as follow:

```sql
show arbgroups;

select * from information_schema.ins_arbgroups;
            db_name             |  vgroup_id  | v1_dnode | v2_dnode | is_sync | assigned_dnode |         assigned_token         |
=================================================================================================================================
 db                             |           2 |        2 |        3 |       0 | NULL           | NULL                           |
 db                             |           3 |        1 |        2 |       0 |              1 | d1#g3#1714119404630#663        |
 db                             |           4 |        1 |        3 |       1 | NULL           | NULL                           |

```

**Description**
is_sync has the following two values:
- 0: the data is not synchronized between the two vnodes in the vgroup. If the data is not synchorinzed and the vnode leader is down, the vgroup will fail to serve.
- 1: the data is already synchronized between the two vnode in the vgroup. Only when the data is in synchronized state, when the vnode leader is down, the remaining follower vnode can be designated to `AssignedLeader` role.

Only Vnodes in the synchronized Vgroup can be designated as `AssignedLeader` role.

assigned_dnode：
- Identifies the DnodeId of the Vnode in `AssignedLeader` role
- If AssignedLeader is not specified, this column is NULL

assigned_token：
- Identifies the Token of the Vnode in `AssignedLeader` role
- If AssignedLeader is not specified, this column is NULL

## Drop Database 

You can drop a database with 2 replicas using the SQL commmand as below:
```sql
DROP DATABASE db;
```

## Restrictions and Limitations
1. Splite or Redistribute the Vgroup associated with a two-replication database is not supported, the command will be rejected
2. The replica parameter of a database with 2 replicas can't be altered
3. Database with single replica can be altered to 2 or 3 replicas
