---
toc_max_heading_level: 4
title: "Double Replicas with High Availability"
sidebar_label: "Double Replicas"
---

## Introduction

This section describes the double replicas feature, which is available from TDengine Enterprise 3.3.0.0. Compared with triple-replicas database, double-replicas database can achieve high availability with lower hardware cost. Each Vgroup in a double-replicas database has only two Vnodes. When one Vnode fails, the Mnode can determine whether the other Vnode can provide services independently based on the data synchronization status.

## Create Database with 2 Replicas

You can create a database with 2 replicas using the SQL commmand as below:

```sql
CREATE DATABASE <db_name> REPLICA 2;
```

## View Vgroup Details

The state of the Vgroups of a database with 2 replicas can be viewed as follow:

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
- 0: the data is not synchronized between the two Vnodes in the Vgroup. In this state, if one of the Vnodes in the Vgroup is inaccessible and the other Vnode cannot be assigned the `AssignedLeader` role, the Vgroup will not be available for service.
- 1: the data is already synchronized between the two Vnodes in the Vgroup. In this state, if one of the Vnodes in the Vgroup is inaccessible, the other Vnode can be designated as the `AssignedLeader` role, and the Vgroup can continue to provide services.

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

## Recommendations

1. New Deployment

The key value of 2 replicas is to reduce storage cost while maintaining a certain level of high availability and high reliability. The recommended cluster deployment is as below:
- N dnodes, while >=3
- (N-1) dnodes are responsible for storing time series data
- Only 1 dnode is not involved in storing and reading time series data, i.e. no data replica is stored on it. You can configure parameter `supportVnodes` as 0 in `taos.cfg` to achieve this purpose.
- The dnode without storing any replica can be a low end server, because the usage of CPU/Memory is much lower than a normal dnode

2. Upgrade From Single Replica

Assuming you already have a cluster on which there are a few databases of single replica, the number of dnodes in this cluster is equal to or greater than 1. After upgrading, you need to make sure there are N (N>=3) dndoes in the cluster, while only one dnode is configured to have zero vnodes, i.e. `supportVnodes` is configured as 0. After that, you can use `alter database replica 2` to change one database to 2 replicas.
