---
title: High Availability
sidebar_label: High Availability
description: This document describes how TDengine implements high availability.
---

## High Availability of Vnode

High availability of vnode can be achieved through replicas in TDengine.

A TDengine cluster can have multiple databases. Each database has a number of vnodes associated with it. A different number of replicas can be configured for each DB. When creating a database, the parameter `replica` is used to specify the number of replicas. The default value for `replica` is 1. Naturally, a single replica cannot guarantee high availability since if one node is down, the data service is unavailable. Note that the number of dnodes in the cluster must NOT be lower than the number of replicas set for any DB, otherwise the `create table` operation will fail with error "more dnodes are needed". The SQL statement below is used to create a database named "demo" with 3 replicas.

```sql
CREATE DATABASE demo replica 3;
```

The data in a DB is divided into multiple shards and stored in multiple vgroups. The number of vnodes in each vgroup is determined by the number of replicas set for the DB. The vnodes in each vgroup store exactly the same data. For the purpose of high availability, the vnodes in a vgroup must be located in different dnodes on different hosts. As long as over half of the vnodes in a vgroup are in an online state, the vgroup is able to provide data access. Otherwise the vgroup can't provide data access for reading or inserting data.

There may be data for multiple DBs in a dnode. When a dnode is down, multiple DBs may be affected. While in theory, the cluster will provide data access for reading or inserting data if over half the vnodes in vgroups are online, because of the possibly complex mapping between vnodes and dnodes, it is difficult to guarantee that the cluster will work properly if over half of the dnodes are online.

## High Availability of Mnode

Each TDengine cluster is managed by `mnode`, which is a module of `taosd`. For the high availability of mnode, multiple mnodes can be configured in the system. When a TDengine cluster is started from scratch, there is only one `mnode`, then you can use command `create mnode` to and start corresponding dnode to add more mnodes. 

```sql
SHOW MNODES;
```

The end point and role/status (leader, follower, candidate, offline) of all mnodes can be shown by the above command. When the first dnode is started in a cluster, there must be one mnode in this dnode. Without at least one mnode, the cluster cannot work.

From TDengine 3.0.0, RAFT protocol is used to guarantee the high availability, so the number of mnodes is should be 1 or 3.
