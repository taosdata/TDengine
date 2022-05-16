---
sidebar_label: HA & LB
title: High Availability and Load Balancing
---

## High Availability of Vnode

High availability of vnode and mnode can be achieved through replicas in TDengine.

The number of vnodes is associated with each DB, there can be multiple DBs in a TDengine cluster. For the purpose of operation, different number of replicas can be configured properly for each DB. When creating a database, the parameter `replica` is used to specify the number of replicas, the default value is 1. With single replica, the high availability of the system can't be guaranteed. Whenever one node is down, data service would be unavailable. The number of dnodes in the cluster must NOT be lower than the number of replicas set for any DB, otherwise the `create table` operation would fail with error "more dnodes are needed". Below SQL statement is used to create a database named as "demo" with 3 replicas.

```sql
CREATE DATABASE demo replica 3;
```

The data in a DB is divided into multiple shards and stored in multiple vgroups. The number of vnodes in each group is determined by the number of replicas set for the DB. The vnodes in each vgroups store exactly same data. For the purpose of high availability, the vnodes in a vgroup must be located in different dnodes on different hosts. As long as over half of the vnodes in a vgroup are in online state, the vgroup is able to serve data access. Otherwise the vgroup can't handle any data access for reading or inserting data.

There may be data for multiple DBs in a dnode. Once a dnode is down, multiple DBs may be affected. However, it's hard to say the cluster is guaranteed to work properly as long as over half of dnodes are online because vnodes are introduced and there may be complex mapping between vnodes and dnodes.

## High Availability of Mnode

Each TDengine cluster is managed by `mnode`, which is a module of `taosd`. For the high availability of mnode, multiple mnodes can be configured using system parameter `numOfMNodes`, the valid time range is [1,3]. To make sure the data consistency between mnodes, the data replication between mnodes is performed in synchronous way.

There may be multiple dnodes in a cluster, but only one mnode can be started in each dnode. Which one or ones of the dnodes will be designated as mnodes is automatically determined by TDengine according to the cluster configuration and system resources. Command `show mnodes` can be executed in TDengine `taos` to show the mnodes in the cluster.

```sql
SHOW MNODES;
```

The end point and role/status (master, slave, unsynced, or offline) of all mnodes can be shown by the above command. When the first dnode is started in a cluster, there must be one mnode in this dnode, because there must be at least one mnode otherwise the cluster doesn't work. If `numOfMNodes` is configured to 2, another mnode will be started when the second dnode is launched.

For the high availability of mnode, `numOfMnodes` needs to be configured to 2 or a higher value. Because the data consistency between mnodes must be guaranteed, the replica confirmation parameter `quorum` is set to 2 automatically if `numOfMNodes` is set to 2 or higher.

:::note
If high availability is important for your system, both vnode and mnode must be configured to have multiple replicas. How to configure for them are different and have been described.

:::

## Load Balance

Load balance will be triggered in 3 cades without manual intervention.

- When a new dnode is joined in the cluster, automatic load balancing may be triggered, some data from some dnodes may be transferred to the new dnode automatically.
- When a dnode is removed from the cluster, the data from this dnode will be transferred to other dnodes automatically.
- When a dnode is too hot, i.e. too much data has been stored in it, automatic load balancing may be triggered to migrate some vnodes from this dnode to other dnodes.
- :::tip
  Automatic load balancing is controlled by parameter `balance`, 0 means disabled and 1 means enabled.

:::

## Dnode Offline

When a dnode is offline, it can be detected by the TDengine cluster. There are two cases:

- The dnode becomes online again before the threshold configured in `offlineThreshold` is reached, it is still in the cluster and data replication is started automatically. The dnode can work properly after the data syncup is finished.

- If the dnode has been offline over the threshold configured in `offlineThreshold` in `taos.cfg`, the dnode will be removed from the cluster automatically. System alert will be generated and automatic load balancing will be triggered too if `balance` is set to 1. When the removed dnode is restarted and becomes online, it will not be joined in the cluster automatically, it can only be joined manually by the system operator.

:::note
If all the vnodes in a vgroup (or mnodes in mnode group) are in offline or unsynced status, the master node can only be voted after all the vnodes or mnodes in the group become online and can exchange status, then the vgroup (or mnode group) is able to provide service.

:::

## Arbitrator

If the number of replicas is set to an even number like 2, when half of the vnodes in a vgroup don't work master node can't be voted. Similar case is also applicable to mnode if the number of mnodes is set to an even number like 2.

To resolve this problem, a new arbitrator component named `tarbitrator`, abbreviated for TDengine Arbitrator, was introduced. Arbitrator simulates a vnode or mnode but it's only responsible for network communication and doesn't handle any actual data access. With Arbitrator, any vgroup or mnode group can be considered as having number of member nodes and master node can be selected.

Normally, it's suggested to configure replica number of each DB or system parameter `numOfMNodes` to an odd number. However, if a user is very sensitive to storage space, replica number of 2 plus arbitrator component can be used to achieve both lower cost of storage space and high availability.

Arbitrator component is installed with the server package. For details about how to install, please refer to [Install](/operation/pkg-install). The `-p` parameter of `tarbitrator` can be used to specify the port on which it provides service.

In the configuration file `taos.cfg` of each dnode, parameter `arbitrator` needs to be configured to the end point of the `tarbitrator` process. arbitrator component will be used automatically if the replica is configured to an even number and will be ignored if the replica is configured to an odd number.

Arbitrator can be shown by executing command in TDengine CLI `taos` with its role shown as "arb".

```sql
SHOW DNODES;
```
