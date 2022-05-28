---
sidebar_label: HA & LB
title: High Availability and Load Balancing
---

## High Availability of Vnode

High availability of vnode and mnode can be achieved through replicas in TDengine.

A TDengine cluster can have multiple databases. Each database has a number of vnodes associated with it. A different number of replicas can be configured for each DB. When creating a database, the parameter `replica` is used to specify the number of replicas. The default value for `replica` is 1. Naturally, a single replica cannot guarantee high availability since if one node is down, the data service is unavailable. Note that the number of dnodes in the cluster must NOT be lower than the number of replicas set for any DB, otherwise the `create table` operation will fail with error "more dnodes are needed". The SQL statement below is used to create a database named "demo" with 3 replicas.

```sql
CREATE DATABASE demo replica 3;
```

The data in a DB is divided into multiple shards and stored in multiple vgroups. The number of vnodes in each vgroup is determined by the number of replicas set for the DB. The vnodes in each vgroup store exactly the same data. For the purpose of high availability, the vnodes in a vgroup must be located in different dnodes on different hosts. As long as over half of the vnodes in a vgroup are in an online state, the vgroup is able to provide data access. Otherwise the vgroup can't provide data access for reading or inserting data.

There may be data for multiple DBs in a dnode. When a dnode is down, multiple DBs may be affected. While in theory, the cluster will provide data access for reading or inserting data if over half the vnodes in vgroups are online, because of the possibly complex mapping between vnodes and dnodes, it is difficult to guarantee that the cluster will work properly if over half of the dnodes are online.

## High Availability of Mnode

Each TDengine cluster is managed by `mnode`, which is a module of `taosd`. For the high availability of mnode, multiple mnodes can be configured using system parameter `numOfMNodes`. The valid range for `numOfMnodes` is [1,3]. To ensure data consistency between mnodes, data replication between mnodes is performed synchronously.

There may be multiple dnodes in a cluster, but only one mnode can be started in each dnode. Which one or ones of the dnodes will be designated as mnodes is automatically determined by TDengine according to the cluster configuration and system resources. The command `show mnodes` can be executed in TDengine `taos` to show the mnodes in the cluster.

```sql
SHOW MNODES;
```

The end point and role/status (master, slave, unsynced, or offline) of all mnodes can be shown by the above command. When the first dnode is started in a cluster, there must be one mnode in this dnode. Without at least one mnode, the cluster cannot work. If `numOfMNodes` is configured to 2, another mnode will be started when the second dnode is launched.

For the high availability of mnode, `numOfMnodes` needs to be configured to 2 or a higher value. Because the data consistency between mnodes must be guaranteed, the replica confirmation parameter `quorum` is set to 2 automatically if `numOfMNodes` is set to 2 or higher.

:::note
If high availability is important for your system, both vnode and mnode must be configured to have multiple replicas.

:::

## Load Balancing

Load balancing will be triggered in 3 cases without manual intervention.

- When a new dnode joins the cluster, automatic load balancing may be triggered. Some data from other dnodes may be transferred to the new dnode automatically.
- When a dnode is removed from the cluster, the data from this dnode will be transferred to other dnodes automatically.
- When a dnode is too hot, i.e. too much data has been stored in it, automatic load balancing may be triggered to migrate some vnodes from this dnode to other dnodes.

:::tip
Automatic load balancing is controlled by the parameter `balance`, 0 means disabled and 1 means enabled. This is set in the file [taos.cfg](https://docs.tdengine.com/reference/config/#balance).

:::

## Dnode Offline

When a dnode is offline, it can be detected by the TDengine cluster. There are two cases:

- The dnode comes online before the threshold configured in `offlineThreshold` is reached. The dnode is still in the cluster and data replication is started automatically. The dnode can work properly after the data sync is finished.

- If the dnode has been offline over the threshold configured in `offlineThreshold` in `taos.cfg`, the dnode will be removed from the cluster automatically. A system alert will be generated and automatic load balancing will be triggered if `balance` is set to 1. When the removed dnode is restarted and becomes online, it will not join the cluster automatically. The system administrator has to manually join the dnode to the cluster.

:::note
If all the vnodes in a vgroup (or mnodes in mnode group) are in offline or unsynced status, the master node can only be voted on, after all the vnodes or mnodes in the group become online and can exchange status. Following this, the vgroup (or mnode group) is able to provide service.

:::

## Arbitrator

The "arbitrator" component is used to address the special case when the number of replicas is set to an even number like 2,4 etc. If half of the vnodes in a vgroup don't work, it is impossible to vote and select a master node. This situation also applies to mnodes if the number of mnodes is set to an even number like 2,4 etc.

To resolve this problem, a new arbitrator component named `tarbitrator`, an abbreviation of TDengine Arbitrator, was introduced. The `tarbitrator` simulates a vnode or mnode but it's only responsible for network communication and doesn't handle any actual data access. As long as more than half of the vnode or mnode, including Arbitrator, are available the vnode group or mnode group can provide data insertion or query services normally.

Normally, it's prudent to configure the replica number for each DB or system parameter `numOfMNodes` to be an odd number. However, if a user is very sensitive to storage space, a replica number of 2 plus arbitrator component can be used to achieve both lower cost of storage space and high availability.

Arbitrator component is installed with the server package. For details about how to install, please refer to [Install](/operation/pkg-install). The `-p` parameter of `tarbitrator` can be used to specify the port on which it provides service.

In the configuration file `taos.cfg` of each dnode, parameter `arbitrator` needs to be configured to the end point of the `tarbitrator` process. Arbitrator component will be used automatically if the replica is configured to an even number and will be ignored if the replica is configured to an odd number.

Arbitrator can be shown by executing command in TDengine CLI `taos` with its role shown as "arb".

```sql
SHOW DNODES;
```
