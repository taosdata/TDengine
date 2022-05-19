---
sidebar_label: Operation
title: Manage DNODEs
---

It has been introduced that how to deploy and start a cluster from scratch. Once a cluster is ready, the dnode status in the cluster can be shown at any time, new dnode can be added to scale out the cluster, an existing dnode can be removed, even load balance can be performed manually.\

:::note
All the commands to be introduced in this chapter need to be run through TDengine CLI, sometimes it's necessary to use root privilege.

:::

## Show DNODEs

below command can be executed in TDengine CLI `taos` to list all dnodes in the cluster, including ID, end point (fqdn:port), status (ready, offline), number of vnodes, number of free vnodes, etc. It's suggested to execute this command to check after adding or removing a dnode.

```sql
SHOW DNODES;
```

Below is the example output of this command.

```
taos> show dnodes;
   id   |           end_point            | vnodes | cores  |   status   | role  |       create_time       |      offline reason      |
======================================================================================================================================
      1 | localhost:6030                 |      9 |      8 | ready      | any   | 2022-04-15 08:27:09.359 |                          |
Query OK, 1 row(s) in set (0.008298s)
```

## Show VGROUPs

To utilize system resources efficiently and provide scalability, data sharding is required. The data of each database is divided into multiple shards and stored in multiple vnodes. These vnodes may be located in different dnodes, scaling out can be achieved by adding more vnodes from more dnodes. Each vnode can only be used for a single DB, but one DB can have multiple vnodes. The allocation of vnode is scheduled automatically by mnode according to system resources of the dnodes.

Launch TDengine CLI `taos` and execute below command:

```sql
USE SOME_DATABASE;
SHOW VGROUPS;
```

The example output is as below:

```
taos> show dnodes;
   id   |           end_point            | vnodes | cores  |   status   | role  |       create_time       |      offline reason      |
======================================================================================================================================
      1 | localhost:6030                 |      9 |      8 | ready      | any   | 2022-04-15 08:27:09.359 |                          |
Query OK, 1 row(s) in set (0.008298s)

taos> use db;
Database changed.

taos> show vgroups;
    vgId     |   tables    |  status  |   onlines   | v1_dnode | v1_status | compacting  |
==========================================================================================
          14 |       38000 | ready    |           1 |        1 | master    |           0 |
          15 |       38000 | ready    |           1 |        1 | master    |           0 |
          16 |       38000 | ready    |           1 |        1 | master    |           0 |
          17 |       38000 | ready    |           1 |        1 | master    |           0 |
          18 |       37001 | ready    |           1 |        1 | master    |           0 |
          19 |       37000 | ready    |           1 |        1 | master    |           0 |
          20 |       37000 | ready    |           1 |        1 | master    |           0 |
          21 |       37000 | ready    |           1 |        1 | master    |           0 |
Query OK, 8 row(s) in set (0.001154s)
```

## Add DNODE

Launch TDengine CLI `taos` and execute the command below to add the end point of a new dnode into the EPI (end point) list of the cluster. "fqdn:port" must be quoted using double quotes.

```sql
CREATE DNODE "fqdn:port";
```

The example output is as below:

```
taos> create dnode "localhost:7030";
Query OK, 0 of 0 row(s) in database (0.008203s)

taos> show dnodes;
   id   |           end_point            | vnodes | cores  |   status   | role  |       create_time       |      offline reason      |
======================================================================================================================================
      1 | localhost:6030                 |      9 |      8 | ready      | any   | 2022-04-15 08:27:09.359 |                          |
      2 | localhost:7030                 |      0 |      0 | offline    | any   | 2022-04-19 08:11:42.158 | status not received      |
Query OK, 2 row(s) in set (0.001017s)
```

It can be seen that the status of the new dnode is "offline", once the dnode is started and connects the firstEp of the cluster, execute the command again and get below example output, from which it can be seen that two dnodes are both in "ready" status.

```
taos> show dnodes;
   id   |           end_point            | vnodes | cores  |   status   | role  |       create_time       |      offline reason      |
======================================================================================================================================
      1 | localhost:6030                 |      3 |      8 | ready      | any   | 2022-04-15 08:27:09.359 |                          |
      2 | localhost:7030                 |      6 |      8 | ready      | any   | 2022-04-19 08:14:59.165 |                          |
Query OK, 2 row(s) in set (0.001316s)
```

## Drop DNODE

Launch TDengine CLI `taos` and execute the command below to drop or remove a dndoe from the cluster. In the command, `dnodeId` can be gotten from `show dnodes`.

```sql
DROP DNODE "fqdn:port";
```

or

```sql
DROP DNODE dnodeId;
```

The example output is as below：

```
taos> show dnodes;
   id   |           end_point            | vnodes | cores  |   status   | role  |       create_time       |      offline reason      |
======================================================================================================================================
      1 | localhost:6030                 |      9 |      8 | ready      | any   | 2022-04-15 08:27:09.359 |                          |
      2 | localhost:7030                 |      0 |      0 | offline    | any   | 2022-04-19 08:11:42.158 | status not received      |
Query OK, 2 row(s) in set (0.001017s)

taos> drop dnode 2;
Query OK, 0 of 0 row(s) in database (0.000518s)

taos> show dnodes;
   id   |           end_point            | vnodes | cores  |   status   | role  |       create_time       |      offline reason      |
======================================================================================================================================
      1 | localhost:6030                 |      9 |      8 | ready      | any   | 2022-04-15 08:27:09.359 |                          |
Query OK, 1 row(s) in set (0.001137s)
```

In the above example, when `show dnodes` is executed the first time, two dnodes are shown. Then `drop dnode 2` is executed, after that from the output of executing `show dnodes` again it can be seen that only the dnode with ID 1 is still in the cluster.

:::note

- Once a dnode is dropped, it can't rejoin the cluster. To rejoin, the dnode needs to deployed again after cleaning up the data directory. Normally, before dropping a dnode, the data belonging to the dnode needs to be migrated to other place.
- Please be noted that `drop dnode` is different from stopping `taosd` process. `drop dnode` just removes the dnode out of TDengine cluster. Only after a dnode is dropped, can the corresponding `taosd` process be stopped.
- Once a dnode is dropped, other dnodes in the cluster will be notified of the drop and will not accept the request from the dropped dnode.
- dnodeID is allocated automatically and can't be interfered manually. dnodeID is generated in ascending order without duplication.

:::

## Move VNODE

A vnode can be manually moved from one dnode to another.

Launch TDengine CLI `taos` and execute below command:

```sql
ALTER DNODE <source-dnodeId> BALANCE "VNODE:<vgId>-DNODE:<dest-dnodeId>";
```

In the above command, `source-dnodeId` is the original dnodeId where the vnode resides, `dest-dnodeId` specifies the target dnode. vgId (vgroup ID) can be shown by `SHOW VGROUPS `.

Firstly `show vgroups` is executed to show the vgrup distribution.

```
taos> show vgroups;
    vgId     |   tables    |  status  |   onlines   | v1_dnode | v1_status | compacting  |
==========================================================================================
          14 |       38000 | ready    |           1 |        3 | master    |           0 |
          15 |       38000 | ready    |           1 |        3 | master    |           0 |
          16 |       38000 | ready    |           1 |        3 | master    |           0 |
          17 |       38000 | ready    |           1 |        3 | master    |           0 |
          18 |       37001 | ready    |           1 |        3 | master    |           0 |
          19 |       37000 | ready    |           1 |        1 | master    |           0 |
          20 |       37000 | ready    |           1 |        1 | master    |           0 |
          21 |       37000 | ready    |           1 |        1 | master    |           0 |
Query OK, 8 row(s) in set (0.001314s)
```

It can be seen that there are 5 vgroups in dnode 3 and 3 vgroups in node 1, now we want to move vgId 18 from dnode 3 to dnode 1. Execute below command in `taos`

```
taos> alter dnode 3 balance "vnode:18-dnode:1";

DB error: Balance already enabled (0.00755
```

However, the operation fails with error message show above, which means automatic load balancing has been enabled in the current database so manual load balance can't be performed.

Shutdown the cluster, configure `balance` parameter in all the dnodes to 0, then restart the cluster, and execute `alter dnode` and `show vgroups` as below.

```
taos> alter dnode 3 balance "vnode:18-dnode:1";
Query OK, 0 row(s) in set (0.000575s)

taos> show vgroups;
    vgId     |   tables    |  status  |   onlines   | v1_dnode | v1_status | v2_dnode | v2_status | compacting  |
=================================================================================================================
          14 |       38000 | ready    |           1 |        3 | master    |        0 | NULL      |           0 |
          15 |       38000 | ready    |           1 |        3 | master    |        0 | NULL      |           0 |
          16 |       38000 | ready    |           1 |        3 | master    |        0 | NULL      |           0 |
          17 |       38000 | ready    |           1 |        3 | master    |        0 | NULL      |           0 |
          18 |       37001 | ready    |           2 |        1 | slave     |        3 | master    |           0 |
          19 |       37000 | ready    |           1 |        1 | master    |        0 | NULL      |           0 |
          20 |       37000 | ready    |           1 |        1 | master    |        0 | NULL      |           0 |
          21 |       37000 | ready    |           1 |        1 | master    |        0 | NULL      |           0 |
Query OK, 8 row(s) in set (0.001242s)
```

It can be seen from above output that vgId 18 has been moved from dndoe 3 to dnode 1.

:::note

- Manual load balancing can only be performed when the automatic load balancing is disabled, i.e. `balance` is set to 0 只.
- Only vnode in normal state, i.e. master or slave, can be moved. vnode can't moved when its in status offline, unsynced or syncing.
- Before moving a vnode, it's necessary to make sure the target dnode has enough resources: CPU, memory and disk.

:::
