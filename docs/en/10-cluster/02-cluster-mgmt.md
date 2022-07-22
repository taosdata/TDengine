---
sidebar_label: Operation
title: Manage DNODEs
---

The previous section, [Deployment],(/cluster/deploy) showed you how to deploy and start a cluster from scratch. Once a cluster is ready, the status of dnode(s) in the cluster can be shown at any time. Dnodes can be managed from the TDengine CLI. New dnode(s) can be added to scale out the cluster, an existing dnode can be removed and you can even perform load balancing manually, if necessary.

:::note
All the commands introduced in this chapter must be run in the TDengine CLI - `taos`. Note that sometimes it is necessary to use root privilege.

:::

## Show DNODEs

The below command can be executed in TDengine CLI `taos` to list all dnodes in the cluster, including ID, end point (fqdn:port), status (ready, offline), number of vnodes, number of free vnodes and so on. We recommend executing this command after adding or removing a dnode.

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

To utilize system resources efficiently and provide scalability, data sharding is required. The data of each database is divided into multiple shards and stored in multiple vnodes. These vnodes may be located on different dnodes. One way of scaling out is to add more vnodes on dnodes. Each vnode can only be used for a single DB, but one DB can have multiple vnodes. The allocation of vnode is scheduled automatically by mnode based on system resources of the dnodes.

Launch TDengine CLI `taos` and execute below command:

```sql
USE SOME_DATABASE;
SHOW VGROUPS;
```

The output is like below:

taos> use db;
Database changed.

taos> show vgroups;
vgId | tables | status | onlines | v1_dnode | v1_status | compacting |
==========================================================================================
14 | 38000 | ready | 1 | 1 | leader | 0 |
15 | 38000 | ready | 1 | 1 | leader | 0 |
16 | 38000 | ready | 1 | 1 | leader | 0 |
17 | 38000 | ready | 1 | 1 | leader | 0 |
18 | 37001 | ready | 1 | 1 | leader | 0 |
19 | 37000 | ready | 1 | 1 | leader | 0 |
20 | 37000 | ready | 1 | 1 | leader | 0 |
21 | 37000 | ready | 1 | 1 | leader | 0 |
Query OK, 8 row(s) in set (0.001154s)

````

## Add DNODE

Launch TDengine CLI `taos` and execute the command below to add the end point of a new dnode into the EPI (end point) list of the cluster. "fqdn:port" must be quoted using double quotes.

```sql
CREATE DNODE "fqdn:port";
````

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

It can be seen that the status of the new dnode is "offline". Once the dnode is started and connects to the firstEp of the cluster, you can execute the command again and get the example output below. As can be seen, both dnodes are in "ready" status.

```
taos> show dnodes;
   id   |           end_point            | vnodes | cores  |   status   | role  |       create_time       |      offline reason      |
======================================================================================================================================
      1 | localhost:6030                 |      3 |      8 | ready      | any   | 2022-04-15 08:27:09.359 |                          |
      2 | localhost:7030                 |      6 |      8 | ready      | any   | 2022-04-19 08:14:59.165 |                          |
Query OK, 2 row(s) in set (0.001316s)
```

## Drop DNODE

Launch TDengine CLI `taos` and execute the command below to drop or remove a dnode from the cluster. In the command, you can get `dnodeId` from `show dnodes`.

```sql
DROP DNODE "fqdn:port";
```

or

```sql
DROP DNODE dnodeId;
```

The example output is below：

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

In the above example, when `show dnodes` is executed the first time, two dnodes are shown. After `drop dnode 2` is executed, you can execute `show dnodes` again and it can be seen that only the dnode with ID 1 is still in the cluster.

:::note

- Once a dnode is dropped, it can't rejoin the cluster. To rejoin, the dnode needs to deployed again after cleaning up the data directory. Before dropping a dnode, the data belonging to the dnode MUST be migrated/backed up according to your data retention, data security or other SOPs.
- Please note that `drop dnode` is different from stopping `taosd` process. `drop dnode` just removes the dnode out of TDengine cluster. Only after a dnode is dropped, can the corresponding `taosd` process be stopped.
- Once a dnode is dropped, other dnodes in the cluster will be notified of the drop and will not accept the request from the dropped dnode.
- dnodeID is allocated automatically and can't be manually modified. dnodeID is generated in ascending order without duplication.

:::
