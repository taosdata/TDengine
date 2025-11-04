---
title: Maintaining Your Cluster
slug: /operations-and-maintenance/maintain-your-cluster
---

This section introduces advanced cluster maintenance methods provided in TDengine Enterprise, which can make the TDengine cluster run more robustly and efficiently over the long term.

## Node Management

For how to manage cluster nodes, please refer to [Node Management](../../tdengine-reference/sql-manual/manage-nodes/)

## Data Reorganization

TDengine is designed for various writing scenarios, and many of these scenarios can lead to data storage amplification or data file holes in TDengine's storage. This not only affects the efficiency of data storage but also impacts query performance. To address these issues, TDengine Enterprise offers a data reorganization feature, namely the DATA COMPACT function, which reorganizes stored data files, removes file holes and invalid data, improves data organization, and thus enhances storage and query efficiency. The data reorganization feature was first released in version 3.0.3.0 and has since undergone several iterations of optimization. It is recommended to use the latest version.

### Syntax

```sql
COMPACT DATABASE db_name [start with 'XXXX'] [end with 'YYYY'] [META_ONLY] [FORCE];
COMPACT [db_name.]VGROUPS IN (vgroup_id1, vgroup_id2, ...) [start with 'XXXX'] [end with 'YYYY'] [META_ONLY] [FORCE];
SHOW COMPACTS;
SHOW COMPACT compact_id;
KILL COMPACT compact_id;
```

### Effects

- Scans and compresses all data files in all VGROUP VNODEs of the specified DB
- COMPACT will delete data that has been deleted and data from deleted tables
- COMPACT will merge multiple STT files
- You can specify the start time of the COMPACT data with the start with keyword
- You can specify the end time of the COMPACT data with the end with keyword
- You can specify the META_ONLY keyword to only compact the meta data which are not compacted by default. Meta data compaction can block write and the database compacting meta should stop write and query
- A file group will not be compacted if no new data has been written since the last compaction, unless the FORCE keyword is specified  
- The COMPACT command will return the ID of the COMPACT task
- COMPACT tasks are executed asynchronously in the background, and you can view the progress of COMPACT tasks using the SHOW COMPACTS command
- The SHOW command will return the ID of the COMPACT task, and you can terminate the COMPACT task using the KILL COMPACT command

### Additional Information

- COMPACT is asynchronous; after executing the COMPACT command, it returns without waiting for the COMPACT to finish. If a previous COMPACT has not completed, it will wait for the previous task to finish before returning.
- COMPACT may block writing, especially in databases where stt_trigger = 1, but it does not block queries.

## Scanning Data

```sql
SCAN DATABASE db_name [start with 'XXXX'] [end with 'YYYY'];
SCAN [db_name.]VGROUPS IN (vgroup_id1, vgroup_id2, ...) [start with 'XXXX'] [end with 'YYYY'];
SHOW SCANS;
SHOW SCAN <scan_id>;
KILL SCAN <scan_id>;
```
### Effects
- Scans all time-series data files of all VGROUP VNODEs in the specified DB. If there are issues with the data files, they will be output in the corresponding server logs.
- Scans all time-series data files of all VGROUP VNODEs in the specified list of VGROUPS in the DB. If db_name is empty, the current database is used by default. If there are issues with the data files, they will be output in the corresponding server logs.
- You can specify the start time of the SCAN data with the start with keyword
- You can specify the end time of the SCAN data with the end with keyword
- SCAN tasks are executed asynchronously in the background, and you can view the list of SCAN tasks using the SHOW SCANS command
- The SHOW command will return the ID of the SCAN task, and you can terminate the SCAN task using the KILL SCAN command

### Additional Information
- SCAN is asynchronous; after executing the SCAN command, it returns without waiting for the SCAN to finish. If a previous SCAN has not completed, it will wait for the previous task to finish before returning.

## Vgroup Leader Rebalancing

When one or more nodes in a multi-replica cluster restart due to upgrades or other reasons, it may lead to an imbalance in the load among the various dnodes in the cluster. In extreme cases, all vgroup leaders may be located on the same dnode. To solve this problem, you can use the following commands, which were first released in version 3.0.4.0. It is recommended to use the latest version as much as possible.

```sql
balance vgroup leader; # Rebalance all vgroup leaders
balance vgroup leader on <vgroup_id>; # Rebalance a vgroup leader
balance vgroup leader database <database_name>; # Rebalance all vgroup leaders within a database
```

### Functionality

Attempts to evenly distribute one or all vgroup leaders across their respective replica nodes. This command forces a re-election of the vgroup, changing the vgroup's leader during the election process, thereby eventually achieving an even distribution of leaders.

### Note

Vgroup elections are inherently random, so the even distribution produced by the re-election is also probabilistic and not perfectly even. The side effect of this command is that it affects queries and writing; during the vgroup re-election, from the start of the election to the election of a new leader, this vgroup cannot be written to or queried. The election process generally completes within seconds. All vgroups will be re-elected one by one sequentially.

## Restore Data Node

If the data on a data node (dnode) in the cluster is completely lost or damaged, such as due to disk damage or directory deletion, you can use the restore dnode command to recover some or all logical nodes on that data node. This feature relies on data replication from other replicas in the cluster, so it only works when the number of dnodes in the cluster is three or more and the number of replicas is three.

```sql
restore dnode <dnode_id>; # Restore mnode, all vnodes, and qnode on dnode
restore mnode on dnode <dnode_id>; # Restore mnode on dnode
restore vnode on dnode <dnode_id>; # Restore all vnodes on dnode
restore qnode on dnode <dnode_id>; # Restore qnode on dnode
```

### Limitations

- This feature is based on the recovery of existing replication capabilities, not disaster recovery or backup recovery. Therefore, for the mnode and vnode to be recovered, the prerequisite for using this command is that the other two replicas of the mnode or vnode can still function normally.
- This command cannot repair individual files in the data directory that are damaged or lost. For example, if individual files or data in an mnode or vnode are damaged, it is not possible to recover a specific file or block of data individually. In this case, you can choose to completely clear the data of that mnode/vnode and then perform recovery.

## Splitting Virtual Groups

When a vgroup is overloaded with CPU or Disk resource usage due to too many subtables, after adding a dnode, you can split the vgroup into two virtual groups using the `split vgroup` command. After the split, the newly created two vgroups will undertake the read and write services originally provided by one vgroup. This command was first released in version 3.0.6.0, and it is recommended to use the latest version whenever possible.

```sql
split vgroup <vgroup_id>
```

### Note

- For single-replica vgroups, after the split, the total disk space usage of historical time-series data may double. Therefore, before performing this operation, ensure there are sufficient CPU and disk resources in the cluster by adding dnode nodes to avoid resource shortages.
- This command is a DB-level transaction; during execution, other management transactions of the current DB will be rejected. Other DBs in the cluster are not affected.
- During the split task, read and write services can continue; however, there may be a perceptible brief interruption in read and write operations.
- Streams and subscriptions are not supported during the splitting process. After the split ends, historical WAL will be cleared.
- During the split process, node crash restart fault tolerance is supported; however, node disk failure fault tolerance is not supported.

## Online Cluster Configuration Update

Starting from version 3.1.1.0, TDengine Enterprise supports online hot updates of the very important dnode configuration parameter `supportVnodes`. This parameter, originally configured in the `taos.cfg` file, represents the maximum number of vnodes that the dnode can support. When a database is created, new vnodes are allocated, and when a database is deleted, its vnodes are destroyed.

However, online updates of `supportVnodes` do not persist, and after a system restart, the maximum number of vnodes allowed will still be determined by the `supportVnodes` configured in taos.cfg.

If the `supportVnodes` set through online updates or configuration files is less than the current actual number of vnodes on the dnode, the existing vnodes will not be affected. However, whether a new database can be successfully created will still depend on the actual effective `supportVnodes` parameter.

## Dual Replicas

Dual replicas are a special high-availability configuration for databases. This section provides special instructions for their use and maintenance. This feature was first released in version 3.3.0.0, and it is recommended to use the latest version whenever possible.

### Viewing the Status of Vgroups

Use the following SQL commands to view the status of each Vgroup in a dual-replica database:

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

- 0: Vgroup data has not achieved synchronization. In this state, if one Vnode in the Vgroup is inaccessible, the other Vnode cannot be designated as the `AssignedLeader` role, and the Vgroup will not be able to provide service.
- 1: Vgroup data has achieved synchronization. In this state, if one Vnode in the Vgroup is inaccessible, the other Vnode can be designated as the `AssignedLeader` role, and the Vgroup can continue to provide service.

assigned_dnode:

- Identifies the DnodeId of the Vnode designated as AssignedLeader
- Displays NULL when no AssignedLeader is specified

assigned_token:

- Identifies the Token of the Vnode designated as AssignedLeader
- Displays NULL when no AssignedLeader is specified

### Best Practices

1. New Deployment

The main value of dual replicas lies in saving storage costs while maintaining a certain level of high availability and reliability. In practice, the recommended configuration is:

- N node cluster (where N>=3)
- N-1 dnodes responsible for storing time-series data
- The Nth dnode does not participate in the storage and retrieval of time-series data, i.e., it does not store replicas; this can be achieved by setting the `supportVnodes` parameter to 0
- The dnode that does not store data replicas also has lower CPU/Memory resource usage, allowing the use of lower-specification servers

1. Upgrading from Single Replica

Assuming there is an existing single replica cluster with N nodes (N>=1), and you want to upgrade it to a dual replica cluster, ensure that N>=3 after the upgrade, and configure the `supportVnodes` parameter of a newly added node to 0. After completing the cluster upgrade, use the command `alter database replica 2` to change the replica count for a specific database.
