---
title: Maintain Your Cluster
slug: /operations-and-maintenance/maintain-your-cluster
---

This section introduces the advanced cluster maintenance techniques provided in TDengine Enterprise, which can help TDengine clusters run more robustly and efficiently over the long term.

## Node Management

For managing cluster nodes, please refer to [Manage Nodes](../../tdengine-reference/sql-manual/manage-nodes/).

## Data Compaction

TDengine is designed for various write scenarios, but in many cases, its storage can lead to data amplification or empty spaces in data files. This not only affects storage efficiency but can also impact query performance. To address these issues, TDengine Enterprise provides a data compaction feature, called DATA COMPACT, which reorganizes stored data files, removes empty spaces and invalid data, and improves data organization, thereby enhancing both storage and query efficiency. The data compaction feature was first released in version 3.0.3.0 and has undergone multiple iterations and optimizations since then, so it is recommended to use the latest version.

### Syntax

```SQL
COMPACT DATABASE db_name [start with 'XXXX'] [end with 'YYYY'];
SHOW COMPACTS [compact_id];
KILL COMPACT compact_id;
```

### Effects

- Scans and compresses all data files of all VGROUPs in the specified database.
- COMPACT will delete deleted data and data of deleted tables.
- COMPACT will merge multiple STT files.
- You can specify the start time of the data to be compacted using the `start with` keyword.
- You can specify the end time of the data to be compacted using the `end with` keyword.
- The COMPACT command will return the ID of the COMPACT task.
- The COMPACT task will execute asynchronously in the background; you can check the progress of the COMPACT task using the SHOW COMPACTS command.
- The SHOW command will return the ID of the COMPACT task, and you can terminate the COMPACT task using the KILL COMPACT command.

### Additional Notes

- COMPACT is asynchronous; after executing the COMPACT command, it will return without waiting for the COMPACT to finish. If a previous COMPACT has not completed and another COMPACT task is initiated, it will wait for the previous task to finish before returning.
- COMPACT may block writes, especially in databases with `stt_trigger = 1`, but it does not block queries.

## Vgroup Leader Rebalancing

After one or more nodes in a multi-replica cluster restart due to upgrades or other reasons, there may be an imbalance in the load among the dnodes, and in extreme cases, all the leaders of vgroups may be located on the same dnode. To resolve this issue, you can use the command below, which was first released in version 3.0.4.0. It is recommended to use the latest version whenever possible.

```SQL
balance vgroup leader; # Rebalance the leaders of all vgroups
balance vgroup leader on <vgroup_id>; # Rebalance the leader of a specific vgroup
balance vgroup leader database <database_name>; # Rebalance the leaders of all vgroups in a specific database
```

### Functionality

This command attempts to distribute the leaders of one or all vgroups evenly across their respective replica nodes. It will force the vgroup to re-elect its leader, thereby changing the leader during the election process, which ultimately helps in achieving an even distribution of leaders.

### Notes

Vgroup elections inherently involve randomness, so the even distribution produced by the re-election is also probabilistic and may not be completely uniform. The side effect of this command is that it can affect queries and writes; during the re-election of the vgroup, it will be unable to write or query until the new leader is elected. The election process typically completes within seconds. All vgroups will be re-elected one by one.

## Restoring Data Nodes

If a data node (dnode) in the cluster loses or damages all its data (e.g., due to disk failure or directory deletion), you can restore part or all of the logical nodes on that data node using the `restore dnode` command. This feature relies on data replication from other replicas, so it only works when the number of dnodes in the cluster is greater than or equal to 3, and the number of replicas is 3.

```SQL
restore dnode <dnode_id>; # Restore the mnode, all vnodes, and qnodes on the dnode
restore mnode on dnode <dnode_id>; # Restore the mnode on the dnode
restore vnode on dnode <dnode_id>; # Restore all vnodes on the dnode
restore qnode on dnode <dnode_id>; # Restore the qnode on the dnode
```

### Limitations

- This function is based on the existing replication capability for recovery; it is not a disaster recovery or backup recovery feature. Therefore, for the mnode and vnode that you want to restore, the prerequisite for using this command is that there are still two other functioning replicas of that mnode or vnode.
- This command cannot repair individual file damage or loss in the data directory. For instance, if a particular file or data in a mnode or vnode is damaged, you cannot restore just that damaged file or data block. In this case, you can choose to clear all data from the mnode/vnode and then perform the restore.

## Splitting Virtual Groups

When a vgroup experiences high CPU or disk resource utilization due to an excessive number of subtables, and additional dnodes have been added, you can use the `split vgroup` command to split that vgroup into two virtual groups. After the split, the newly created two vgroups will share the read and write services originally provided by one vgroup. This command was first released in version 3.0.6.0, and it is recommended to use the latest version whenever possible.

```SQL
split vgroup <vgroup_id>
```

### Notes

- For a single-replica virtual group, the total disk space usage of historical time-series data may double after the split is complete. Therefore, before performing this operation, ensure that there are sufficient CPU and disk resources in the cluster by adding dnodes to avoid resource shortages.
- This command is a database-level transaction; during the execution process, other management transactions for the current database will be rejected. Other databases in the cluster will not be affected.
- The split task can continue to provide read and write services during execution; however, there may be a perceptible brief interruption in read and write services during this time.
- Streaming and subscriptions are not supported during the split. After the split, historical WAL will be cleared.
- The split operation supports fault tolerance for node crashes and restarts; however, it does not support fault tolerance for node disk failures.

## Online Updating Cluster Configuration

Starting from version 3.1.1.0, TDengine Enterprise supports online hot updates for the important dnode configuration parameter `supportVnodes`. This parameter was originally configured in the `taos.cfg` configuration file, indicating the maximum number of vnodes that the dnode can support. When creating a database, new vnodes need to be allocated, and when deleting a database, its vnodes will be destroyed.

However, online updates to `supportVnodes` will not persist; after the system restarts, the maximum allowed number of vnodes will still be determined by the `supportVnodes` configured in `taos.cfg`.

If the `supportVnodes` set by online updates or configuration file settings is less than the current actual number of vnodes, the existing vnodes will not be affected. However, whether a new database can be successfully created will still depend on the effective `supportVnodes` parameter.

## Dual Replica

Dual replicas are a special high availability configuration for databases, and this section provides special instructions for their use and maintenance. This feature was first released in version 3.3.0.0, and it is recommended to use the latest version whenever possible.

### Viewing the Status of Vgroups

You can view the status of each vgroup in the dual replica database using the following SQL command:

```SQL
show arbgroups;

select * from information_schema.ins_arbgroups;
            db_name             |  vgroup_id  | v1_dnode | v2_dnode | is_sync | assigned_dnode |         assigned_token         |
=================================================================================================================================
 db                             |           2 |        2 |        3 |       0 | NULL           | NULL                           |
 db                             |           3 |        1 |        2 |       0 |              1 | d1#g3#1714119404630#663        |
 db                             |           4 |        1 |        3 |       1 | NULL           | NULL                           |

```

The `is_sync` column has two possible values:

- 0: Vgroup data is not synchronized. In this state, if a vnode in the vgroup becomes inaccessible, the other vnode cannot be designated as the `AssignedLeader`, and the vgroup will be unable to provide services.
- 1: Vgroup data is synchronized. In this state, if a vnode in the vgroup becomes inaccessible, the other vnode can be designated as the `AssignedLeader`, and the vgroup can continue to provide services.

The `assigned_dnode` column:

- Indicates the DnodeId of the vnode designated as the AssignedLeader.
- When there is no AssignedLeader specified, this column displays NULL.

The `assigned_token` column:

- Indicates the token of the vnode designated as the AssignedLeader.
- When there is no AssignedLeader specified, this column displays NULL.

### Best Practices

1. Fresh Deployment

The main value of dual replicas lies in saving storage costs while ensuring a certain level of high availability and reliability. In practice, it is recommended to configure:

- An N-node cluster (where N >= 3).
- N-1 of these dnodes are responsible for storing time-series data.
- The Nth dnode does not participate in the storage and reading of time-series data, meaning it does not hold replicas; this can be achieved by setting the `supportVnodes` parameter to 0.
- The dnode that does not store data replicas will have lower CPU/memory resource usage, allowing it to run on lower-specification servers.

2. Upgrading from Single Replica

Assuming there is already a single replica cluster with N nodes (N >= 1), you can upgrade it to a dual replica cluster, ensuring that N >= 3 after the upgrade, and that the `supportVnodes` parameter of the newly added node is set to 0. After the cluster upgrade is complete, use the `alter database replica 2` command to change the number of replicas for a specific database.
