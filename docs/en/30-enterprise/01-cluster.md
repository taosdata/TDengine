---
toc_max_heading_level: 4
title: Cluster Management
sidebar_label: Cluster Management
---

## Introduction

This article describes how to manage clusters in TDengine Enterprise to ensure that your deployment is robust and operates efficiently.

## Data Defragmentation

In some scenarios, data stored in TDengine may become fragmented or take up an excessive amount of space on disk. This can have an impact on the storage and query efficiency of the database. You can use the `COMPACT` command to defragment the data in your TDengine database and remove deleted or invalid data.

### Syntax

```SQL
COMPACT DATABASE db_name [start with 'XXXX'] [end with 'YYYY']； 
```

### Results

- All data files on the vnodes in the vgroups associated with the specified database are scanned and compressed.
- Deleted data and data from deleted tables is completely removed.
- STT files are combined.
- You can use the `START WITH` keyword to specify a start time for the `COMPACT` command.
- You can use the `END WITH` keyword to specify an end time for the `COMPACT` command.


### Additional Notes

- The `COMPACT` command runs asynchronously and returns before the command has finished. If you run the `COMPACT` command while a previous `COMPACT` operation is still in progress, the new `COMPACT` operation waits until the previous `COMPACT` operation has finished.
- The `COMPACT` command blocks data ingestion but does not block data querying.
- The progress of the `COMPACT` command is not displayed.

## Raft Leader Balancing

If one or more nodes in a multi-replica cluster are restarted due to an upgrade or another reason, load on the cluster may become unevenly distributed among the dnodes in the cluster. In extreme cases, it is possible that a single dnode becomes the leader node of all vgroups in the cluster. You can run the following command to rebalance your cluster:

```SQL
balance vgroup leader;
```

### Features

The command distributes all vgroup leader nodes evenly across replicas. It implements this by forcing vgroups to reelect their leaders.

### Notes

There is an element of randomness in all Raft elections. For this reason, it is possible that load is not completely evenly distributed even after this command has been run. This command affects ingestion and query operations. Data cannot be written to or queried from a vgroup while it is reelecting its Raft leader. The reelection process generally is completed in a number of seconds. Each vgroup in the cluster will go through the reelection process one by one.

## Restoring Dnodes

If the data on a dnode has been lost or corrupted, for example due to a hard disk failure or accidental deletion, you can use the `RESTORE DNODE` command to restore some or all logical nodes. This command can be used only in clusters with three or more dnodes and three replicas.

```sql
restore dnode <dnode_id> # Restore all mnodes, vnodes, and qnodes on the specified dnode
restore mnode on dnode <dnode_id> # Restore the mnode on the specified dnode
restore vnode on dnode <dnode_id> # Restore all vnodes on the specified dnode
restore qnode on dnode <dnode_id> # Restore all qnodes on the specified dnode
```

### Limitations

- This command restores data by copying it from other replicas. It is not intended as a disaster recovery or backup tool. For this command to succeed, it is necessary that the other two replicas in the cluster operate normally.
- This command cannot restore specific files or directories on a node. It is not possible to specify files on an mnode or vnode that may have become corrupted and restore them from a replica. Instead, you can delete all data on the affected mnode or vnode and use this command to restore it.

## Split Vgroups

If a vgroup contains an excessive number of subtables, its CPU or disk usage may become too high. In this situation, you can add additional dnodes to your cluster and then use the `SPLIT VGROUP` command to split your overloaded vgroup into two vgroups. After a vgroup has been split, the two newly created vgroups provide the read and write services for its subtables.

```sql
split vgroup <vgroup_id>
```

### Notes

- If you split the vgroup for a single-replica database, the disk usage of its historical data may double. For this reason, you must add a dnode to your cluster and ensure that CPU and disk resources are sufficient before splitting a vgroup.
- This command is a database-level transaction. While the command is running, all management transactions on the database stored on the specified vgroup are rejected. The other databases in the cluster are not affected.
- While the command is running, the database stored on the specified vgroup can continue to ingest and query data. However, there may be a short service interruption.
- While the command is running, stream processing and data subscription tasks cannot be run. Splitting a vgroup erases its historical WAL.
- While the command is running, the cluster can recover from node restarts but not from hard disk failures.

## Hot Update of Cluster Configuration

You can configure the `supportVnodes` parameter on the fly to specify the maximum number of vnodes on each dnode in your cluster. This parameter is originally set in the `taos.cfg` file. When you create a database, TDengine allocates a new vnode to the database. When you delete a database, TDengine deletes the vnode allocated to that database.

Note that changing the value of the `supportVnodes` parameter on the fly is not persistent. If your cluster is restarted, the maximum number of vnodes on each dnode in the cluster is reset to the value specified in the `taos.cfg` file. 

Setting the value of the `supportVnodes` parameter lower than the number of vnodes currently running on your dnodes will not delete existing vnodes. However, more vnodes cannot be created, which may cause database creation to fail.