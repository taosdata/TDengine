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
- The start time of COMPACT data can be specified with the start with keyword.
- The end time of the COMPACT data can be specified with the end with keyword.


### Notes

- COMPACT is asynchronous and returns after executing the COMPACT command without waiting for COMPACT to finish. If another COMPACT task is initiated if the previous COMPACT did not complete, it will wait for the previous task to complete before returning.
- COMPACT may block writes, but does not block queries.
- COMPACT's progress is not observable

## Raft Leader Balancing

When one or more nodes in a multicopy cluster are restarted due to upgrades or other reasons, it is possible that the load on each dnode in the cluster is not balanced, and in extreme cases, the leader of all vgroups may be on the same dnode. To solve this problem, you can use the following command

```SQL
balance vgroup leader;
```

## Features

Let the leader of all vgroups be evenly distributed across their respective replica nodes. This command causes the vgroup to force a reelection, and by doing so, transforms the leaders of the vgroup during the election process, and by doing so, ends up with an even distribution of leaders.

### Notes

Raft elections inherently carry randomness, so the uniform distribution produced by the redistribution of the election also carries some probability that it will not be perfectly uniform. The side effect of this command is that it affects queries and writes. When a vgroup is reelected, it cannot be written to or queried by the vgroup from the start of the election until a new leader is elected. The election process is typically completed in seconds. All vgroups will be re-elected one by one in turn.

## Restore a Node

When all data on a data node (dnode) in the cluster is lost or corrupted, such as disk corruption or directory deletion by mistake, you can restore some or all of the logical nodes on the data node by using the restore dnode command, which relies on other replicas in the multicopy to replicate the data, and therefore only works if the number of dnodes in the cluster is greater than or equal to 3 and the number of replicas is 3.

```sql
restore dnode <dnode_id> # Restore all mnodes, vnodes, and qnodes on the specified dnode
restore mnode on dnode <dnode_id> # Restore the mnode on the specified dnode
restore vnode on dnode <dnode_id>  # Restore all vnodes on the specified dnode
restore qnode on dnode <dnode_id> # Restore all qnodes on the specified dnode
```

### Restrictions

- This function is based on the recovery of the existing replication function, not disaster recovery or backup recovery, so for the mnode and vnode to be recovered, the use of this command assumes that there still exists the other two copies of the mnode or vnode that still work properly.
- This command cannot repair damage or loss of individual files in the data directory. For example, if individual files or data in an mnode or vnode are corrupted, it is not possible to recover the corrupted file or chunk of data individually. At this point, you can choose to clear all the data of the mnode/vnode and then restore it.

## Split Vgroups

When a vgroup is overloaded in terms of CPU or Disk resource usage due to an excessive number of sub-tables, the vgroup can be split into two virtual groups by using the split vgroup command after adding dnode nodes. After the split is complete, the two newly created vgroups assume the read and write services originally provided by one vgroup.

```sql
split vgroup <vgroup_id>
```

### Notes

- The single replica library virtual group, after the split is complete, the total disk space usage for historical time-series data, may double. Therefore, before performing this operation, make sure that there are enough CPU and disk resources in the cluster by adding dnode nodes to avoid under-resourcing.
- This command is a DB-level transaction; during execution, other management transactions for the current DB will be rejected. other DBs in the cluster are not affected.
- Read and write services are available continuously during split task execution; during this time, there may be perceptible brief interruptions in read and write operations.
- Streams and subscriptions are not supported during splits. The historical WAL is cleared when the split ends.
- During the splitting process, node downtime restart fault tolerance can be supported; however, node disk failure fault tolerance is not supported.

## Hot Update of Cluster Settings

As of version 3.1.1.0, TDengine Enterprise supports online hot updating of `supportVnodes`, a very important dnode configuration parameter. This parameter was originally configured in the `taos.cfg` configuration file to indicate the maximum number of vnodes that the dnode can support. New vnodes are allocated when a database is created, and vnodes are destroyed when a database is deleted.

However, updating `supportVnodes` online does not result in persistence, and when the system is rebooted, the maximum number of vnodes allowed is still determined by the `supportVnodes` configured in taos.cfg.

If the number of `supportVnodes` set by online update or configuration file is less than the number of vnodes already physically present in the dnode, the existing vnodes will not be affected. However, when attempting to create a new database, the success of the creation is still determined by the actual `supportVnodes` parameter in effect.