---
title: Three-Replica Configuration
sidebar_label: Three-Replica
toc_max_heading_level: 4
---

TDengine TSDB in three-replica configuration uses the Raft algorithm to ensure consistency for both metadata and time-series data. Each vgroup is a Raft group, and the vnodes within the group are the members of that Raft group, also referred to as replicas.

1. Each vnode has a role: leader, follower, or candidate.
1. Each vnode maintains a continuous log that records all operations such as inserts, updates, or deletes. The log is composed of an ordered sequence of entries, each with a unique identifier to track consensus progress and execution.
1. The leader vnode provides read and write services, ensuring high availability as long as a majority of nodes remain operational. Even in cases of node restarts or leader re-elections, the Raft algorithm guarantees that the new leader can always serve all data that has been successfully written.
1. Every change request to the database (e.g., a data insert) corresponds to a log entry. During continuous writes, Raft ensures that identical log entries are created on all member nodes in the same order, and the corresponding changes are applied consistently. These logs are stored as write-ahead log (WAL) files in the data directory.
1. A log entry is considered safe only after it has been appended to the WAL file on a majority of nodes and acknowledgments have been received. At this point, the entry transitions to the committed state, and the data change is finalized. Once applied, the log entry is marked as applied.

For details on how multi-replica mechanisms work, see [Data Writing and Replication Process](../../26-tdinternal/01-arch.md#data-writing-and-replication-process).

## Cluster Configuration

The three-replica architecture requires at least three server nodes. The basic deployment and configuration steps are as follows:

1. Determine the number of server nodes and their hostnames or domain names, then configure DNS or /etc/hosts for proper name resolution.
1. Install the TDengine TSDB server package on each node and edit the taos.cfg file on each node as needed.
1. Start the taosd service on each node. Other services (such as taosadapter, taosx, taoskeeper, or taos-explorer) can be started as required.

## Maintenance Commands

### Create a Cluster

Create two additional dnodes in your cluster for a total of three dnodes:

```sql
CREATE dnode <dnode_ep> port <dnode_port>;
CREATE dnode <dnode_ep> port <dnode_port>;
```

Create mnodes on the two dnodes for a total of three mnodes:

```sql
CREATE mnode on dnode <dnode_id>;
CREATE mnode on dnode <dnode_id>;
```

### Create a Three-Replica Database

Create a three-replica database:

```sql
create database <dbname> replica 3 vgroups xx buffer xx ...
```

### Modify an Existing Database

If you have already created a database with fewer than three replicas, you can change it into a three-replica database:

```sql
alter database <dbname> replica 3|1
```

## Frequently Asked Questions

### 1. Creating or modifying a database to use three replicas causes `DB error: Out of dnodes`

- Cause: There are fewer than three server nodes in the cluster.
- Solution: Ensure that there are at least three dnodes before creating a three-replica database.

### 2. Creating a three-replica database or running SPLIT VGROUP causes `DB error: Vnodes exhausted`

- Cause: Some dnodes have fewer available vnodes than required for database creation or vgroup splitting.
- Solution: Increase the number of CPU cores on the dnodes or modify the SupportVnodes parameter.
