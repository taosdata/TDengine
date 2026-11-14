---
title: Active-Active Configuration
sidebar_label: Active-Active
toc_max_heading_level: 4
---

import { Enterprise } from '../../resources/_resources.mdx';

<Enterprise/>

You can deploy TDengine in active-active mode to achieve high availability and reliability with limited resources. Active-active mode also supports disaster recovery by maintaining offsite replicas of the database. It uses data replication and client failover. This document covers the architecture, configuration, and maintenance of an active-active deployment.

Active-active mode is available only in TDengine TSDB Enterprise and was first released in v3.3.0.0. From v3.4.3.12, active-active tasks are managed through SQL. The management cluster's mnode stores task records, xnoded schedules the tasks, and taosX executes them on xnodes. The `taosx replica` command line used in v3.3.x was removed in v3.4.3.12. We recommend using the latest version.

An active-active system consists of exactly two servers, each running a separate TDengine deployment. From the perspective of the business layer, the two servers and their services form a single complete system, and the internal details are transparent. The two nodes in an active-active deployment are often referred to as Master-Slave, meaning "primary" and "secondary"; these terms may be used interchangeably in this document.

The deployment architecture of the TDengine active-active system is shown below. It involves three key mechanisms:

1. The client driver implements failover between the two systems, switching from the primary node to the secondary node when the primary node is down.
1. taosX replicates data from the (current) primary node to the secondary node; active-active tasks are scheduled by xnoded.
1. When replicated data is written, the data subscription write interface adds a special marker to the WAL; the data subscription read interface automatically filters out data carrying this marker, preventing infinite replication loops.

![Active-Active architecture](../../../assets/dual-01.webp)

Note that this diagram uses a single-node TDengine cluster as an example. In practice, each host in the diagram can represent a TDengine cluster of any size.

## Cluster Configuration

The following prerequisites must be met before you deploy an active-active system:

- Version: The management cluster on which you run `CREATE XNODE TASK` must be upgraded to v3.4.3.12 or later. Both the source and target clusters must run v3.4.0.0 or later, and their versions can differ. taosX must use the same version as taosd.
- Database: The source and target databases must be created in advance with the same precision, and the source database must have `wal_retention_period` greater than 0. Active-active tasks are created per database; newly created databases are not replicated automatically.
- License: Both the source and target clusters must have the `active_active` license (except for cloud service instances), and the target cluster must also have the `td3.0` license.
- Deployment: Deploy taosX for the management cluster and register an xnode that can connect to both clusters. For registration instructions, see [Data Ingestion (Xnode)](../../../05-tdengine-sql/08-cluster-management/02-xnode.md). We recommend using the source cluster as the management cluster and connecting to the source as the root user to manage subscription resources and WAL retention markers.

The WAL retention period affects the fault tolerance of an active-active deployment. Data loss will occur if the secondary node remains unreachable longer than the configured WAL retention period. Even shorter downtime can result in data loss, depending on how close it is to the retention period and how quickly data is synchronized. For more information, see [Troubleshooting](#troubleshooting).

## Client Configuration

Active-active mode is supported only by the Java client library in WebSocket connection mode. The following is an example configuration:

```java
url = "jdbc:TAOS-WS://" + host + ":6041/?user=root&password=taosdata";
Properties properties = new Properties();
properties.setProperty(TSDBDriver.PROPERTY_KEY_SLAVE_CLUSTER_HOST, "192.168.1.11");
properties.setProperty(TSDBDriver.PROPERTY_KEY_SLAVE_CLUSTER_PORT, "6041");
properties.setProperty(TSDBDriver.PROPERTY_KEY_ENABLE_AUTO_RECONNECT, "true");
properties.setProperty(TSDBDriver.PROPERTY_KEY_RECONNECT_INTERVAL_MS, "2000");
properties.setProperty(TSDBDriver.PROPERTY_KEY_RECONNECT_RETRY_COUNT, "3");
properties.setProperty(TSDBDriver.PROPERTY_KEY_VARCHAR_AS_STRING, "true");
connection = DriverManager.getConnection(url, properties);
```

These parameters are described as follows:

| Property                              | Description |
| ---------------------------------- | --- |
| PROPERTY_KEY_SLAVE_CLUSTER_HOST    | Enter the hostname or IP address of the secondary node. |
| PROPERTY_KEY_SLAVE_CLUSTER_PORT    | Enter the port number of the secondary node.  |
| PROPERTY_KEY_ENABLE_AUTO_RECONNECT | Specify whether to enable automatic reconnection. For active-active mode, set the value of this parameter to true. |
| PROPERTY_KEY_RECONNECT_INTERVAL_MS | Enter the interval in milliseconds at which reconnection is attempted. The default value is 2000. You can enter 0 to attempt to reconnect immediately. There is no maximum limit. |
| PROPERTY_KEY_RECONNECT_RETRY_COUNT | Enter the maximum number of retries per node. The default value is 3. There is no maximum limit. |

## Limitations

1. You cannot use the data subscription APIs when active-active mode is enabled; creating a consumer fails if active-active parameters are configured.
1. We do not recommend using the parameter binding interface for writes or queries. If you use it, your application must handle related objects becoming invalid after a connection switch.
1. In an active-active deployment, we do not recommend explicitly running `USE <database>` in your application. Specify the database in the connection parameters instead.
1. When the same client connection fails over between the two clusters, the database names and configurations, as well as the usernames, passwords, and permissions used by that client, must be consistent on both sides. The active-active task itself can map the source database to a target database with a different name.
1. The client failover described above supports only WebSocket connection mode; active-active tasks support both Native and WebSocket connections.
1. Each active-active task replicates one database; newly created databases are not synchronized automatically. Each task has its own topic, and the number of topics in a cluster is limited by `tmqMaxTopicNum`.
1. Replication is at-least-once: the last batch of data may be replayed after a failure recovery. Writes with the same timestamp overwrite each other and metadata operations are replayed idempotently, so no duplicate rows are produced. In a bidirectional deployment, concurrent writes of different values to the same table at the same timestamp on both sides may diverge permanently without converging; assign the write ownership of each table (or primary key range) to a single cluster.

## Maintenance

### Task Management

Use SQL to create, view, start, stop, modify, and drop active-active tasks. For the complete syntax, parameter rules, and deletion behavior, see [Data Ingestion (Xnode)](../../../05-tdengine-sql/08-cluster-management/02-xnode.md#active-active-tasks).

Create a task with `CREATE XNODE TASK ... WITH TYPE 'replica'`. Each task replicates one database from the source cluster to one database in the target cluster. For bidirectional replication, create a separate task on each side. The following example replicates the `power` database in both directions between td1 and td2:

```sql
-- Run on td1: td1 -> td2
CREATE XNODE TASK 'aa_power_td1_td2'
  FROM 'tmq+ws://root:taosdata@td1:6041/power'
  TO 'taos+ws://root:taosdata@td2:6041/power'
  WITH TYPE 'replica';
START XNODE TASK 'aa_power_td1_td2';

-- Run on td2: td2 -> td1
CREATE XNODE TASK 'aa_power_td2_td1'
  FROM 'tmq+ws://root:taosdata@td2:6041/power'
  TO 'taos+ws://root:taosdata@td1:6041/power'
  WITH TYPE 'replica';
START XNODE TASK 'aa_power_td2_td1';
```

A newly created task is stopped; run `START XNODE TASK` to start it. When creating a task, you can set subscription start parameters in `FROM`, such as `auto.offset.reset`, `experimental.snapshot.enable`, and `msg.consume.start.ts`, to control where replication starts. These parameters cannot be changed later. To change them, drop and recreate the task. The new task does not inherit the old task's progress.

Unlike normal tasks, active-active tasks are deleted asynchronously. A successful `DROP XNODE TASK` only means the deletion request has been accepted. The task first enters the `deleting` state and disappears from `SHOW XNODE TASKS` only after the source topic, consumer group, and positions have been cleaned up. If cleanup hits a permanent error, such as a permission or credential failure, the task enters the `delete_failed` state; fix the problem and run `DROP XNODE TASK` again. A task with the same mapping cannot be recreated until cleanup completes.

### View Synchronization Progress

First, query the cluster ID on the management cluster. Then use the system-generated topic and consumer group names to query each vgroup's consumption position on the source cluster:

```sql
-- Run on the management cluster (ins_cluster is a privileged table and requires SYSINFO)
SELECT id FROM information_schema.ins_cluster;

-- Run on the source cluster, replacing <management_cluster_id> and <task_id> with actual values
SELECT topic_name, vgroup_id, consumer_id, `offset`, `rows`
  FROM information_schema.ins_subscriptions
  WHERE topic_name = '__replica__<management_cluster_id>_<task_id>'
    AND consumer_group = '__replica__<management_cluster_id>_<task_id>';
```

- During snapshot replication, `offset` starts with `tsdb:`; after entering WAL replication it is displayed as `wal:<consumed version>/<latest version>`. The difference between the two versions is the WAL version lag of the vgroup, which approximates the backlog and has the same meaning as `taosx replica diff` in v3.3. This position is reported by the consumer heartbeat and can be ahead of the committed offset.
- `rows` is the number of rows consumed cumulatively by the current consumer process. It restarts from zero when the task restarts and does not represent the number of rows written to the target successfully.
- An empty `consumer_id` means that no consumer is currently assigned to the vgroup, for example when the task is stopped.

## Troubleshooting

After an active-active task stops or the target cluster fails, replication may take longer than the source database's WAL retention period (`wal_retention_period`) to resume and catch up. In that case, the corresponding WAL may have been deleted, leaving unreplicated data missing from the target. The existing task cannot automatically recover this data. Compare the data in both clusters to identify the affected tables and time ranges. Catching up to the latest consumption position does not prove that the data in the two clusters is consistent.

If the source still retains the required historical data, delete the active-active task for the affected replication direction, wait for cleanup to complete, and then recreate and start it. When recreating the task, enable historical data snapshots (`auto.offset.reset=earliest` and `experimental.snapshot.enable=true`) and set the replication start time through `msg.consume.start.ts`. Choose a time no later than the timestamp of the earliest missing data so that it can be replicated again. Subscription start parameters can be set only when the task is created. For the parameter rules, see [Data Ingestion (Xnode)](../../../05-tdengine-sql/08-cluster-management/02-xnode.md#active-active-tasks).

## Upgrade and Migration

### Version Upgrade and Downgrade

- Do not create active-active tasks during a rolling upgrade: before all taosd nodes are upgraded, an mnode of an earlier version can silently ignore the `TYPE` option and create the task as a standard task, losing loop prevention. After creation, verify through `SHOW XNODE TASKS` that the `type` column is `replica`. Upgrade taosX together with taosd.
- Before downgrading to a version earlier than v3.4.3.12, you must delete all active-active tasks: an mnode of an earlier version does not recognize the task type, can run active-active tasks as standard tasks, and permanently loses the type marker.

### Migrate from v3.3

Active-active tasks from v3.3 are not migrated automatically. Recreate them after upgrading and use `msg.consume.start.ts` to specify the new synchronization start time. New tasks do not reuse the old tasks' topics, consumer groups, or committed offsets. The `taosx replica` command line was removed in v3.4.3.12, so you must stop old tasks before upgrading. Perform the following steps for each replication direction:

1. Before upgrading, determine the start time from the old task's verified synchronization progress and record it as a Unix timestamp in milliseconds. It must be no later than the timestamp of the earliest data that may not have been synchronized. If you cannot confirm the progress, you can choose `0` to replay all data that remains readable. Do not use the upgrade completion time as the start time.
1. Increase the `wal_retention_period` of the source databases on both sides so that it covers the total duration of the shutdown, upgrade, and subsequent catch-up.
1. On the old version, run `taosx replica stop <id>` for each direction and wait for the tasks to stop. Record the database mappings, connection addresses, credentials, and tuning parameters of the old tasks, and then stop the old taosX services so that they do not automatically create tasks for new databases.
1. Complete the upgrade according to the release notes, confirm that both sides meet the version, database, and license requirements, and deploy and register xnodes.
1. Create and start a new task for each mapping, specifying the start timestamp determined in step 1 through `msg.consume.start.ts` in `FROM`.
1. Use `SHOW XNODE TASKS` to verify that the `type` of each new task is `replica`. Use the progress queries in [Maintenance](#maintenance) to confirm that the tasks use subscription identifiers in the form `__replica__<management_cluster_id>_<task_id>`. After replication has caught up in all directions, restore the original WAL retention period. Once you have confirmed that all old tasks are stopped and no other applications use the old topics, clean up the old resources separately following the v3.3 maintenance procedure.

The following is an example of the creation statement in step 5:

```sql
CREATE XNODE TASK 'aa_power_td1_td2'
  FROM 'tmq+ws://root:taosdata@td1:6041/power?msg.consume.start.ts=1700000000000'
  TO 'taos+ws://root:taosdata@td2:6041/power'
  WITH TYPE 'replica';
START XNODE TASK 'aa_power_td1_td2';
```

Data that has already been replicated by the old task at or after the start timestamp may be replicated again; timestamp overwriting and idempotent metadata replay prevent duplicate rows. Active-active tasks exported from v3.3 cannot be restored through the standard task import path of Explorer; recreate them with SQL as described above.
