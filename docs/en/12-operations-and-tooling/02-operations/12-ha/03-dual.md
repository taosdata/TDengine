---
title: Active-Active Configuration
sidebar_label: Active-Active
toc_max_heading_level: 5
---

import { Enterprise } from '../../resources/_resources.mdx';

<Enterprise/>

Some users can deploy only two servers because of environmental constraints, while also needing higher service availability and data reliability. TDengine active-active mode meets these needs through data replication and client failover. It also supports disaster recovery between two TDengine clusters, whose size is not limited to two servers. This document covers the architecture, configuration, and maintenance of an active-active deployment.

Active-active mode is available only in TDengine TSDB Enterprise and was first released in v3.3.0.0. From v3.4.3.12, active-active tasks are managed through SQL, and the `taosx replica` command line used in v3.3.x has been removed. We recommend using the latest version.

An active-active system consists of two servers, each running a separate TDengine deployment. From the perspective of the business layer, the two servers and their services form a single complete system, and the internal details are transparent. The two nodes in an active-active deployment are often referred to as Master-Slave, meaning "primary" and "secondary"; these terms may be used interchangeably in this document.

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
- Deployment: Deploy taosX for the management cluster and register an xnode that can connect to both clusters. For registration instructions, see [Data Ingestion (Xnode)](../../../05-tdengine-sql/08-cluster-management/02-xnode.md). We recommend using the source cluster as the management cluster and connecting to the source as a superuser to manage subscription resources and WAL retention markers.

The WAL retention period affects the fault tolerance of an active-active deployment. Data loss will occur if the secondary node remains unreachable longer than the configured WAL retention period. Even shorter downtime can result in data loss, depending on how close it is to the retention period and how quickly data is synchronized. For more information, see [Troubleshooting](#troubleshooting).

## Client Configuration

Active-active mode is supported only by the Java client library in WebSocket connection mode. The following is an example configuration:

```java
url = "jdbc:TAOS-WS://" + host + ":6041/?user=root&password=taosdata&varcharAsString=true";
Properties properties = new Properties();
properties.setProperty(TSDBDriver.PROPERTY_KEY_SLAVE_CLUSTER_HOST, "192.168.1.11");
properties.setProperty(TSDBDriver.PROPERTY_KEY_SLAVE_CLUSTER_PORT, "6041");
properties.setProperty(TSDBDriver.PROPERTY_KEY_ENABLE_AUTO_RECONNECT, "true");
properties.setProperty(TSDBDriver.PROPERTY_KEY_RECONNECT_INTERVAL_MS, "2000");
properties.setProperty(TSDBDriver.PROPERTY_KEY_RECONNECT_RETRY_COUNT, "3");
connection = DriverManager.getConnection(url, properties);
```

These parameters are described as follows:

| Property | Description |
| --- | --- |
| `PROPERTY_KEY_SLAVE_CLUSTER_HOST` | Hostname or IP address of the secondary node. Empty by default. |
| `PROPERTY_KEY_SLAVE_CLUSTER_PORT` | Port number of the secondary node. Empty by default. |
| `PROPERTY_KEY_ENABLE_AUTO_RECONNECT` | Whether to enable automatic reconnection. Applies only to WebSocket connections. `true` enables reconnection; `false` disables it. The default is `false`. Set this parameter to `true` for active-active mode. |
| `PROPERTY_KEY_RECONNECT_INTERVAL_MS` | Reconnection interval in milliseconds. The default is `2000` milliseconds (2 seconds). The minimum is `0`, which retries immediately. There is no maximum limit. |
| `PROPERTY_KEY_RECONNECT_RETRY_COUNT` | Maximum number of retries per node. The default is `3`. The minimum is `0`, which disables retries. There is no maximum limit. |

## Limitations

1. You cannot use the data subscription APIs when active-active mode is enabled; creating a consumer fails if active-active parameters are configured.
1. We do not recommend using the parameter binding interface for writes or queries. If you use it, your application must handle related objects becoming invalid after a connection switch.
1. In an active-active deployment, we do not recommend explicitly running `USE <database>` in your application. Specify the database in the connection parameters instead.
1. When the same client connection fails over between the two clusters, the database names and configurations, as well as the usernames, passwords, and permissions used by that client, must be consistent on both sides. The active-active task itself can map the source database to a target database with a different name.
1. The client failover described above supports only WebSocket connection mode; active-active tasks support both Native and WebSocket connections.
1. Each active-active task replicates one database; newly created databases are not synchronized automatically. Each task has its own topic, and the number of topics in a cluster is limited by `tmqMaxTopicNum`.

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

Active-active tasks from v3.3 are not migrated automatically when the software is upgraded. Recreate them through SQL after upgrading. New tasks do not reuse the old tasks' topics, consumer groups, or committed positions.

You can use [manual migration](#manual-migration) to record old configurations and recreate tasks individually, or [script migration](#script-migration) to convert configurations, optionally stop and delete old tasks, and create and start new tasks. Both methods require the preparation below and the same verification after migration. Active-active tasks exported from v3.3 cannot be restored through Explorer's normal-task import feature.

#### Preparation Before Migration

1. Determine the replication start time for each direction based on verified progress, expressed as a Unix millisecond timestamp. Choose a time no later than the timestamp of the earliest record that might not have been synchronized. If progress cannot be confirmed, choose `0` to replay all data that remains readable. Do not simply use the upgrade completion time.
2. Record and increase `wal_retention_period` for the source databases on both sides so that retention covers the total time needed for downtime, upgrading, and catching up. For WAL-only consumption, confirm that data at the selected start time is still retained.
3. Check the task scope, source-to-target database mappings, connection addresses, credentials, and tuning parameters for each old taosX instance. Migration files and backups can contain plaintext credentials and require restricted access.
4. Plan the upgrade according to the release notes and ensure that the upgraded environment meets the version, database, license, and deployment requirements in [Cluster Configuration](#cluster-configuration). A nonzero start time requires all source vnodes to run v3.4.3.6 or later. When using WebSocket connections, the taosAdapter instances used must also run that version or later.

The `taosx replica` command is no longer available from v3.4.3.12. Before upgrading, stop all old replication tasks and their associated monitors for discovering new databases, then stop all separately deployed old taosX services so that old tasks or monitors cannot resume after the upgrade. Online export and old-task operations through the script require the old services to remain available, so complete these operations before stopping the services.

#### Manual Migration

Stop all old tasks and complete the software upgrade, then recreate tasks for each database mapping:

1. On the old version, record the task configurations for all replication directions and run `taosx replica stop <id>` for each task. Wait for all tasks to stop, then stop the old taosX services.
2. Upgrade all taosd nodes, taosX, and the required taosAdapter instances according to the release notes. Prepare the databases and register xnodes, then confirm that the requirements in Preparation Before Migration are met.
3. Write a `CREATE XNODE TASK ... WITH TYPE 'replica'` statement for each source-to-target database mapping. Specify the selected replication start time through `msg.consume.start.ts` in `FROM`. Check the connection information and tuning parameters against the DSN parameter rules in [Data Ingestion (Xnode)](../../../05-tdengine-sql/08-cluster-management/02-xnode.md#active-active-tasks). Do not reuse old topics, consumer groups, client identifiers, or other system-reserved parameters.
4. Create tasks on the selected management cluster and run `SHOW XNODE TASKS WHERE type = 'replica';` to verify their names, mappings, and types. After confirming the configurations, run `START XNODE TASK` for each task.
5. After recreating tasks for every replication direction, complete the checks and cleanup in [Verification After Migration](#verification-after-migration).

The following example recreates the task that replicates the `power` database from td1 to td2. Replace the addresses, credentials, and start time with actual values. Create a separate task for reverse replication:

```sql
CREATE XNODE TASK 'aa_power_td1_td2'
  FROM 'tmq+ws://root:taosdata@td1:6041/power?msg.consume.start.ts=1700000000000'
  TO 'taos+ws://root:taosdata@td2:6041/power'
  WITH TYPE 'replica';

SHOW XNODE TASKS WHERE type = 'replica';

START XNODE TASK 'aa_power_td1_td2';
```

#### Script Migration

The migration script converts active-active task configurations from v3.3.6 into SQL for the new version. By default, it only generates migration files. You can also use it to stop and delete old tasks, or create and start new tasks after upgrading. Software upgrades and data verification after migration remain manual operations.

Expand the code below and save it as `migrate_active_active.py`. Run it with Python 3.8 or later; no third-party dependencies are required. To execute SQL for new tasks, the script invokes the local `taos` client.

<details>
<summary>Migration script</summary>

```python
{{#include docs/examples/taosx/migrate_active_active.py}}
```

</details>

##### Before Upgrading

1. Export old tasks online and generate migration files. The script reads the old taosX REST API directly, on port `6050` by default, so you do not need to export JSON manually. Replace the example start time with the value selected during preparation:

   ```bash
   python3 migrate_active_active.py \
     --old-taosx-url http://td-a:6050 \
     --start-time '2026-11-06T10:00:00+08:00' \
     --output-dir replica-migration-a
   ```

   For different start times per task, repeat `--task-start-time TASK_ID=TIME`, for example `--task-start-time 7=1700000000000 --task-start-time 8=1700000001000`. For start-time inheritance rules, see [Script Parameters](#script-parameters).

   Alternatively, use `--sqlite /backup/taosx.db` to generate files offline from a SQLite backup without connecting to the old service. Use the actual configured database path and create a consistent backup with SQLite backup tools or after stopping the service. Copying a single `.db` file while the service is running can omit WAL contents. Use the following example instead of online export:

   ```bash
   python3 migrate_active_active.py \
     --sqlite /backup/taosx.db \
     --start-time '2026-11-06T10:00:00+08:00' \
     --output-dir replica-migration-a
   ```

2. Inspect `tasks.json`, `create.sql`, and `start.sql` in the migration directory. They contain the old task snapshot, creation statements, and start statements, respectively. Verify the task scope, addresses, database mappings, credentials, and start times. Task IDs can overlap between old taosX instances, so use a separate directory for each instance. The script does not overwrite an existing directory.

   The script preserves database mappings, connection credentials, and allowed tuning parameters, removes system-reserved parameters from old DSNs, and generates new task names. Normal and deleted tasks are excluded from migration. `auto.offset.reset=latest`, `VIA`, custom parsers, and parallel task configurations are not supported. Duplicate mappings or unsupported parameters cause an error and require manual review of the old configurations.

   Do not edit the generated migration files directly. Before executing SQL, the script checks that the SQL files match the task snapshot and stops execution if they differ.

3. After online export, use the same migration directory to stop all active-active tasks on that old instance and their associated monitors for discovering new databases, and wait for them to stop:

   ```bash
   python3 migrate_active_active.py \
     --output-dir replica-migration-a --stop-old --stop-timeout 300
   ```

   Deleting old tasks is optional and can be scheduled according to the rollback window. After confirming that the old tasks have stopped, use `--delete-old` with the same directory to delete all old active-active tasks in the snapshot and their associated database discovery configurations:

   ```bash
   python3 migrate_active_active.py \
     --output-dir replica-migration-a --delete-old
   ```

   The script cannot stop or delete old tasks using an offline-generated snapshot. Complete these operations manually according to the old version's maintenance procedures.

4. After completing these operations for every old instance, stop all separately deployed old taosX services. Then upgrade taosd, taosX, and the required taosAdapter instances according to the release notes, prepare the databases, and register xnodes before proceeding with the post-upgrade operations.

##### After Upgrading

Confirm that the upgraded environment meets the requirements in [Preparation Before Migration](#preparation-before-migration), then process the migration directory generated for each old instance separately.

You can use the script to create and start new tasks in separate steps. `--dsn` connects to the new management cluster and is independent of the source and target connection configurations in the task's `FROM` and `TO` clauses:

```bash
python3 migrate_active_active.py \
  --output-dir replica-migration-a \
  --dsn 'ws://root:password@td-a:6041' --execute create

python3 migrate_active_active.py \
  --output-dir replica-migration-a \
  --dsn 'ws://root:password@td-a:6041' --execute start --start-timeout 300
```

`--execute create` creates tasks and verifies their configurations. `--execute start` verifies their configurations, starts them, and waits for them to enter `running`. You can also use `--execute all` to create and verify all tasks before starting them. The script checks task names, types, connection configurations, and running states.

**Security note:** During migration, the `--dsn` argument and SQL that the script passes to `taos` may expose management, source, and target credentials in the process list or shell history. Run the script only on a controlled administration host, and rotate or revoke any temporary credentials after the migration as needed.

The script cannot execute SQL automatically when `xnodeDsnProtection` is enabled on the management cluster. It also refuses execution if the setting cannot be confirmed or has a pending change that requires a restart. In these cases, an administrator must create and verify tasks manually according to the cluster's protection requirements. Do not disable protection to run the script.

You can also execute the generated `create.sql` and `start.sql` manually on the selected management cluster. Review the SQL first, execute the creation file, verify that every new task has `type='replica'` and the correct configuration, and then execute the start file, for example:

```bash
taos -h <management_cluster_host> -f replica-migration-a/create.sql
```

```sql
SHOW XNODE TASKS WHERE type = 'replica';
```

```bash
taos -h <management_cluster_host> -f replica-migration-a/start.sql
```

After starting all replication directions, check the synchronization results as described in [Verification After Migration](#verification-after-migration).

##### Failure Handling and Retries

If manual SQL execution fails, check the configurations and states of tasks that have already been created or started before retrying the unfinished steps. Avoid resubmitting the entire batch of statements.

Results for old-task operations and new-task execution are saved in `old-operations.json` and `new-operations.json`, respectively. After a failure, preserve the original migration directory, inspect the results, resolve the cause, and retry the failed phase using the same directory.

On CREATE retries, the script skips only tasks with the same name and an exactly matching configuration. On START retries, it skips running tasks and continues waiting for queued tasks. If the CREATE phase of `all` fails, START is not executed. New tasks that have already been created or started are not rolled back automatically.

If only some old tasks have been deleted, retry using the original directory. If tasks are missing for unknown reasons, configurations have changed, or file validation fails, preserve the original snapshot, SQL, and operation records and check the actual task states before proceeding. Do not clear records, replace the snapshot, or edit files to bypass checks.

##### Script Parameters

Each invocation of `migrate_active_active.py` handles one old taosX instance or one new management cluster. Use a separate output directory for each old instance.

| Parameter | Default or requirement | Stage | Description |
| --- | --- | --- | --- |
| `--output-dir DIR` | Required except when displaying help | All | Directory for migration files. Use a directory that does not yet exist when generating files; reuse the original directory for later operations and SQL execution. |
| `--old-taosx-url URL` | `http://127.0.0.1:6050` for generation; saved URL for later operations | Online generation, old-task operations | HTTP or HTTPS address of the old taosX REST API. Later operations must use the address saved in the snapshot. Cannot be combined with `--sqlite` or `--execute`. |
| `--sqlite FILE` | Read online when omitted | Offline generation | Generates migration files from a consistent SQLite backup without connecting to the old service. Cannot be combined with `--old-taosx-url`, old-task operations, or `--execute`. |
| `--start-time TIME` | Optional | Generation | Specifies the same start time for all migrated tasks, overriding `msg.consume.start.ts` in each `FROM` DSN. Cannot be combined with `--task-start-time`. |
| `--task-start-time TASK_ID=TIME` | Optional; repeatable | Generation | Specifies the start time by old task ID, overriding only the listed tasks. Duplicate IDs or IDs outside the migration scope cause an error. |
| `--stop-old` | Do not stop by default | Old-task operations | Stops all old tasks in the snapshot and their associated database discovery monitors, then waits for them to stop. If the directory does not exist, first generates migration files online. |
| `--delete-old` | Do not delete by default | Old-task operations | Deletes associated database discovery configurations before deleting all old tasks in the snapshot. Tasks must already be stopped; this option does not stop them automatically. |
| `--stop-delete-old` | Do not stop or delete by default | Old-task operations | Stops all old tasks in the snapshot and deletes them only after confirming that all have stopped. A stop failure or timeout prevents the deletion phase. |
| `--stop-timeout SECONDS` | `300` seconds | Stop phase of old-task operations | Total timeout for queries, stopping monitors and tasks, and waiting for them to stop. Must be greater than `0`. Can be supplied only with an old-task operation option; deletion alone does not use this timeout. |
| `--execute MODE` | Do not execute SQL by default | Post-upgrade execution | `create` runs CREATE and verifies tasks; `start` verifies task configurations before running START and checking running status; `all` completes all CREATE statements and verification before START. Reads an existing migration directory and requires `--dsn`. |
| `--start-timeout SECONDS` | `300` seconds | New-task start phase | Total timeout for starting tasks and waiting for all tasks to enter `running`. Must be greater than `0`. Valid only with `--execute start` or `--execute all`; excludes preflight and creation. |
| `--dsn DSN` | Required with `--execute` | Post-upgrade execution | DSN for the new management cluster, for example `ws://root:password@td-a:6041`, configured separately from the source and target connections in the task's `FROM` and `TO` clauses. |
| `-h`, `--help` | No other parameters required | Help | Displays command-line help and exits without generating files or operating tasks. |

Parameter combinations and time formats follow these rules:

- Without an old-task operation or `--execute`, the script only generates files. `--stop-old`, `--delete-old`, and `--stop-delete-old` are mutually exclusive and cannot be combined with `--execute`. Do not supply start-time options when operating old tasks from an existing directory.
- Both start-time options accept RFC 3339 times with a `Z` or `±HH:MM` time zone, such as `2026-11-06T10:00:00+08:00`, with at most three fractional-second digits. Integers mean Unix milliseconds and cannot be negative.
- Start-time precedence is: explicit option, old `FROM` DSN value, then new task default. Tasks without an explicit start time inherit `msg.consume.start.ts` from the old `FROM` DSN, which must be a nonnegative integer Unix millisecond timestamp. If the old value is absent, the new task default is `0`, which disables time filtering.
- An explicit `--start-time 0` or `--task-start-time TASK_ID=0` overrides the old value and disables time filtering. The effective nonzero start time cannot be combined with nonzero `msg.consume.rawdata`.
- Inheriting a time filter does not inherit old topics, consumer groups, or consumption positions. Readable data still depends on snapshots, data retention, and WAL retention.

#### Verification After Migration

Whether you migrate manually or with the script, complete the following checks for every replication direction:

1. Run `SHOW XNODE TASKS WHERE type = 'replica';` to check the task count, source-to-target database mappings, `type='replica'`, and running states. Confirm that every expected replication direction has a corresponding task and that each task has started.
2. Follow [View Synchronization Progress](#view-synchronization-progress) to query each vgroup's progress using the new task's `__replica__<management_cluster_id>_<task_id>` identifier. Confirm that replication continues and gradually catches up.
3. Check the replication start-time boundary, table schemas, and actual data to confirm that no required data was missed during migration. Successful SQL generation, a `running` task state, or consumption positions catching up cannot individually prove the absence of data loss. Backfill separately if the data is no longer readable from snapshots or WAL.
4. After every direction has caught up and data verification passes, restore the original WAL retention periods. Confirm that all old tasks have stopped and no other business uses their topics, then clean up old resources separately according to the v3.3 maintenance procedures. Deleting new tasks does not clean up old resources.

Data that has already been replicated and falls after the start time can be replayed. Writes with the same timestamp overwrite each other, and idempotent metadata replay prevents duplicate rows. New tasks do not automatically restore the old version's database discovery rules. Create active-active tasks for each new database added later.
