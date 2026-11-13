---
sidebar_label: Data Ingestion (Xnode)
title: Data Ingestion (Xnode)
description: "Xnode distributed node and task management instructions"
toc_max_heading_level: 4
---

Xnode is a distributed execution node in the TDengine data ingestion service. It runs data synchronization tasks and writes data from external data sources into TDengine in real time or in batches. This document describes the SQL commands used to manage data ingestion, including Xnode nodes, data ingestion tasks, Job shards, and Agents.

Task auto-resume is controlled by the `xnoded` daemon and can be configured via `--auto-resume` or the `XNODED_AUTO_RESUME` environment variable; it is enabled by default.

## Xnode Management

Xnodes are the basic execution units of the data synchronization service, responsible for specific data transmission tasks.

### Create Xnode

**Syntax**

```sql
CREATE XNODE 'url'
CREATE XNODE 'url' USER name PASS 'password'
CREATE XNODE 'url' TOKEN 'token'
```

**Parameter Description**

- **url**: The address of the Xnode, in the format `host:port` with port to taosx GRPC service (6055 by default)
- **name** and **password**: For initial creation, it is recommended to specify a token or username and password for daemon xnoded to connect to taosd. If neither a token nor username and password is specified, a default token will be created.
- **token**: For authenticating when connecting to taosd

While the control connection is active, a taosX instance accepts xnoded connections from only one TDengine cluster. If another cluster tries to register it, `CREATE XNODE` returns an ownership conflict without interrupting the existing control connection. To migrate the taosX instance, first execute `DROP XNODE` in the original cluster and then execute `CREATE XNODE` in the new cluster. The current xnoded identity uses a compile-time static JWT secret and a client-declared `cluster_id`. This mechanism prevents accidental registration; it does not protect against forged identities or malicious takeover.

**Example**

```sql
taos> CREATE XNODE "h1:6055";
Create OK, 0 row(s) affected (0.050798s)

taos> CREATE XNODE 'x1:6055' USER root PASS 'taosdata';
Create OK, 0 row(s) affected (0.050798s)

taos> CREATE XNODE 'x2:6055' TOKEN 'C8V3o0ZVvYQ6sMEnjfixjtw0OvN9nIPFAL1HWvSKmHbQsds8vBpVbrEZn2hrzar';
Create OK, 0 row(s) affected (0.050798s)
```

### Modify Authorization

Modifying authentication will restart the daemon xnoded. This command alters the credentials used by the single `xnoded` daemon to connect to `taosd`.

```sql
ALTER XNODE SET USER name PASS 'password'
ALTER XNODE SET TOKEN 'token'
```

**Parameter Description**

- **token**: For authenticating when connecting to taosd

**Example**

```sql
taos> ALTER XNODE SET TOKEN 'C8V3o0ZVvYQ6sMEnjfixjtw0OvN9nIPFAL1HWvSKmHbQsds8vBpVbrEZn2hrzar';
Query OK, 0 row(s) affected (0.024293s)

taos> ALTER XNODE SET USER root PASS 'taosdata';
Query OK, 0 row(s) affected (0.025161s)
```

### Restart Daemon

Restart the `xnoded` daemon on the dnode hosting the current mnode leader. This command does not restart the remote `taosx` data-plane process.

This operation requires the `NODE ALTER` privilege and briefly interrupts the `xnoded` daemon. Avoid issuing it repeatedly in a short period.

**Syntax**

```sql
RESTART XNODE
```

**Example**

```sql
taos> RESTART XNODE;
Query OK, 0 row(s) affected (0.028153s)
```

### View Xnodes

**Syntax**

```sql
SHOW XNODES [WHERE condition]
```

**Example**

```sql
taos> SHOW XNODES;
```

Output result:

```sql
id | url     | status | create_time                 | update_time             |
===============================================================================
1  | h1:6055 | online | 2025-12-14 01:01:34.655     | 2025-12-14 01:01:34.655 |
Query OK, 1 row(s) in set (0.005518s)
```

### Drain Xnode

Reassign existing tasks of a node to other nodes for execution.

**Syntax**

```sql
DRAIN XNODE id
```

**Parameter Description**

- **id**: The ID of the Xnode

**Example**

```sql
taos> DRAIN XNODE 4;
Query OK, 0 row(s) affected (0.014246s)
```

### Delete Xnode

**Syntax**

```sql
DROP XNODE {id | 'url'} [FORCE]
DROP XNODE FORCE {id | 'url'}
```

**Parameter Description**

- **id**: The ID of the Xnode
- **url**: The address of the Xnode
- **FORCE**: Force delete Xnode

**Example**

```sql
taos> DROP XNODE 1;
Drop OK, 0 row(s) affected (0.038173s)

taos> DROP XNODE 'localhost:6055';
Drop OK, 0 row(s) affected (0.038593s)
```

To register the taosX instance with a different TDengine cluster, you must first execute `DROP XNODE` in the original cluster to release ownership.

## Task Management

Tasks define the source, destination, and data parsing rules for data synchronization.

### Create Task

There are two types of tasks:

- **Normal tasks**: write data from external data sources such as MQTT and Kafka into TDengine.
- **Active-active tasks**: available from v3.4.3.11, replicate data from a source database to a target database and can be used to build an active-active configuration.

Use `CREATE XNODE TASK` to create either type. Specify `WITH TYPE 'replica'` for an active-active task; omit it for a normal task.

#### Normal Tasks

**Syntax**

```sql
CREATE XNODE TASK 'name'
  FROM { 'from_dns' | DATABASE 'dbname' | TOPIC 'topic' }
  TO { 'to_dns' | DATABASE 'dbname' }
  [ WITH task_options ]

task_options:
  [ TYPE 'replica' ]
  [ PARSER 'parser' ]
  [ STATUS 'status' ]
  [ VIA viaId ]
  [ XNODE_ID xnodeId ]
  [ REASON 'reason' ]
  [ LABELS 'labels' ]
```

Syntax note: All task_options can be used simultaneously, separated by spaces, order independent

**Parameter Description**

| Parameter    | Description                                        |
| :----------- | :------------------------------------------------- |
| **name**     | Task name                                          |
| **from_dns** | Source connection string (e.g., `mqtt://...`)      |
| **dbname**   | Database name                                      |
| **topic**    | Topic name                                         |
| **to_dns**   | Destination connection string (e.g., `taos://...`) |
| **parser**   | Data parsing configuration (JSON format)           |
| **status**   | Task status                                        |
| **xnodeId**  | The xnode node ID where the task resides           |
| **viaId**    | The agent ID where the task resides                |
| **reason**   | Reason for recent task execution failure           |
| **labels**   | Task labels, stored as a JSON string               |
| `TYPE` | Available from v3.4.3.11. Accepts only `replica`, case-insensitively; omission creates a normal task. Immutable after creation. |

**Example**

```sql
taos> CREATE XNODE TASK "t4" FROM 'kafka://localhost:9092?topics=abc&group=abcgroup' TO 'taos+ws://localhost:6041/test' WITH parser '{"model":{"name":"cc_abc","using":"cc","tags":["g"],"columns":["ts","b"]},"mutate":[{"map":{"ts":{"cast":"ts","as":"TIMESTAMP(ms)"},"b":{"cast":"a","as":"VARCHAR"},"g":{"value":"1","as":"INT"}}}]}';
Create OK, 0 row(s) affected (0.038959s)
```

#### Active-Active Tasks

From v3.4.3.11, `WITH TYPE 'replica'` creates active-active tasks scheduled by xnoded. Each task copies data from a source database to a target database. Create a reverse task in the other cluster to build an active-active configuration.

**Prerequisites**

- The management cluster, xnoded, and taosX have been upgraded to a version supporting this syntax.
- Both endpoints run v3.4.0.0 or later.
- Both databases exist with matching precision, and the source database has `wal_retention_period > 0`.
- Both endpoints meet the active-active license requirements.

**DSN Parameters**

Specify the DSNs in `FROM` and `TO` when creating a task, and use `ALTER XNODE TASK` to update them. Active-active tasks accept only the connection information and parameters listed below. Unrecognized parameters are rejected.

**Connection Information**

The following connection information applies to both `FROM` and `TO`. The driver, protocol, address list, ports, and database name are immutable after creation; changes to the other items take effect on the next task start.

| Parameter | Description |
| --- | --- |
| Driver, protocol, address list, ports, and database | Must meet the source and target requirements on creation; immutable after creation |
| Username and password | Optional on creation; can be modified, and changes take effect on the next start |
| `token`, `bearer_token`, `td.connect.token` | Optional on creation; can be modified, and changes take effect on the next start |
| `tls_mode`, `tls_version`, `tls_ca`, `conn_mode` | Optional on creation; can be modified, and changes take effect on the next start |

**Subscription Start Parameters**

The following parameters apply only to `FROM` and can be set only when creating a task. When modifying the DSN, keep any supplied parameters and their original values. Do not add previously omitted parameters, even if their values match the defaults.

| Parameter | Description |
| --- | --- |
| `auto.offset.reset` | Initial position of the consumer group subscription, defaulting to `earliest`. Accepts `earliest` or `latest`, effective only when no committed position exists; otherwise consumption resumes from the committed position |
| `experimental.snapshot.enable` | Defaults to `true`. `true` reads persisted TSDB data before WAL; `false` reads only WAL |
| `msg.consume.start.ts` | Subscription sync start time, defaulting to `0`. Copies only data records whose primary timestamp `ts` is no earlier than this value. Accepts a nonnegative integer Unix timestamp in milliseconds; `0` disables the filter |

**Runtime Parameters**

The following parameters apply only to `FROM`. You can set them when creating a task or change them when modifying the DSN. Changes take effect the next time the task starts. Parameter values and combination restrictions are checked during both creation and modification. For detailed definitions, see [Managing Consumers](../../10-developer-guide/07-subscription-api.md#creation-parameters).

| Parameter | Description |
| --- | --- |
| `write_concurrency` | Write concurrency tuning |
| `num.of.writers` | Write concurrency tuning |
| `commit.chunk.size` | taosX position commit tuning |
| `commit.interval.ms` | taosX position commit tuning; adjusts the commit interval |
| `max.polling.timeout` | Consumption polling tuning |
| `enable.concurrent.polling` | Consumption polling tuning |
| `refresh.progress.interval` | Progress refresh tuning |
| `enable.auto.commit` | Whether to enable automatic offset commit. Accepts only `false`; taosX commits positions after successful target writes |
| `auto.commit.interval.ms` | Time interval for automatic offset commits. Forwarded, but has no effect because auto-commit is disabled |
| `enable.replay` | Whether to enable the data replay function. Accepts only `false`; timed WAL replay does not support database replication and resume |
| `msg.consume.rawdata` | Pulls raw WAL binary payloads when consuming data. Defaults to `0` for decoded data; nonzero values require `msg.consume.start.ts` to be omitted or set to `0` |
| `msg.with.table.name` | Whether to allow parsing the table name from the message. Forwarded |
| `msg.enable.batchmeta` | Whether the server may combine multiple metadata records into a batch metadata response. Forwarded |
| `session.timeout.ms` | Timeout after consumer heartbeat is lost. Forwarded |
| `max.poll.interval.ms` | The longest time interval for consumer poll data fetching. Forwarded |
| `fetch.max.wait.ms` | The maximum time it takes for the server to return data once. Forwarded |
| `min.poll.rows` | The minimum number of data returned by the server once. Forwarded |

**System-Reserved Parameters**

The system sets or generates these parameters. They cannot be supplied manually in either `FROM` or `TO`, on creation or modification.

| Parameter | Description |
| --- | --- |
| `replica`, `msg.consume.excluded` | Set by the system |
| `timeout`, `prefer`, `enable.wal.marker` | Set by the system |
| `group.id`, `use.topic.name`, `client.id` | Generated by the system |

Parameter names are URL-decoded before validation, so `%72eplica` is reserved too. Normal tasks also reject `replica` and `msg.consume.excluded` in the source DSN on creation and modification.

**Usage Notes**

`FROM` must be a `tmq` DSN and `TO` a `taos` DSN, with database names in their paths. `FROM DATABASE`, `FROM TOPIC`, `TO DATABASE`, `PARSER` (including transformations), and `VIA` are unsupported. A newly created task is stopped.

Within one management cluster, each mapping from a source address and database to a target address and database can have only one active-active task. Stopped tasks and tasks in `deleting` or `delete_failed` state still reserve their mapping. A duplicate `CREATE XNODE TASK` fails without creating another record. Reverse mappings, different target databases, and different target clusters can have separate tasks. The uniqueness check treats Native and WebSocket drivers as equivalent for the same address. Administrators must also avoid creating duplicate tasks through address aliases or in separate management clusters.

Each task owns a separate topic and consumer group, both named `__replica__<management_cluster_id>_<task_id>`. `group.id`, `use.topic.name`, and `client.id` are generated automatically and cannot be supplied. Different mappings must not share a consumer group: sharing distributes source vgroups across tasks and leaves each target with only part of the data.

See [Error Codes](../../10-developer-guide/09-error-codes.md#xnode) for related error codes, causes, and recommended actions. See [Active-Active Configuration](../../12-operations-and-tooling/02-operations/12-ha/03-dual.md) for progress queries, WAL limits, monitoring, and migration from v3.3.

**Example**

```sql
CREATE XNODE TASK 'aa_power_a_b'
  FROM 'tmq+ws://root:taosdata@td1:6041/power'
  TO 'taos+ws://root:taosdata@td2:6041/power'
  WITH TYPE 'replica';
START XNODE TASK 'aa_power_a_b';
```

### View Tasks

**Syntax**

```sql
SHOW XNODE TASKS [WHERE condition]
```

From v3.4.3.11, the final `type` column contains `normal` for normal tasks or `replica` for active-active tasks; `information_schema.ins_xnode_tasks` provides the same column. Use `SHOW XNODE TASKS WHERE type = 'replica';` to query active-active tasks. The `desired_state` column indicates the state requested by the user, while the `status` and `reason` columns indicate the actual execution state of the task and the most recent failure reason.

The `parser` column uses BLOB to return the complete task configuration. In a `WHERE` condition, `parser` supports relational comparisons, `IS NULL`, `IS NOT NULL`, `LIKE`, `NOT LIKE`, `MATCH`/`REGEXP`, `NMATCH`, `IN`, `NOT IN`, `BETWEEN`, and `NOT BETWEEN`, including logical combinations. String functions, JSON operators, arithmetic, and bitwise operators are not supported for this column. Operators for other columns depend on their respective data types.

```sql
SHOW XNODE TASKS WHERE parser != '{}' AND parser IS NOT NULL;
```

**Example**

```sql
taos> SHOW XNODE TASKS;
```

Output result:

```sql
taos> SHOW XNODE TASKS \G;
*************************** 1.row ***************************
         id: 3
       name: t4
       from: kafka://localhost:9092?topics=abc&group=abcgroup
         to: taos+ws://localhost:6041/test
     parser: {"model":{"name":"cc_abc","using":"cc","tags":["g"],"columns":["ts","b"]},"mutate":[{"map":{"ts":{"cast":"ts","as":"TIMESTAMP(ms)"},"b":{"cast":"a","as":"VARCHAR"},"g":{"value":"1","as":"INT"}}}]}
        via: NULL
   xnode_id: NULL
     status: NULL
     reason: NULL
 created_by: root
     labels: NULL
create_time: 2026-01-13 07:56:18.076
update_time: 2026-01-13 07:56:18.076
Query OK, 2 row(s) in set (0.019692s)
```

### Start Task

**Syntax**

```sql
START XNODE TASK id | 'name'
```

Before running this command, ensure the corresponding Xnode is online and reachable.

If xnoded finds an invalid DSN for an active-active task during startup or reconnection, it saves `status=failed` and `desired_state=stopped` and records the failure in `reason`. The task and its jobs do not resume automatically. Run `SHOW XNODE TASKS` to check the status and reason. After correcting the DSN, explicitly run `START XNODE TASK` to resume the task.

**Example**

```sql
taos> START XNODE TASK 1;
```

### Stop Task

**Syntax**

```sql
STOP XNODE TASK id | 'name'
```

**Active-Active Task Note**: Stopping an active-active task commits the offsets of data that has been written to the target successfully; starting the task again resumes replication from those offsets.

**Example**

```sql
taos> STOP XNODE TASK 1;
```

### Modify Task

**Syntax**

```sql
ALTER XNODE TASK { id | 'name' }
  [ FROM { 'from_dns' | DATABASE 'dbname' | TOPIC 'topic' } ]
  [ TO { 'to_dns' | DATABASE 'dbname' } ]
  [ WITH alter_options ]

alter_options:
  [ PARSER 'parser' ]
  [ NAME 'name' ]
  [ STATUS 'status' ]
  [ VIA viaId ]
  [ XNODE_ID xnodeId ]
  [ REASON 'reason' ]
  [ LABELS 'labels' ]
```

Syntax note: The meaning of task_options is the same as when creating a task

**Active-Active Task Notes**

Task type is immutable after creation. When modifying an active-active task's DSNs, follow these rules:

- The driver, protocol, address list, ports, and database are immutable. Hostnames are compared case-insensitively, and ports are compared as supplied without inserting defaults. Switching between Native and WebSocket also changes the connection target.
- Keep all supplied subscription start parameters and their original values. Do not add previously omitted parameters, even if their values match the defaults.
- Credentials and allowed runtime parameters can be changed. Changes take effect on the next task start. For a running task, run `STOP XNODE TASK` followed by `START XNODE TASK` to apply the changes.

**Example**

```sql
taos> ALTER XNODE TASK 3 FROM 'pulsar://zgc...' TO 'testdb' WITH xnode_id 33 via 333 reason 'zgc_test';
Query OK, 0 row(s) affected (0.036077s)
```

### Delete Task

**Syntax**

```sql
DROP XNODE TASK id | 'name'
```

**Deletion Behavior**

Normal tasks are deleted synchronously, and active-active tasks are deleted asynchronously. Running tasks can be dropped directly.

**Normal Task Notes**

When `DROP XNODE TASK` succeeds, the task record has been removed and no longer appears in `SHOW XNODE TASKS`. xnoded then stops the task and removes its jobs in the background. If the node running the task is temporarily unavailable, xnoded stops the leftover task processes after the node reconnects. Normal tasks never enter the `deleting` or `delete_failed` state.

**Active-Active Task Notes**

For an active-active task, in addition to stopping the task and removing its jobs, xnoded must clean up the source topic, consumer group, and positions. Because the source may be temporarily unreachable, active-active tasks are deleted asynchronously.

A successful `DROP XNODE TASK` means the deletion request has been accepted. mnode first saves `desired_state=stopped` and `status=deleting`, and removes the task record only after xnoded completes the cleanup and reports it. The task still appears in `SHOW XNODE TASKS` during cleanup.

If the request cannot be delivered to xnoded, `DROP XNODE TASK` may return an error even though the task has already entered the `deleting` state. Once xnoded is available again, it checks the task state and continues the pending deletion. You can run `SHOW XNODE TASKS` to check the status and run `DROP XNODE TASK` again to retry. Tasks in `deleting` or `delete_failed` cannot be started.

If no other consumer group uses the topic, cleanup uses `DROP TOPIC IF EXISTS FORCE`; otherwise it uses `DROP CONSUMER GROUP IF EXISTS FORCE` to remove only this task's group. Cleanup also succeeds if the resources no longer exist.

xnoded retries temporary connection failures with backoff and resumes cleanup from mnode after a restart. Permanent errors, such as permission or credential failures, put the task in `delete_failed` state. This state and the `reason` are persisted, and restarting does not resume cleanup. Fix the problem and run `DROP XNODE TASK` again.

The same active-active mapping cannot be recreated until cleanup completes. Recreating a task after cleanup generates new identifiers and positions without inheriting old progress.

**Example**

```sql
taos> DROP XNODE TASK 3;
Drop OK, 0 row(s) affected (0.038191s)
```

## Job Management

Job is the execution shard of a Task, supporting both manual and automatic load balancing.

### View Jobs

**Syntax**

```sql
SHOW XNODE JOBS [WHERE condition]
```

The `config` column uses BLOB to return the complete job configuration. In a `WHERE` condition, `config` supports relational comparisons, `IS NULL`, `IS NOT NULL`, `LIKE`, `NOT LIKE`, `MATCH`/`REGEXP`, `NMATCH`, `IN`, `NOT IN`, `BETWEEN`, and `NOT BETWEEN`, including logical combinations. String functions, JSON operators, arithmetic, and bitwise operators are not supported for this column. Operators for other columns depend on their respective data types.

```sql
SHOW XNODE JOBS WHERE config != '{}' AND config IS NOT NULL;
```

**Example**

```sql
taos> SHOW XNODE JOBS\G;
*************************** 1.row ***************************
       id: 1
  task_id: 3
   config: config_json
      via: -1
 xnode_id: 11
   status: running
   reason: NULL
create_time: 2025-12-14 02:52:31.281
update_time: 2025-12-14 02:52:31.281
Query OK, 1 row(s) in set (0.004714s)
```

### Manual Rebalance

**Syntax**

```sql
REBALANCE XNODE JOB jid WITH XNODE_ID xnodeId;
```

Syntax note: Manual rebalance currently only supports the xnode_id parameter, which must include xnode id information.

**Example**

```sql
taos> REBALANCE XNODE JOB 1 WITH xnode_id 1;
Query OK, 0 row(s) affected (0.011808s)
```

### Automatic Rebalance

**Syntax**

```sql
REBALANCE XNODE JOBS [ WHERE job_conditions ]
```

Syntax note: WHERE job_conditions is optional, used to filter job data that meets the conditions. Functions are not supported, all fields that appear in the SHOW XNODE JOBS command are supported. Without a WHERE condition statement, it means all jobs will undergo automatic load balancing.

**Example**

```sql
taos> REBALANCE XNODE JOBS WHERE id>1;
Query OK, 0 row(s) affected (0.014246s)

taos> REBALANCE XNODE JOBS WHERE task_id=1 and (xnode_id=3 or xnode_id=4);
Query OK, 0 row(s) affected (0.007237s)

taos> REBALANCE XNODE JOBS;
Query OK, 0 row(s) affected (0.023245s)
```

## Agent Management

The Agent node serves as the data collection and forwarding unit in the data synchronization service, responsible for collecting data and forwarding it to Xnode nodes.

### Create Agent

**Syntax**

```sql
CREATE XNODE AGENT 'name' [WITH agent_options]

agent_options:
  [STATUS 'status']
```

**Parameters**

- **name**: Name of the Agent node
- **status**: Specifies the initial status using the `WITH`clause

**Example**

```sql
taos> create xnode agent 'a1';
Create OK, 0 row(s) affected (0.013910s)

taos> create xnode agent 'a2' with status 'running';
Create OK, 0 row(s) affected (0.013414s)
```

### Query Agent

**Syntax**

```sql
SHOW XNODE AGENTS [WHERE condition]
```

**Example**

```sql
taos> show xnode agents\G;
*************************** 1.row ***************************
         id: 1
       name: a1
      token: eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE3NjgxODI3MDEzNjQsInN1YiI6MX0.FP5rfzQplBrJrbV7Dj_R8fCpiN5uLaADegcnqExwepg
     status: NULL
create_time: 2026-01-12 09:51:41.364
update_time: 2026-01-12 09:51:41.364
```

### Update Agent

**Syntax**

```sql
ALTER XNODE AGENT {agent_id | 'name'} WITH alter_options

alter_options {
  STATUS 'status'
  | NAME 'name'
}
```

**Parameters**

- **agent_id** / **name**: ID or name of the Agent node
- **name**: New name for the Agent node (in `WITH`)
- **status**: New status using the `WITH` clause

**Example**

```sql
taos> alter xnode agent 1 with name 'test1';
Query OK, 0 row(s) affected (0.008387s)

taos> alter xnode agent 'a2' with name 'test2' status 'online';
Query OK, 0 row(s) affected (0.008685s)
```

### Delete Agent

**Syntax**

```sql
DROP XNODE AGENT {agent_id | 'name'}
```

**Parameters**

- **agent_id** / **name**: ID or name of the Agent node

**Example**

```sql
taos> drop xnode agent 1;
Drop OK, 0 row(s) affected (0.012281s)
```
