---
sidebar_label: Data Ingestion
title: Data Ingestion
description: "Xnode distributed node and task management instructions"
---

# Data Synchronization SQL Manual

This document introduces SQL commands for managing the data synchronization functionality of TDengine, including Xnode nodes, synchronization tasks, Job shards, and Agent nodes.

## Xnode Management

Xnodes are the basic execution units of the data synchronization service, responsible for specific data transmission tasks.

### Create Xnode

#### Syntax

```sql
CREATE XNODE 'url'
CREATE XNODE 'url' USER name PASS 'password'
CREATE XNODE 'url' TOKEN 'token'
```

#### Parameter Description

- **url**: The address of the Xnode, in the format `host:port` with port to taosx GRPC service (6055 by default)
- **name** and **password**: For initial creation, it is recommended to specify a token or username and password for daemon xnoded to connect to taosd. If neither a token nor username and password is specified, a default token will be created.
- **token**: For authenticating when connecting to taosd

#### Example

```sql
taos> CREATE XNODE "h1:6055";
Create OK, 0 row(s) affected (0.050798s)

taos> CREATE XNODE 'x1:6055' USER root PASS 'taosdata';
Create OK, 0 row(s) affected (0.050798s)

taos> CREATE XNODE 'x2:6055' TOKEN 'C8V3o0ZVvYQ6sMEnjfixjtw0OvN9nIPFAL1HWvSKmHbQsds8vBpVbrEZn2hrzar';
Create OK, 0 row(s) affected (0.050798s)
```

### Modify Authorization

Modifying authentication will restart the daemon xnoded.

```sql
ALTER XNODE SET USER name PASS 'password'
ALTER XNODE SET TOKEN 'token'
```

#### Parameter Description

- **token**: For authenticating when connecting to taosd

#### Example

```sql
taos> ALTER XNODE SET TOKEN 'C8V3o0ZVvYQ6sMEnjfixjtw0OvN9nIPFAL1HWvSKmHbQsds8vBpVbrEZn2hrzar';
Query OK, 0 row(s) affected (0.024293s)

taos> ALTER XNODE SET USER root PASS 'taosdata';
Query OK, 0 row(s) affected (0.025161s)
```

### View Xnodes

#### Syntax

```sql
SHOW XNODES [WHERE condition]
```

#### Example

```sql
taos> SHOW XNODES;
```

Output result:

```sql
id | url     | status | create_time                 | update_time             |
===============================================================================
1  | h1:6050 | online | 2025-12-14 01:01:34.655     | 2025-12-14 01:01:34.655 |
Query OK, 1 row(s) in set (0.005518s)
```

### Drain Xnode

Reassign existing tasks of a node to other nodes for execution.

#### Syntax

```sql
DRAIN XNODE id
```

#### Parameter Description

- **id**: The ID of the Xnode

#### Example

```sql
taos> DRAIN XNODE 4;
Query OK, 0 row(s) affected (0.014246s)
```

### Delete Xnode

#### Syntax

```sql
DROP XNODE [FORCE] id | 'url'
```

#### Parameter Description

- **id**: The ID of the Xnode
- **url**: The address of the Xnode
- **FORCE**: Force delete Xnode

#### Example

```sql
taos> DROP XNODE 1;
Drop OK, 0 row(s) affected (0.038173s)

taos> DROP XNODE "h2:6050";
Drop OK, 0 row(s) affected (0.038593s)
```

## Task Management

Tasks define the source, destination, and data parsing rules for data synchronization.

### Create Task

#### Syntax

```sql
CREATE XNODE TASK 'name'
  FROM { 'from_dns' | DATABASE 'dbname' | TOPIC 'topic' }
  TO { 'to_dns' | DATABASE 'dbname' }
  [ WITH task_options ]

task_options:
  [ PARSER 'parser' ]
  [ STATUS 'status' ]
  [ VIA viaId ]
  [ XNODE_ID xnodeId ]
  [ REASON 'reason' ]
  [ LABELS 'labels' ]
```

Syntax note: All task_options can be used simultaneously, separated by spaces, order independent

#### Parameter Description

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

#### Example

```sql
taos> CREATE XNODE TASK "t4" FROM 'kafka://localhost:9092?topics=abc&group=abcgroup' TO 'taos+ws://localhost:6041/test' WITH parser '{"model":{"name":"cc_abc","using":"cc","tags":["g"],"columns":["ts","b"]},"mutate":[{"map":{"ts":{"cast":"ts","as":"TIMESTAMP(ms)"},"b":{"cast":"a","as":"VARCHAR"},"g":{"value":"1","as":"INT"}}}]}';
Create OK, 0 row(s) affected (0.038959s)
```

### View Tasks

#### Syntax

```sql
SHOW XNODE TASKS [WHERE condition]
```

#### Example

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

#### Syntax

```sql
START XNODE TASK id | 'name'
```

#### Example

```sql
taos> START XNODE TASK 1;
DB error: Xnode url response http code not 200 error [0x8000800C] (0.002160s)
```

### Stop Task

#### Syntax

```sql
STOP XNODE TASK id | 'name'
```

#### Example

```sql
taos> STOP XNODE TASK 1;
DB error: Xnode url response http code not 200 error [0x8000800C] (0.002047s)
```

### Modify Task

#### Syntax

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

#### Example

```sql
taos> ALTER XNODE TASK 3 FROM 'pulsar://zgc...' TO 'testdb' WITH xnode_id 33 via 333 reason 'zgc_test';
Query OK, 0 row(s) affected (0.036077s)
```

### Delete Task

#### Syntax

```sql
DROP XNODE TASK id | 'name'
```

#### Example

```sql
taos> DROP XNODE TASK 3;
Drop OK, 0 row(s) affected (0.038191s)
```

## Job Management

Job is the execution shard of a Task, supporting both manual and automatic load balancing.

### View Jobs

#### Syntax

```sql
SHOW XNODE JOBS [WHERE condition]
```

#### Example

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

#### Syntax

```sql
REBALANCE XNODE JOB jid WITH XNODE_ID xnodeId;
```

Syntax note: Manual rebalance currently only supports the xnode_id parameter, which must include xnode id information.

#### Example

```sql
taos> REBALANCE XNODE JOB 1 WITH xnode_id 1;
Query OK, 0 row(s) affected (0.011808s)
```

### Automatic Rebalance

#### Syntax

```sql
REBALANCE XNODE JOBS [ WHERE job_conditions ]
```

Syntax note: WHERE job_conditions is optional, used to filter job data that meets the conditions. Functions are not supported, all fields that appear in the SHOW XNODE JOBS command are supported. Without a WHERE condition statement, it means all jobs will undergo automatic load balancing.

#### Example

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

#### Syntax

```sql
CREATE XNODE AGENT 'name' [WITH agent_options]

agent_options:
  [STATUS 'status']
```

#### Parameters

- **name**: Name of the Agent node
- **status**: Specifies the initial status using the `WITH`clause

#### Example

```sql
taos> create xnode agent 'a1';
Create OK, 0 row(s) affected (0.013910s)

taos> create xnode agent 'a2' with status 'running';
Create OK, 0 row(s) affected (0.013414s)
```

### Query Agent

#### Syntax

```sql
SHOW XNODE AGENTS [WHERE condition]
```

#### Example

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

#### Syntax

```sql
ALTER XNODE AGENT agent_id WITH alter_options

alter_options {
  STATUS 'status'
  | NAME 'name'
}
```

#### Parameters

- **name**: New name for the Agent node
- **status**: New status using the `WITH`clause

#### Example

```sql
taos> alter xnode agent 1 with name 'test1';
Query OK, 0 row(s) affected (0.008387s)

taos> alter xnode agent 'a2' with name 'test2' status 'online';
Query OK, 0 row(s) affected (0.008685s)
```

### Delete Agent

#### Syntax

```sql
DROP XNODE AGENT agent_id
```

#### Parameters

- **agent_id**: ID of the Agent node

#### Example

```sql
taos> drop xnode agent 1;
Drop OK, 0 row(s) affected (0.012281s)
```
