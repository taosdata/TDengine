---
sidebar_label: 数据接入（Xnode）
title: 数据接入（Xnode）
description: Xnode 节点、数据接入任务、Job 分片与 Agent 的 SQL 管理
toc_max_heading_level: 3
---

Xnode 是 TDengine 数据接入服务的分布式执行节点，负责运行数据同步任务，将外部数据源的数据实时或批量写入 TDengine。本文介绍管理数据接入功能的 SQL 命令，包括 Xnode 节点、数据接入任务、Job 分片以及 Agent。

## Xnode 节点管理

Xnode 节点是数据同步服务的基本执行单元，负责具体的数据传输工作。

### 创建节点

**语法**

```sql
CREATE XNODE 'url'
CREATE XNODE 'url' USER name PASS 'password'
CREATE XNODE 'url' TOKEN 'token'
```

**参数说明**

- **url**：Xnode 节点地址，格式为 `host:port`；端口为 taosX gRPC 端口（默认 `6055`）
- **name** / **password**：首次创建建议指定 `TOKEN`，或用户名与密码，供守护进程 `xnoded` 连接 `taosd`。未指定时将创建默认 token
- **token**：用于连接 `taosd` 的认证凭据

**示例**

```sql
CREATE XNODE 'localhost:6055';
CREATE XNODE 'localhost:6055' USER root PASS 'taosdata';
CREATE XNODE 'localhost:6055' TOKEN 'C8V3o0ZVvYQ6sMEnjfixjtw0OvN9nIPFAL1HWvSKmHbQsds8vBpVbrEZn2hrzar';
```

### 修改认证

修改认证会重启守护进程 `xnoded`。本命令修改的是单个 `xnoded` 守护进程连接 `taosd` 所用的认证凭据。

```sql
ALTER XNODE SET USER name PASS 'password'
ALTER XNODE SET TOKEN 'token'
```

**参数说明**

- **token**：用于连接 `taosd` 的认证凭据

**示例**

```sql
ALTER XNODE SET TOKEN 'C8V3o0ZVvYQ6sMEnjfixjtw0OvN9nIPFAL1HWvSKmHbQsds8vBpVbrEZn2hrzar';
ALTER XNODE SET USER root PASS 'taosdata';
```

### 查看节点

**语法**

```sql
SHOW XNODES [WHERE condition]
```

**示例**

```sql
SHOW XNODES;
```

示例输出：

```text
id | url             | status | create_time             | update_time             |
===================================================================================
1  | localhost:6055  | online | 2025-12-14 01:01:34.655 | 2025-12-14 01:01:34.655 |
```

### 排空节点

将某节点上已有任务重新分配到其他节点执行。

**语法**

```sql
DRAIN XNODE id
```

**参数说明**

- **id**：Xnode 节点 ID

**示例**

```sql
DRAIN XNODE 4;
```

### 删除节点

**语法**

```sql
DROP XNODE {id | 'url'} [FORCE]
DROP XNODE FORCE {id | 'url'}
```

**参数说明**

- **id**：Xnode 节点 ID
- **url**：Xnode 节点地址
- **FORCE**：强制删除节点

**示例**

```sql
DROP XNODE 1;
DROP XNODE 'localhost:6055';
```

## 任务管理

任务（Task）定义了数据同步的源端、目标端以及数据解析规则。

### 创建任务

**语法**

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

**说明**

`task_options` 各选项可同时使用，以空格分隔，顺序无关。

**参数说明**

| 参数 | 说明 |
| ---------- | --- |
| `name`     | 任务名称 |
| `from_dns` | 源端连接字符串（如 `mqtt://...`） |
| `dbname`   | 数据库名称 |
| `topic`    | Topic 名称 |
| `to_dns`   | 目标端连接字符串（如 `taos://...`） |
| `parser`   | 数据解析配置（JSON 格式） |
| `status`   | 任务状态 |
| `xnodeId`  | 任务所在的 Xnode 节点 ID |
| `viaId`    | 任务所在的 Agent ID |
| `reason`   | 任务最近执行失败原因 |
| `labels`   | 任务标签，JSON 字符串 |

**示例**

```sql
CREATE XNODE TASK 't4'
  FROM 'kafka://localhost:9092?topics=abc&group=abcgroup'
  TO 'taos+ws://localhost:6041/test'
  WITH PARSER '{"model":{"name":"cc_abc","using":"cc","tags":["g"],"columns":["ts","b"]},"mutate":[{"map":{"ts":{"cast":"ts","as":"TIMESTAMP(ms)"},"b":{"cast":"a","as":"VARCHAR"},"g":{"value":"1","as":"INT"}}}]}';
```

### 查看任务

**语法**

```sql
SHOW XNODE TASKS [WHERE condition]
```

`parser` 列使用 BLOB 返回完整任务配置。在 `WHERE` 条件中，`parser` 支持关系比较、`IS NULL`、`IS NOT NULL`、`LIKE`、`NOT LIKE`、`MATCH`/`REGEXP`、`NMATCH`、`IN`、`NOT IN`、`BETWEEN`、`NOT BETWEEN` 及其逻辑组合；不支持字符串函数、JSON 运算、算术和位运算。其他列支持的运算由各自的数据类型决定。

```sql
SHOW XNODE TASKS WHERE parser != '{}' AND parser IS NOT NULL;
```

**示例**

```sql
SHOW XNODE TASKS\G;
```

示例输出：

```text
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
```

### 启动任务

**语法**

```sql
START XNODE TASK {id | 'name'}
```

执行前需确保对应 Xnode 在线且可达。

**示例**

```sql
START XNODE TASK 1;
```

### 停止任务

**语法**

```sql
STOP XNODE TASK {id | 'name'}
```

**示例**

```sql
STOP XNODE TASK 1;
```

### 修改任务

**语法**

```sql
ALTER XNODE TASK {id | 'name'}
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

**说明**

`alter_options` 各选项含义与创建任务时相同。

**示例**

```sql
ALTER XNODE TASK 3
  FROM 'pulsar://localhost:6650'
  TO 'testdb'
  WITH XNODE_ID 33 VIA 333 REASON 'zgc_test';
```

### 删除任务

**语法**

```sql
DROP XNODE TASK {id | 'name'}
```

**示例**

```sql
DROP XNODE TASK 3;
```

## Job 分片管理

Job 是任务（Task）的执行分片，支持手动与自动负载均衡。

### 查看 Job 分片

**语法**

```sql
SHOW XNODE JOBS [WHERE condition]
```

`config` 列使用 BLOB 返回完整 Job 配置。在 `WHERE` 条件中，`config` 支持关系比较、`IS NULL`、`IS NOT NULL`、`LIKE`、`NOT LIKE`、`MATCH`/`REGEXP`、`NMATCH`、`IN`、`NOT IN`、`BETWEEN`、`NOT BETWEEN` 及其逻辑组合；不支持字符串函数、JSON 运算、算术和位运算。其他列支持的运算由各自的数据类型决定。

```sql
SHOW XNODE JOBS WHERE config != '{}' AND config IS NOT NULL;
```

**示例**

```sql
SHOW XNODE JOBS\G;
```

示例输出：

```text
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
```

### 手动负载均衡

**语法**

```sql
REBALANCE XNODE JOB jid WITH XNODE_ID xnodeId;
```

**说明**

手动负载均衡当前仅支持 `XNODE_ID` 参数，必须指定目标 Xnode ID。

**示例**

```sql
REBALANCE XNODE JOB 1 WITH XNODE_ID 1;
```

### 自动负载均衡

**语法**

```sql
REBALANCE XNODE JOBS [WHERE job_conditions]
```

**说明**

`WHERE job_conditions` 可选，用于过滤待均衡的 Job。不支持函数；支持 `SHOW XNODE JOBS` 中出现的所有字段。未指定 `WHERE` 时，对全部 Job 做自动负载均衡。

**示例**

```sql
REBALANCE XNODE JOBS WHERE id > 1;
REBALANCE XNODE JOBS WHERE task_id = 1 AND (xnode_id = 3 OR xnode_id = 4);
REBALANCE XNODE JOBS;
```

## Agent 管理

Agent 是数据同步服务中的采集与转发单元，负责采集数据并转发至 Xnode 节点。

### 创建 Agent

**语法**

```sql
CREATE XNODE AGENT 'name' [WITH agent_options]

agent_options:
  [STATUS 'status']
```

**参数说明**

- **name**：Agent 名称
- **status**：创建时的状态（通过 `WITH` 指定）

**示例**

```sql
CREATE XNODE AGENT 'a1';
CREATE XNODE AGENT 'a2' WITH STATUS 'running';
```

### 查询 Agent

**语法**

```sql
SHOW XNODE AGENTS [WHERE condition]
```

**示例**

```sql
SHOW XNODE AGENTS\G;
```

示例输出：

```text
*************************** 1.row ***************************
         id: 1
       name: a1
      token: eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE3NjgxODI3MDEzNjQsInN1YiI6MX0.FP5rfzQplBrJrbV7Dj_R8fCpiN5uLaADegcnqExwepg
     status: NULL
create_time: 2026-01-12 09:51:41.364
update_time: 2026-01-12 09:51:41.364
```

### 更新 Agent

**语法**

```sql
ALTER XNODE AGENT {agent_id | 'name'} WITH alter_options

alter_options:
  STATUS 'status'
  | NAME 'name'
```

**参数说明**

- **name**：Agent 名称
- **status**：更新后的状态

**示例**

```sql
ALTER XNODE AGENT 1 WITH NAME 'test1';
ALTER XNODE AGENT 'a2' WITH NAME 'test2' STATUS 'online';
```

### 删除 Agent

**语法**

```sql
DROP XNODE AGENT {agent_id | 'name'}
```

**参数说明**

- **agent_id** / **name**：Agent 的 ID 或名称

**示例**

```sql
DROP XNODE AGENT 1;
```
