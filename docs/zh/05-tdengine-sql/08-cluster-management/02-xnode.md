---
sidebar_label: 数据接入（Xnode）
title: 数据接入（Xnode）
description: Xnode 节点、数据接入任务、Job 分片与 Agent 的 SQL 管理
toc_max_heading_level: 4
---

Xnode 是 TDengine 数据接入服务的分布式执行节点，负责运行数据同步任务，将外部数据源的数据实时或批量写入 TDengine。本文介绍管理数据接入功能的 SQL 命令，包括 Xnode 节点、数据接入任务、Job 分片以及 Agent。

任务自动恢复由守护进程 `xnoded` 控制，可通过命令行参数 `--auto-resume` 或环境变量 `XNODED_AUTO_RESUME` 配置，默认开启。

## Xnode 节点管理

Xnode 节点是数据同步服务的基本执行单元，负责具体的数据传输工作。

### 创建节点

**语法**

```sql
CREATE XNODE 'url';

CREATE XNODE 'url'
  USER name
  PASS 'password';

CREATE XNODE 'url'
  TOKEN 'token';
```

**参数说明**

- **url**：Xnode 节点地址，格式为 `host:port`；端口为 taosX gRPC 端口（默认 `6055`）
- **name** / **password**：首次创建建议指定 `TOKEN`，或用户名与密码，供守护进程 `xnoded` 连接 `taosd`。未指定时将创建默认 token
- **token**：用于连接 `taosd` 的认证凭据

正常连接状态下，一个 taosX 实例只接受同一 TDengine 集群的 xnoded 控制连接。若该实例已被其他集群纳管，`CREATE XNODE` 会返回归属冲突错误；误注册不会中断原集群的控制连接。如需将 taosX 迁移到新集群，请先在原集群执行 `DROP XNODE`，再在新集群执行 `CREATE XNODE`。当前 xnoded 身份使用编译期静态 JWT 密钥，`cluster_id` 由客户端声明，因此该机制仅防误操作，不提供抵抗伪造身份或恶意抢占的安全保证。

**示例**

```sql
CREATE XNODE 'localhost:6055';

CREATE XNODE 'localhost:6055'
  USER root
  PASS 'taosdata';

CREATE XNODE 'localhost:6055'
  TOKEN 'C8V3o0ZVvYQ6sMEnjfixjtw0OvN9nIPFAL1HWvSKmHbQsds8vBpVbrEZn2hrzar';
```

### 修改认证

修改认证会重启守护进程 `xnoded`。本命令修改的是单个 `xnoded` 守护进程连接 `taosd` 所用的认证凭据。

```sql
ALTER XNODE SET USER name PASS 'password';

ALTER XNODE SET TOKEN 'token';
```

**参数说明**

- **token**：用于连接 `taosd` 的认证凭据

**示例**

```sql
ALTER XNODE SET TOKEN 'C8V3o0ZVvYQ6sMEnjfixjtw0OvN9nIPFAL1HWvSKmHbQsds8vBpVbrEZn2hrzar';

ALTER XNODE SET USER root PASS 'taosdata';
```

### 重启守护进程

重启当前 mnode leader 所在 dnode 上的守护进程 `xnoded`。该命令不会重启远端 `taosx` 数据面进程。

该操作需要 `NODE ALTER` 权限，会短暂中断 `xnoded` 守护进程，请勿在短时间内高频执行。

**语法**

```sql
RESTART XNODE
```

**示例**

```sql
RESTART XNODE;
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

如需将 taosX 实例注册到其他 TDengine 集群，必须先通过原集群执行 `DROP XNODE` 解除归属。

## 任务管理

任务（Task）定义了数据同步的源端、目标端以及数据解析规则。

### 创建任务

数据接入任务分为两类：

- **普通任务**：将 MQTT、Kafka 等外部数据源的数据写入 TDengine。
- **双活任务**：从 v3.4.3.12 起支持，将源数据库的数据复制到目标数据库，可用于构建双活方案。

两类任务均通过 `CREATE XNODE TASK` 创建。创建双活任务时需指定 `WITH TYPE 'replica'`，省略该选项则创建普通任务。

#### 普通任务

**语法**

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
| `TYPE`     | 从 v3.4.3.12 起支持。只接受 `replica`（不区分大小写），省略时创建普通任务；创建后不能修改 |

**示例**

```sql
CREATE XNODE TASK 't4'
  FROM 'kafka://localhost:9092?topics=abc&group=abcgroup'
  TO 'taos+ws://localhost:6041/test'
  WITH PARSER '{"model":{"name":"cc_abc","using":"cc","tags":["g"],"columns":["ts","b"]},"mutate":[{"map":{"ts":{"cast":"ts","as":"TIMESTAMP(ms)"},"b":{"cast":"a","as":"VARCHAR"},"g":{"value":"1","as":"INT"}}}]}';
```

#### 双活任务

从 v3.4.3.12 起，可通过 `WITH TYPE 'replica'` 创建双活任务，由 xnoded 调度。每个任务将源数据库的数据复制到目标数据库；在另一集群创建反向任务，可构建双活方案。

**前置条件**

- 管理集群、xnoded 和 taosX 均已升级到支持本语法的版本。
- 源端和目标端均为 v3.4.0.0 或更高版本。
- 两端数据库已存在且精度相同，源库的 `wal_retention_period > 0`。
- 源端和目标端满足双活授权要求。

**DSN 参数**

创建双活任务时，通过 `FROM`、`TO` 指定 DSN；修改时，通过 `ALTER XNODE TASK` 更新相应 DSN。DSN 仅接受下列连接信息和参数，未识别的参数会被拒绝。

**连接信息**

以下连接信息适用于 `FROM` 和 `TO`。驱动、协议、地址列表、端口和库名创建后不能修改；其余连接信息修改后在下次启动任务时生效。

| 参数 | 说明 |
| --- | --- |
| 驱动、协议、地址列表、端口和库名 | 创建时必须按源端和目标端的要求提供；创建后不能修改 |
| 用户名、密码 | 创建时可以指定；可以修改，下次启动生效 |
| `token`、`bearer_token`、`td.connect.token` | 创建时可以指定；可以修改，下次启动生效 |
| `tls_mode`、`tls_version`、`tls_ca`、`conn_mode` | 创建时可以指定；可以修改，下次启动生效 |

**订阅起点参数**

以下参数仅适用于 `FROM`，只能在创建任务时设置。修改 DSN 时，已指定的参数不能删除或改值，未指定的参数也不能补充，即使补充的值与默认值相同也会被拒绝。

| 参数 | 说明 |
| --- | --- |
| `auto.offset.reset` | 消费组订阅的初始位置，默认 `earliest`。可选 `earliest` 或 `latest`，仅在没有已提交位点时生效；已有位点时从该位点续传 |
| `experimental.snapshot.enable` | 默认 `true`。`true` 表示先复制已落盘数据，再消费 WAL；`false` 表示只消费 WAL |
| `msg.consume.start.ts` | 订阅同步开始时间，默认 `0`。只复制主键时间 `ts` 不早于该值的数据记录；取值须为非负整数形式的 Unix 毫秒时间戳，`0` 表示不过滤 |

**运行参数**

以下参数仅适用于 `FROM`，可在创建任务时设置，也可在修改 DSN 时调整。修改在下次启动任务时生效，创建和修改时都会检查参数取值及组合限制。详细定义见 [数据订阅编程接口](../../10-developer-guide/07-subscription-api.md#创建参数)。

| 参数 | 说明 |
| --- | --- |
| `write_concurrency` | 写入并发调优参数 |
| `num.of.writers` | 写入并发调优参数 |
| `commit.chunk.size` | taosX 位点提交调优参数 |
| `commit.interval.ms` | taosX 位点提交调优参数，用于调整提交间隔 |
| `max.polling.timeout` | 消费轮询调优参数 |
| `enable.concurrent.polling` | 消费轮询调优参数 |
| `refresh.progress.interval` | 进度刷新调优参数 |
| `enable.auto.commit` | 是否启用消费位点自动提交。仅允许 `false`，由 taosX 在目标写入成功后提交位点 |
| `auto.commit.interval.ms` | 消费位点自动提交的时间间隔。允许透传，但由于自动提交已关闭，此参数不生效 |
| `enable.replay` | 是否开启数据回放功能。仅允许 `false`，WAL 按时回放不适用于数据库复制及续传 |
| `msg.consume.rawdata` | 消费数据时拉取二进制类型的 WAL 原始载荷。默认 `0` 表示消费解码数据；非零时 `msg.consume.start.ts` 必须未设置或为 `0` |
| `msg.with.table.name` | 是否允许从消息中解析表名。允许透传 |
| `msg.enable.batchmeta` | 是否启用服务端批量元数据返回。允许透传 |
| `session.timeout.ms` | consumer 心跳丢失后的超时时间。允许透传 |
| `max.poll.interval.ms` | consumer poll 拉取数据的最长时间间隔。允许透传 |
| `fetch.max.wait.ms` | 服务端单次返回数据的最大耗时。允许透传 |
| `min.poll.rows` | 服务端单次返回数据的最小条数。允许透传 |

**系统保留参数**

以下参数由系统设置或自动生成，在 `FROM` 和 `TO` 中均不能手工指定，创建和修改任务时都会检查。

| 参数 | 说明 |
| --- | --- |
| `replica`、`msg.consume.excluded` | 由系统设置 |
| `timeout`、`prefer`、`enable.wal.marker` | 由系统设置 |
| `group.id`、`use.topic.name`、`client.id` | 由系统自动生成 |

参数名先经过 URL 解码，再校验，因此 `%72eplica` 也属于保留参数。创建或修改普通任务时，源端 DSN 同样不能使用 `replica` 和 `msg.consume.excluded`。

**使用说明**

`FROM` 必须是 `tmq` DSN，`TO` 必须是 `taos` DSN，路径指定库名；不支持 `FROM DATABASE`、`FROM TOPIC`、`TO DATABASE`、`PARSER`（含转换配置）或 `VIA`。创建后任务处于停止状态。

同一管理集群内，每组源地址、源库到目标地址、目标库的映射只能创建一个双活任务。任务停止或处于 `deleting`、`delete_failed` 状态时，仍占用该映射，重复执行 `CREATE XNODE TASK` 会报错。反向映射、不同目标库或不同目标集群可以分别创建任务。检查映射是否重复时，同一地址使用 Native 或 WebSocket 驱动均视为相同；管理员还需避免通过地址别名或在不同管理集群中创建重复任务。

每个任务独占一个 topic 和一个消费组，两者均命名为 `__replica__<管理集群ID>_<task id>`。`group.id`、`use.topic.name`、`client.id` 由系统自动生成，不能手工指定。不同映射不能共用消费组，否则源库的 vgroup 会分配给不同任务，导致各目标只收到部分数据。

相关错误码、出错原因及处理建议见 [错误码](../../10-developer-guide/09-error-codes.md#xnode)。进度查询、WAL 保留限制、故障监控及从 v3.3 迁移的方法见 [双活方案](../../12-operations-and-tooling/02-operations/12-ha/03-dual.md)。

**示例**

```sql
CREATE XNODE TASK 'aa_power_a_b'
  FROM 'tmq+ws://root:taosdata@td1:6041/power'
  TO 'taos+ws://root:taosdata@td2:6041/power'
  WITH TYPE 'replica';

START XNODE TASK 'aa_power_a_b';
```

### 查看任务

**语法**

```sql
SHOW XNODE TASKS [WHERE condition]
```

从 v3.4.3.12 起，结果末尾包含 `type` 列，取值为 `normal`（普通任务）或 `replica`（双活任务）；`information_schema.ins_xnode_tasks` 同样提供该列。可通过 `SHOW XNODE TASKS WHERE type = 'replica';` 查询双活任务。`desired_state` 列表示用户期望的任务状态，`status` 和 `reason` 列分别表示任务的实际执行状态和最近一次失败原因。

`parser` 列使用 BLOB 返回完整任务配置。在 `WHERE` 条件中，`parser` 支持关系比较、`IS NULL`、`IS NOT NULL`、`LIKE`、`NOT LIKE`、`MATCH`/`REGEXP`、`NMATCH`、`IN`、`NOT IN`、`BETWEEN`、`NOT BETWEEN` 及其逻辑组合；不支持字符串函数、JSON 运算、算术和位运算。其他列支持的运算由各自的数据类型决定。

```sql
SHOW XNODE TASKS WHERE parser != '{}' AND parser IS NOT NULL;
```

**示例**

```sql
SHOW XNODE TASKS \G;
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

xnoded 启动或重连时，如果发现双活任务的 DSN 无效，会保存 `status=failed` 和 `desired_state=stopped`，并在 `reason` 中记录失败原因。该任务及其子任务不会自动恢复。可执行 `SHOW XNODE TASKS` 查看状态和原因；修正 DSN 后，需手动执行 `START XNODE TASK` 恢复任务。

**示例**

```sql
START XNODE TASK 1;
```

### 停止任务

**语法**

```sql
STOP XNODE TASK {id | 'name'}
```

**双活任务说明**：停止双活任务时会提交已成功写入目标端的位点，再次启动时从该位点续传。

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

**双活任务说明**

任务类型在创建后不能修改。修改双活任务的 DSN 时，应遵循以下规则：

- 驱动、协议、地址列表、端口和库名不能修改。比较连接目标时，主机名不区分大小写，端口按原样比较，不补默认端口；从 Native 改为 WebSocket 也属于修改连接目标。
- 订阅起点参数中，已指定的参数不能删除或改值，未指定的参数也不能补充，即使补充的值与默认值相同也不允许。
- 凭据和允许修改的运行参数可以调整，下次启动任务时生效。运行中的任务需要先执行 `STOP XNODE TASK`，再执行 `START XNODE TASK`，以应用变更。

**示例**

```sql
ALTER XNODE TASK 3 FROM 'pulsar://localhost:6650' TO 'testdb' WITH XNODE_ID 33 VIA 333 REASON 'zgc_test';
```

### 删除任务

**语法**

```sql
DROP XNODE TASK {id | 'name'}
```

**删除行为说明**

普通任务采用同步删除，双活任务采用异步删除。运行中的任务也可直接删除。

**普通任务说明**

`DROP XNODE TASK` 返回成功时，任务记录已删除，不再出现在 `SHOW XNODE TASKS` 中。xnoded 随后在后台停止该任务并删除其子任务。如果任务所在节点暂时不可用，xnoded 会在节点恢复连接后停止残留的任务进程。普通任务不会进入 `deleting` 或 `delete_failed` 状态。

**双活任务说明**

删除双活任务时，除停止任务、删除子任务外，xnoded 还需清理源端自动生成的 topic、消费组及位点。源端可能暂时不可达，因此双活任务采用异步删除。

`DROP XNODE TASK` 返回成功时，表示删除请求已受理。mnode 先保存 `desired_state=stopped` 和 `status=deleting`，xnoded 完成清理并报告后，mnode 才删除任务记录。清理期间，任务仍出现在 `SHOW XNODE TASKS` 中。

如果请求未能发送到 xnoded，即使 `DROP XNODE TASK` 报错，任务也可能已进入 `deleting` 状态。xnoded 恢复可用后，会检查任务状态并继续删除。可执行 `SHOW XNODE TASKS` 查看状态，并再次执行 `DROP XNODE TASK` 重试。处于 `deleting` 或 `delete_failed` 状态的任务不能启动。

topic 没有其他消费组时，使用 `DROP TOPIC IF EXISTS FORCE` 删除；有其他消费组时，使用 `DROP CONSUMER GROUP IF EXISTS FORCE`，只清理本任务的消费组。资源已不存在时，也视为清理成功。

遇到临时连接失败，xnoded 会退避重试，重启后也会从 mnode 恢复清理。遇到权限或凭据等永久错误，任务会进入 `delete_failed` 状态，该状态和 `reason` 会被持久化保存，重启后不会自动继续清理。修复错误后，需重新执行 `DROP XNODE TASK`。

双活任务清理完成前不能重建相同映射。清理完成后重新创建任务，会生成新的标识和位点，不继承旧进度。

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
SHOW XNODE JOBS \G;
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

CREATE XNODE AGENT 'a2'
  WITH STATUS 'running';
```

### 查询 Agent

**语法**

```sql
SHOW XNODE AGENTS [WHERE condition]
```

**示例**

```sql
SHOW XNODE AGENTS \G;
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
