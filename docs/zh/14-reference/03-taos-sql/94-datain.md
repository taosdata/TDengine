---
sidebar_label: 数据接入
title: 数据接入
description: "Xnode 分布式节点管理和任务管理说明"
---

# 数据同步 SQL 手册

本文档介绍用于管理 TDengine 数据同步功能的 SQL 命令，包括 Xnode 节点、同步 Task 任务以及 Job 分片的管理。

## XNODE 节点管理

XNODE 节点是数据同步服务的基本执行单元，负责具体的数据传输工作。

### 创建节点

**语法**

```sql
CREATE XNODE 'url'
CREATE XNODE 'url' USER name PASS 'password';
```

**参数说明**

- **url**: Xnode 节点的地址，格式为 `host:port`
- 首次创建需要指定用户名和密码，用于 xnoded 连接 taosd

**示例**

```
taos> CREATE XNODE "h1:6050";
Create OK, 0 row(s) affected (0.050798s)

taos> CREATE XNODE 'x1:6050' USER root PASS 'taosdata';
Create OK, 0 row(s) affected (0.050798s)
```

### 查看节点

**语法**

```sql
SHOW XNODES
```

**示例**

```sql
taos> SHOW XNODES;
```

输出结果：

```
id | url     | status | create_time                 | update_time             |
===============================================================================
1  | h1:6050 | online | 2025-12-14 01:01:34.655     | 2025-12-14 01:01:34.655 |
Query OK, 1 row(s) in set (0.005518s)
```

### 清理节点

将一个节点已有任务重新分配到其他节点中执行。

**语法**

```sql
DRAIN XNODE id
```

**参数说明**

- **id**: Xnode 节点的 ID

**示例**

```sql
taos> DRAIN XNODE 4;
Query OK, 0 row(s) affected (0.014246s)
```

### 删除节点

**语法**

```sql
DROP XNODE [FORCE] id | 'url'
```

**参数说明**

- **id**: Xnode 节点的 ID
- **url**: Xnode 节点的地址
- **FORCE**: 强制删除节点

**示例**

```sql
taos> DROP XNODE 1;
Drop OK, 0 row(s) affected (0.038173s)

taos> DROP XNODE "h2:6050";
Drop OK, 0 row(s) affected (0.038593s)
```


## TASK 任务管理

TASK 任务定义了数据同步的源端、目标端以及数据解析规则。

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
```

语法说明：task_options 各选项可同时使用，空格分隔，顺序无关

**参数说明**

| 参数              | 说明                                           |
| :--------------- | :--------------------------------------------- |
| **name**         | 任务名称                                       |
| **from_dns**     | 源端连接字符串（如 `mqtt://...`）              |
| **dbname**       | 数据库名称                                     |
| **topic**        | Topic 名称                                     |
| **to_dns**       | 目标端连接字符串（如 `taos://...`）            |
| **parser**       | 数据解析配置（JSON 格式）                      |
| **status**       | 任务状态                                        |
| **xnodeId**     | 任务所在的 xnode 节点 ID                         |
| **viaId**          | 任务所在的 agent 的 ID                           |
| **reason**       | 任务最近执行失败原因                               |

**示例**

```sql
taos> CREATE XNODE TASK "t4" FROM 'kafka://localhost:9092?topics=abc&group=abcgroup' TO 'taos+ws://localhost:6041/test' WITH parser '{"model":{"name":"cc_abc","using":"cc","tags":["g"],"columns":["ts","b"]},"mutate":[{"map":{"ts":{"cast":"ts","as":"TIMESTAMP(ms)"},"b":{"cast":"a","as":"VARCHAR"},"g":{"value":"1","as":"INT"}}}]}';
Create OK, 0 row(s) affected (0.038959s)
```

### 查看任务

**语法**

```sql
SHOW XNODE TASKS;
```

**示例**

```sql
taos> SHOW XNODE TASKS;
```

输出结果：

```
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
create_time: 2025-12-29 13:48:21.058
update_time: 2025-12-29 13:48:21.058
Query OK, 1 row(s) in set (0.005281s)
```

### 启动任务

**语法**

```sql
START XNODE TASK id | 'name';
```

**示例**

```sql
taos> START XNODE TASK 1;
DB error: Xnode url response http code not 200 error [0x8000800C] (0.002160s)
```

### 停止任务

**语法**

```sql
STOP XNODE TASK id | 'name';
```

**示例**

```sql
taos> STOP XNODE TASK 1;
DB error: Xnode url response http code not 200 error [0x8000800C] (0.002047s)
```

### 修改任务

**语法**

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
```

语法说明：task_options 各选项含义与创建任务相同

**示例**

```sql
taos> ALTER XNODE TASK 3 FROM 'pulsar://zgc...' TO 'testdb' WITH xnode_id 33 via 333 reason 'zgc_test';
Query OK, 0 row(s) affected (0.036077s)
```

### 删除任务

**语法**

```sql
DROP XNODE TASK id | 'name';
```

**示例**

```sql
taos> DROP XNODE TASK 3;
Drop OK, 0 row(s) affected (0.038191s)
```



## JOB 任务分片管理

JOB 是 TASK 任务的执行分片，支持手动和自动负载均衡。

### 查看 JOB 分片

**语法**

```sql
SHOW XNODE JOBS;
```

**示例**

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



### 手动负载均衡

**语法**

```sql
REBALANCE XNODE JOB jid WITH XNODE_ID xnodeId;
```

语法说明：手动负载均衡当前只支持 xnode_id 参数，必须附带 xnode id 信息。

**示例**

```sql
taos> REBALANCE XNODE JOB 1 WITH xnode_id 1;
Query OK, 0 row(s) affected (0.011808s)
```

### 自动负载均衡

**语法**

```sql
REBALANCE XNODE JOBS [ WHERE job_conditions ]
```

语法说明：WHERE job_conditions 可选，是用来过滤符合条件的 job 数据。不支持函数，支持 SHOW XNODE JOBS 命令中出现的所有字段。没有 WHERE 条件语句时表示所有 job 均进行自动负载均衡。

**示例**

```sql
taos> REBALANCE XNODE JOBS WHERE id>1;
Query OK, 0 row(s) affected (0.014246s)

taos> REBALANCE XNODE JOBS WHERE task_id=1 and (xnode_id=3 or xnode_id=4);
Query OK, 0 row(s) affected (0.007237s)

taos> REBALANCE XNODE JOBS;
Query OK, 0 row(s) affected (0.023245s)
```
