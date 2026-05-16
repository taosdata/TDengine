# 数据管道工具-Function Spec

## 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2025-01-07 | 2025-01-07 | 0.1 | 霍琳贺 | 根据需求文档整理初版 |
| 2025-01-10 | 2025-01-10 | 1.0 | 霍琳贺 | 完成 1.0 版本定稿 |
| 2026-02-06 | 2025-02-06 | 1.1 | 霍琳贺 | 升级为高可用架构版本，增加 XNode、MNode、SQL 管理接口等 |

## 背景

taosX 是一个易于使用、功能丰富的 TDengine 数据管道工具。它是数据源和数据目标之间的桥梁，支持离线数据导入/导出和从或到 TDengine 实例的实时数据复制。它为性能、可靠性、生产力、可观察性和人体工程学而构建。
在高可用架构下，taosX 已集成到 TDengine 平台，成为 TDengine 平台的数据同步组件，并重命名为 XNode 组件。通过复用 TDengine 的 MNode 保证数据一致性并进行主体任务的调度，实现了分布式高可用架构。
其功能特点包括：
- 基于订阅的数据库或表的复制。
- 从一个版本到另一个版本的数据库或表的数据迁移。
- 数据库或（超级）表的增量备份/恢复。
- 离线数据文件的导出或导入，目前支持 CSV 和 Parquet。
- TDengine 的双活部署和管理。
- 基于 SQL 的数据接入任务管理。
- 分布式架构支持多节点部署，实现负载均衡和故障自动迁移。
- 外部数据源的导入管道，包括：
  - 关系型数据库：MySQL、Oracle、PostgreSQL、Microsoft SQL Server。
  - 时序数据库：OpenTSDB、InfluxDB。
  - 工业实时数据库：PI System、Aveva Historian。
  - 消息队列：Kafka。
  - 其他常见协议：OPC-UA/DA、MQTT。

## 定义

- **双活**：指两套 TDengine 集群，按照既定的数据同步方式，同时提供服务。
- **订阅**：指 TDengine 基于 TMQ 消息队列 API 接收消息的方式。
- **数据迁移**：一般指从 TDengine 较旧的版本迁移数据到新版本。
- **DSN**：Data Source Name，一个数据源的字符串表示。
- **XNode**：taosX 的高可用分布式节点，是 taosX 在 TDengine 集群中的工作节点。
- **MNode**：TDengine 的管理节点，在高可用架构中负责 taosX 元数据存储和任务调度。
- **xnoded**：运行在 MNode Leader 节点上的守护进程。
- **Shard（分片）**：在高可用架构中，一个任务可被划分为多个子任务，分布在不同 XNode 上并行执行。
- **Job（分片任务）**：任务分片后在 XNode 上执行的具体工作单元。
- **DRAIN 模式**：节点的一种状态，表示该节点不再接收新的任务执行请求，已有任务将迁移到其他节点。

## 行为说明

### 4.1 组件

高可用架构下，taosX 包含以下组件：
- **XNode**：`taosx` 可执行文件作为工作节点运行，接收 MNode 调度执行具体任务分片。
- **MNode**：TDengine 的管理节点，存储 taosX 元数据，负责任务分片和负载均衡调度。
- **xnoded**：运行在 MNode Leader 上的守护进程，管理 XNode 节点状态和任务调度。
- **taosx-agent**：代理服务，用于网络安全或 OS 隔离环境下的数据接入。

### 4.2 安装

#### 4.3 XNode 服务

XNode 随 TDengine 企业版安装。安装后，有以下组件：
- `taosx` 可执行文件。
- taosX 独立数据源插件：包括 taosx-influxdb、taosx-opentsdb、taosx-opc。
- `taosx` 服务。
  - Linux 上，使用 systemd 服务单元：`/etc/systemd/system/taosx.service`
  - Windows 上，使用 [WinSW](https://github.com/winsw/winsw) 安装和启动 `taosx` 服务。

#### 4.4 Agent 服务

Agent 需要单独部署安装。安装后，有以下组件：
- `taosx-agent` 可执行文件。
- `taosx-agent` 服务。
  - Linux 上，使用 systemd 服务单元：`/etc/systemd/system/taosx-agent.service`
  - Windows 上，使用 [WinSW](https://github.com/winsw/winsw) 安装和启动 `taosx-agent` 服务。

### 4.3 配置文件

`taosx 使用 taosx.toml 配置文件可自定义数据文件目录、日志目录、日志级别等。`
```shell {wrap}

## data dir

data_dir = "/var/lib/taos/taosx"
#data_dir = "C:\\TDengine\\data\\taosx" # on windows

## number of threads used for tokio workers, default to 0 (means cores * 2)

#jobs = 0

## enable OpenTelemetry tracing and metrics exporter

#otel = false

## server instance id

##

## The instanceId of each instance is unique on the host

instanceId = 16

[telemetry]

## server = "telemetry.taosdata.com"

## port = "80"

[serve]

## listen to ip:port address

#listen = "0.0.0.0:6050"

## TLS/SSL certificate

#ssl_cert = "/path/to/tls/server.pem"

## TLS/SSL certificate key

#ssl_key = "/path/to/tls/server.key"

## TLS/SSL CA certificate

#ssl_ca = "/path/to/tls/ca.pem"

## database url

#database_url = "sqlite:taosx.db"

## default global request timeout which unit is second. This parameter takes effect for certain interfaces that require a timeout setting

#request_timeout = 30

## GRPC listen address，use ip:port like `0.0.0.0:6055`.

##

## When use this in explorer, please set explorer grpc configuration to **Public** IP or

## FQDN with correct port, which might be changed exposing to Public network.

##

## - Example 1: "http://192.168.111.111:6055" 

## - Example 2: "http://node1.company.domain:6055" 

##

## Please also make sure the above address is not blocked if firewall is enabled.

##
#grpc = "0.0.0.0:6055"

## number of threads used for rest api service, default to 0 (means cores * 2)

#rest_api_threads = 0

## number of threads used for grpc service, default to 0 (means cores * 2)

#grpc_threads = 0

## number of threads used for scheduler service, default to 0 (means cores * 2)

#scheduler_threads = 0

[monitor]

## FQDN of taosKeeper service, no default value

fqdn = "ha"

## Port of taosKeeper service, default 6043

port = 6043

## How often to send metrics to taosKeeper, default every 10 seconds. Only value from 1 to 10 is valid.

interval = 10

## log configuration

[log]

## All log files are stored in this directory

##
path = "/var/log/taos"
#path = "C:\\TDengine\\log" # on windows

## log filter level

##
#level = "info"

## Compress archived log files or not

##
#compress = false

## The number of log files retained by the current explorer server instance in the `path` directory

##
#rotationCount = 30

## Rotate when the log file reaches this size

##
#rotationSize = "1GB"

## Log downgrade when the remaining disk space reaches this size, only logging `ERROR` level logs

##
#reservedDiskSize = "1GB"

## The number of days log files are retained

##
#keepDays = 30

## Watching the configuration file for log.loggers changes, default to true.

##
#watching = true

## Customize the log output level of modules, and changes will be applied after modifying the file when log.watching is enabled

##

## ## Examples:

##

## crate = "error"

## crate::mod1::mod2 = "info"

## crate::span[field=value] = "warn"

##
[log.loggers]
#"actix_server::accept" = "warn"
#"taos::query" = "warn"
```

全部配置项包含：
配置项名称说明：
- `data_dir`：数据文件存放目录。
- `instanceId`：当前 taosX 服务的实例 ID，如果同一台机器上启动了多个 taosX 实例，必须保证各个实例的实例 ID 互不相同。
- `logs_home`：日志文件存放目录，`taosX` 日志文件的前缀为 `taosx.log`，外部数据源有自己的日志文件名前缀。已弃用，请使用 `log.path` 代替。
- `log_level`：日志等级，可选级别包括 `error`、`warn`、`info`、`debug`、`trace`，默认值为 `info`。已弃用，请使用 `log.level` 代替。
- `log_keep_days`：日志的最大存储天数，`taosX` 日志将按天划分为不同的文件。已弃用，请使用 `log.keepDays` 代替。
- `jobs`：程序默认运行时的线程数。默认线程数为`当前服务器内核*2`。
- `serve.listen`：是 `taosX` REST API 监听地址，默认值为 `0.0.0.0:6050`。支持 IPv6 协议的地址，同时支持多地址，多地址需保证端口一致，且使用英文逗号，作为分割。
- `serve.ssl_cert`：是 SSL/TLS 证书。
- `serve.ssl_key`：是 SSL/TLS 秘钥。
- `serve.ssl_ca`：是 SSL/TLS 根证书。
- `serve.request_timeout`：全局接口 API 超时时间。
- `serve.grpc`：是 `taosX` gRPC 服务监听地址，默认值为 `0.0.0.0:6055`。支持 IPv6 协议的地址，同时支持多地址，多地址需保证端口一致，且使用英文逗号，作为分割。
- `rest_api_threads`：rest api 服务的运行时线程数。默认线程数为`当前服务器内核*2`。
- `grpc_threads`：grpc 服务的运行时线程数。默认线程数为`当前服务器内核*2`。
- `scheduler_threads`：scheduler 任务服务的运行时线程数。默认线程数为`当前服务器内核*2`。
- `monitor.fqdn`：`taosKeeper` 服务的 FQDN，没有默认值，置空则关闭监控功能。
- `monitor.port`：`taosKeeper` 服务的端口，默认`6043`。
- `monitor.interval`：向 `taosKeeper` 发送指标的频率，默认为每 10 秒一次，只有 1 到 10 之间的值才有效。
- `log.path`：日志文件存放的目录。
- `log.level`：日志级别，可选值为 "error"、"warn"、"info"、"debug"、"trace"。
- `log.compress`：日志文件滚动后的文件是否进行压缩。
- `log.rotationCount`：日志文件目录下最多保留的文件数，超出数量的旧文件被删除。
- `log.rotationSize`：触发日志文件滚动的文件大小（单位为字节），当日志文件超出此大小后会生成一个新文件，新的日志会写入新文件。
- `log.reservedDiskSize`：日志所在磁盘停止写入日志的阈值（单位为字节），当磁盘剩余空间达到此大小后停止写入日志。
- `log.keepDays`：日志文件保存的天数，超过此天数的旧日志文件会被删除。
- `log.watching`：是否对日志文件中 `log.loggers` 配置内容的变更进行监听并尝试重载。
- `log.loggers`：指定模块的日志输出级别，格式为 `"modname" = "level"`，同时适配 tracing 库语法，可以根据 `modname[span{field=value}]=level`，其中 `level` 为日志级别。

### 4.4 SQL 管理命令

高可用架构下，所有管理操作通过 TDengine SQL 命令完成。
XNODE 节点是数据同步服务的基本执行单元，负责具体的数据传输工作。TASK 任务定义了数据同步的源端、目标端以及数据解析规则。JOB 是 TASK 任务的执行分片，支持手动和自动负载均衡。Agent 节点是数据同步服务中的采集与转发单元，负责采集数据，并将采集到的数据转发至 Xnode 节点。

#### 4.4.1 XNode 节点管理

1. 创建节点
**语法：**
```sql {wrap}
CREATE XNODE 'url'
CREATE XNODE 'url' USER name PASS 'password'
CREATE XNODE 'url' TOKEN 'token'
```

**参数说明：**

| 参数 | 说明 |
| --- | --- |
| **url** | Xnode 节点的地址，格式为 host:port，端口号为 taosx GRPC 端口（默认 6055） |
| **name** | 用户名，用于守护进程 xnoded 连接 taosd |
| **password** | 密码，用于守护进程 xnoded 连接 taosd |
| **token** | 用于连接 taosd 认证 |

**说明：**
- 首次创建建议指定 token 或者用户名和密码
- 如果未指定 token 或者用户名密码，则创建默认 token
- 该用户专门用于 XNode 与 MNode 通信和向 TDengine 写入数据
**示例：**
```sql {wrap}
CREATE XNODE "h1:6055";

CREATE XNODE 'x1:6055' USER root PASS 'taosdata';

CREATE XNODE 'x2:6055' TOKEN 'C8V3o0ZVvYQ6sMEnjfixjtw0OvN9nIPFAL1HWvSKmHbQsds8vBpVbrEZn2hrzar';
```

1. 修改认证
修改认证会重启守护进程 xnoded。
**语法：**
```sql {wrap}
ALTER XNODE SET USER name PASS 'password'
ALTER XNODE SET TOKEN 'token'
```

**参数说明：**

| **参数** | **说明** |
| --- | --- |
| **token** | 用于连接 taosd 认证 |

**示例：**
```sql {wrap}
ALTER XNODE SET TOKEN 'C8V3o0ZVvYQ6sMEnjfixjtw0OvN9nIPFAL1HWvSKmHbQsds8vBpVbrEZn2hrzar';

ALTER XNODE SET USER root PASS 'taosdata';
```

1. 查看节点
**语法：**
```sql {wrap}
SHOW XNODES [WHERE condition]
```

**返回字段：**

| **字段** | **说明** |
| --- | --- |
| **id** | XNode ID |
| **url** | 通信地址 |
| **status** | 节点状态（online/offline/drain） |
| **create_time** | 创建时间 |
| **update_time** | 最近更新时间 |

**示例：**
```sql {wrap}
SHOW XNODES;
```

1. 排空节点
将一个节点已有任务重新分配到其他节点中执行。
**语法：**
```sql {wrap}
DRAIN XNODE id
```

**参数说明：**

| **参数** | **说明** |
| --- | --- |
| **id** | Xnode 节点的 ID |

**示例：**
```sql {wrap}
DRAIN XNODE 4;
```

1. 删除节点
**语法：**
```sql {wrap}
DROP XNODE [FORCE] id | 'url'
```

**参数说明：**

| **参数** | **说明** |
| --- | --- |
| **id** | Xnode 节点的 ID |
| **url** | Xnode 节点的地址 |
| **FORCE** | 强制删除节点 |

**示例：**
```sql {wrap}
DROP XNODE 1;

DROP XNODE "h2:6050";
```

#### 4.4.2 Task 任务管理

1. 创建任务
**语法：**
```sql {wrap}
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

语法说明：task_options 各选项可同时使用，空格分隔，顺序无关。
**参数说明：**

| **参数** | **说明** |
| --- | --- |
| **name** | 任务名称 |
| **from_dns** | 源端连接字符串（如 mqtt://...） |
| **dbname** | 数据库名称 |
| **topic** | Topic 名称 |
| **to_dns** | 目标端连接字符串（如 taos://...） |
| **parser** | 数据解析配置（JSON 格式） |
| **status** | 任务状态 |
| **xnodeId** | 任务所在的 xnode 节点 ID |
| **viaId** | 任务所在的 agent 的 ID |
| **reason** | 任务最近执行失败原因 |
| **labels** | 任务标签，使用 JSON 字符串 |

**示例：**
```sql {wrap}
CREATE XNODE TASK "t4"
  FROM 'kafka://localhost:9092?topics=abc&group=abcgroup'
  TO 'taos+ws://localhost:6041/test'
  WITH PARSER '{
    "model":{
      "name":"cc_abc",
      "using":"cc",
      "tags":["g"],
      "columns":["ts","b"]
    },
    "mutate":[{
      "map":{
        "ts":{"cast":"ts","as":"TIMESTAMP(ms)"},
        "b":{"cast":"a","as":"VARCHAR"},
        "g":{"value":"1","as":"INT"}
      }
    }]
  }';
```

数据导出任务示例：
```sql {wrap}
-- 订阅导出
CREATE XNODE TASK 'tmq_export'
  FROM TOPIC 'topic1'
  TO 'kafka://broker:9092';

-- 数据库导出
CREATE XNODE TASK 'db_export'
  FROM DATABASE 'db1'
  TO 'local:/backup/db1';
```

1. 查看任务
**语法：**
```sql {wrap}
SHOW XNODE TASKS [WHERE condition]
```

**返回字段：**

| **字段** | **说明** |
| --- | --- |
| **id** | 任务 ID |
| **name** | 任务名称 |
| **from** | 数据源 DSN |
| **to** | 数据目标 |
| **parser** | 解析器配置 |
| **via** | Agent ID |
| **xnode_id** | XNode ID |
| **status** | 任务状态 |
| **reason** | 失败原因 |
| **created_by** | 创建用户 |
| **labels** | 任务标签 |
| **create_time** | 创建时间 |
| **update_time** | 更新时间 |

**示例：**
```sql {wrap}
SHOW XNODE TASKS;
```

1. 启动任务
**语法：**
```sql {wrap}
START XNODE TASK id | 'name'
```

**示例：**
```sql {wrap}
START XNODE TASK 1;

START XNODE TASK 't4';
```

1. 停止任务
**语法：**
```sql {wrap}
STOP XNODE TASK id | 'name'
```

**示例：**
```sql {wrap}
STOP XNODE TASK 1;

STOP XNODE TASK 't4';
```

1. 修改任务
**语法：**
```sql {wrap}
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

语法说明：alter_options 各选项含义与创建任务相同。
**示例：**
```sql {wrap}
ALTER XNODE TASK 3
  FROM 'pulsar://zgc...'
  TO 'testdb'
  WITH xnode_id 33
       via 333
       reason 'zgc_test';
```

1. 删除任务
**语法：**
```sql {wrap}
DROP XNODE TASK id | 'name'
```

**示例：**
```sql {wrap}
DROP XNODE TASK 3;
```

#### 4.4.3 Job 分片管理

1. 查看 Job 分片
**语法：**
```sql {wrap}
SHOW XNODE JOBS [WHERE condition]
```

**返回字段：**

| **字段** | **说明** |
| --- | --- |
| **id** | Job ID |
| **task_id** | 父任务 ID |
| **config** | 配置 JSON |
| **via** | Agent ID |
| **xnode_id** | 执行节点 ID |
| **status** | 分片状态 |
| **reason** | 失败原因 |
| **create_time** | 创建时间 |
| **update_time** | 更新时间 |

**示例：**
```sql {wrap}
SHOW XNODE JOBS;
```

输出结果：
```markdown {wrap}
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

1. 手动负载均衡
**语法：**
```sql {wrap}
REBALANCE XNODE JOB jid WITH XNODE_ID xnodeId
```

语法说明：手动负载均衡当前只支持 xnode_id 参数，必须附带 xnode id 信息。
**参数说明：**

| **参数** | **说明** |
| --- | --- |
| **jid** | Job 分片 ID |
| **xnodeId** | 目标 XNode ID |

**示例：**
```sql {wrap}
REBALANCE XNODE JOB 1 WITH xnode_id 1;
```

1. 自动负载均衡
**语法：**
```sql {wrap}
REBALANCE XNODE JOBS [ WHERE job_conditions ]
```

语法说明：WHERE job_conditions 可选，是用来过滤符合条件的 job 数据。不支持函数，支持 SHOW XNODE JOBS 命令中出现的所有字段。没有 WHERE 条件语句时表示所有 job 均进行自动负载均衡。
**示例：**
```sql {wrap}
REBALANCE XNODE JOBS WHERE id>1;

REBALANCE XNODE JOBS WHERE task_id=1 and (xnode_id=3 or xnode_id=4);

REBALANCE XNODE JOBS;
```

#### 4.4.4 Agent 管理

1. 创建 Agent
**语法：**
```sql {wrap}
CREATE XNODE AGENT 'name' [WITH agent_options]

agent_options:
  [STATUS 'status']
```

**参数说明：**

| **参数** | **说明** |
| --- | --- |
| **name** | Agent 节点的名称 |
| **status** | 使用 with 语句指定创建时的状态 |

**示例：**
```sql {wrap}
CREATE XNODE AGENT 'a1';

CREATE XNODE AGENT 'a2' WITH STATUS 'running';
```

1. 查询 Agent
**语法：**
```sql {wrap}
SHOW XNODE AGENTS [WHERE condition]
```

**返回字段：**

| **字段** | **说明** |
| --- | --- |
| **id** | Agent ID |
| **name** | Agent 名称 |
| **token** | 认证 Token |
| **status** | 状态 |
| **create_time** | 创建时间 |
| **update_time** | 更新时间 |

**示例：**
```sql {wrap}
SHOW XNODE AGENTS;
```

输出结果：
```markdown {wrap}
*************************** 1.row ***************************
         id: 1
       name: a1
      token: eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
     status: NULL
create_time: 2026-01-12 09:51:41.364
update_time: 2026-01-12 09:51:41.364
```

1. 更新 Agent
**语法：**
```sql {wrap}
ALTER XNODE AGENT agent_id WITH alter_options

alter_options {
  STATUS 'status'
  | NAME 'name'
}
```

**参数说明：**

| **参数** | **说明** |
| --- | --- |
| **name** | Agent 节点的名称 |
| **status** | 可以使用 with 语句指定创建时的状态 |

**示例：**
```sql {wrap}
ALTER XNODE AGENT 1 WITH NAME 'test1';

ALTER XNODE AGENT 'a2' WITH NAME 'test2' STATUS 'online';
```

1. 删除 Agent
**语法：**
```sql {wrap}
DROP XNODE AGENT agent_id
```

**参数说明：**

| **参数** | **说明** |
| --- | --- |
| **agent_id** | Agent 节点的 ID |

**示例：**
```sql {wrap}
DROP XNODE AGENT 1;
```

### 4.5 服务模式

XNode 支持以服务方式启动。在 Linux 上，启动方式为：
```sql {wrap}

## 启动服务

systemctl start taosx

## 停止服务

systemctl stop taosx

## 查看状态

systemctl status taosx
```

在 Windows 上，启动方式为：
```sql {wrap}

## 启动服务

sc start taosx

## 停止服务

sc stop taosx
```

#### 4.5.1 管理 API

Xnoded 与 xnode 使用 Arrow Flight gRPC 进行交互，这些 API 通过 do_exchange 双向流调用，定义如下。
1. 任务生命周期管理:

| API 名称 | 动作名 | 请求参数 | 响应 | 说明 |
| --- | --- | --- | --- | --- |
| plan_task | xnode_plan_task | HaTask { from, to, parser, via } | SplitJobResult | 任务规划/拆分 |
| start_task_job | xnode_start_task_job | StartTaskJobParam { task_id, job_id, from, to, parser, via } | () | 启动任务作业 |
| stop_task_job | xnode_stop_task_job | TaskJobId { task_id, job_id } | () | 停止任务作业 |
| drain | xnode_task_job_drain | () | () | 排空所有任务 |

1. 任务查询/预览

| API 名称 | 动作名 | 请求参数 | 响应 | 说明 |
| --- | --- | --- | --- | --- |
| list_task_job_states | xnode_list_task_job_states | () | ListTaskJobStatesResult | 列出所有任务作业状态 |
| task_preview | xnode_task_preview | TaskPreviewParam { from, parser, input } | Vec<ModeledJsonOutput> | 任务数据预览 |
| check_valid | xnode_check_valid | CheckValidParam { from, to, via } | DataSourceValidation | 验证数据源有效性 |
| get_samples | xnode_get_samples | GetSamplesParam { from, via } | serde_json::Value | 获取数据样本 |

1. Agent 管理

| API 名称 | 动作名 | 请求参数 | 响应 | 说明 |
| --- | --- | --- | --- | --- |
| add_agents | xnode_add_agents | Vec<String> (tokens) | () | 添加 Agents |
| del_agents | xnode_del_agents | Vec<i64> (agent_ids) | () | 删除 Agents |
| list_agents | xnode_list_agents | () | ListAgentsResult | 列出所有 Agents 状态 |

#### 4.5.2 Agent 交互 API

服务端 Agent 数据交互 API 使用 Arrow Flight gRPC 框架。
- Handshake
  - Arrow Flight API 接口：
  ```rust
      type HandshakeStream =
          Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send + Sync + 'static>>;
      async fn handshake(
          &self,
          req: Request<Streaming<HandshakeRequest>>,
      ) -> Result<Response<Self::HandshakeStream>, Status> {
          /* ... */
      }
  ```

  - 参数：
    - HTTP Header `x-version` ：Agent 版本。用于客户端兼容性检查。
    - Hanshake Payload：Agent Token 字符串。
  - 客户端使用：
  ```rust
  let channel = Endpoint::try_from("http://127.0.0.1:6055")?.connect().await?;
  let mut client = FlightServiceClient::new(channel);
  client.add_header("x-version", crate::build::PKG_VERSION)?;
  client.handshake(token.to_string()).await?;
  ```

- Agent 任务状态推送 API
  - Arrow Flight API 接口：
  ```rust
      type DoActionStream =
          Pin<Box<dyn Stream<Item = Result<arrow_flight::Result, Status>> + Send + Sync + 'static>>;
  
      async fn do_action(
          &self,
          request: Request<Action>,
      ) -> Result<Response<Self::DoActionStream>, Status> {
          /* implementations */
      }
  ```

  - 客户端使用：
  ```rust
  let channel = Endpoint::try_from("http://127.0.0.1:6055")?.connect().await?;
  let mut client = FlightServiceClient::new(channel);
  client.add_header("x-version", crate::build::PKG_VERSION)?;
  client.handshake(token.to_string()).await?;
  
  tracing::info!("Push status {status:?} to server");
  let status_bytes = serde_json::to_vec(status)?;
  let action = FlightAction::new("TaskStatus", status_bytes);
  let _resp: Vec<_> = self.client.do_action(action).await?.try_collect().await?;
  ```

- 任务控制流交互 API：
  - Arrow Flight API 接口：
  ```rust
      type DoExchangeStream =
          Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + 'static>>;
  
      #[instrument(skip(self, req))]
      async fn do_exchange(
          &self,
          req: Request<Streaming<FlightData>>,
      ) -> Result<Response<Self::DoExchangeStream>, Status> {
      }
  ```

  - 客户端使用：
  ```rust
  let channel = Endpoint::try_from("http://127.0.0.1:6055")?.connect().await?;
  let mut client = FlightServiceClient::new(channel);
  client.add_header("x-version", crate::build::PKG_VERSION)?;
  client.handshake(token.to_string()).await?;
  
  let req = FlightDataEncoderBuilder::new().with_schema(schema).build(
      resp_rx
          .into_stream()
          .enumerate()
          .map(|(req_id, action)| Ok(resp_action_to_arrow(action, req_id as _).unwrap())),
  );
  let mut stream = self.client.do_exchange(req).await?;
  while let Some(res) = stream.try_next().await? {
      // Deal with record batch that with fields
      // 
      // | ts | action | context | req_id |
  }
  ```

  服务端提供的 Action 如下：
  - `run`：`context` 中包含任务执行信息，反序列化成 Task 数据结构。
  - `stop`：手动停止任务。
  - `cancel`：其他情况触发任务中止运行。
  - `interrupt`：写入阻塞导致任务中止运行。
  - `check`：检查数据源可用性。
  - `sample`：从数据源查询示例数据，支持的数据源包括：
    - Aveva Historian
    - Kafka
    - MQTT
    - MySQL
    - PostgreSQL
    - Orcle
    - MSSQL
    - MongoDB。
  - `put-file`：Server 向 Server 同步文件。
  - `query-data-source`：类似 `sample` 接口，向 PI 服务器请求数据源信息。支持的数据源包括：
    - PI
    - PIBackfill
  - `heartbeat`：Server 向 Agent 发送 heartbeat 信息，Agent 向 Server 返回 HearbeatOk 。
  - `heartbeat-ok`：Agent 向 Server 发送 heartbeat 信息，Server 向 Agent 返回 HeartbeatOk。
- 数据推送 API
  - Arrow Flight API 接口：
  ```rust
      type DoPutStream = Pin<Box<dyn Stream<Item = Result<PutResult, Status>> + Send + 'static>>;
  
      async fn do_put(
          &self,
          req: Request<Streaming<FlightData>>,
      ) -> Result<Response<Self::DoPutStream>, Status> {
      }
  ```

  - 客户端使用：
  ```rust
  let channel = Endpoint::try_from("http://127.0.0.1:6055")?.connect().await?;
  let mut client = FlightServiceClient::new(channel);        client
  client.add_header("x-task-id", &task_id.to_string())
      .context("Add header error")?;
  client.add_header("x-version", crate::build::PKG_VERSION)?;
  client.handshake(token.to_string()).await?;
  
  let data = FlightDataEncoderBuilder::new()
              .with_schema(schema.clone())
              .with_options(
                  IpcWriteOptions::try_new(8, false, arrow::ipc::MetadataVersion::V5).unwrap(),
              )
              .build(data_stream)
              .map({
                  let qid = qid.clone();
                  move |v| {
                      v.map(|message| {
                          message.with_app_metadata(Bytes::copy_from_slice(
                              MessageMetadata::new(qid.get()).as_bytes(),
                          ))
                      })
                  }
              });
  
  let mut stream = self.client.do_push(data).await?;
  while let Some(res) = stream.try_next().await? {
      // Deal with ack.
  }
  ```

### 4.6 命令行模式

#### 4.6.1 双活

使用 replica 子命令可自动化 taosX 配置、一键启动、重启或停止所有双活组件。
- taosx replica start
  - 双活启动配置命令，机器 A/B 上的 taosd 均为存活状态，且运行该命令的机器上 taosx 服务已启动。
  - 可使用两种启动命令：
    - taosx replica start -f <source_endpoint> -t <sink_endpoint> [<database>...] 
      在当前 taosx 服务中建立从 source_endpoint 到 sink_endpoint 的同步任务。运行该命令成功后，将打印 replica ID 到控制台（后续记为 `id`）。
      其中输入参数 source_endpoint 和 sink_endpoiint 为必须，形如 `td2:6030` ，完整的运行命令如：`taosx replica start -f td1:6030 -t td2:6030` 会自动创建除 information_schema、performance_schema、log、audit 库之外的同步任务。**可以使用 **`**http://td2:6041**`** 指定该 endpoint 使用 websocket 接口（默认是原生接口）。**
      也可以指定数据库同步：`taosx replica start -f td1:6030 -t td2:6030 db1` 仅创建指定的数据库同步任务。
    - taosx replica start -i <id> [<database>...]
      **使用上面已经创建的 Replica ID (id) 以在该同步任务中增加其它数据库**。
  - 多次使用该命令，不会创建重复任务，**仅将所指定的数据库增加到相应任务中**。
  - replica id 在一个 taosX 实例内是全局唯一的，与 source/sink 的组合无关
  - 为便于记忆，replica id 为一个随机常用单词，系统自动将 source/sink 组合对应到一个词库中取得一个唯一可用单词。
- taosx replica status [<id>...]
  当 taosx 启动时，返回当前机器上创建的双副本同步任务列表和状态。可以指定一个或多个 replica id 获取其任务列表和状态。
  ```sql
  +---------+----------+----------+----------+------+-------------+----------------+
  | replica | task | source   | sink     | database | status      | note           |
  +---------+----------+----------+----------+------+-------------+----------------+
  | a       | 2    | td1:6030 | td2:6030 | opc      | running     |                |
  | a       | 3    | td2:6030 | td2:6030 | test     | interrupted | <Error reason> |
  ```

- taosx replica stop <id> [<db>...]
  - 停止指定 Replica ID 下所有或指定数据库的双副本同步任务。
  - 使用 `taosx replica stop id1 db1` 表示停止 id1 replica 下 `db1`的同步任务。
- taosx replica restart <id> [<db>...]
  - 重启指定 Replica ID 下所有或指定数据库的双副本同步任务。
  - 使用 `taosx replica stop id1 db1` 仅重启指定数据库 `db1`的同步任务。
- taosx replica diff <id> [<db>....]
  - 当前双副本同步任务中订阅的 Offset  与最新 WAL 的差值（不代表行数）。输出示例如下：
  ```sql
  +---------+----------+----------+----------+-----------+---------+---------+------+
  | replica | database | source   | sink     | vgroup_id | current | latest  | diff |
  +---------+----------+----------+----------+-----------+---------+---------+------+
  | a       | opc      | td1:6030 | td2:6030 | 2         | 17600   | 17600   | 0    |
  | ad       | opc      | td2:6030 | td2:6030 | 3         | 17600   | 17600   | 0    |
  ```

- taosx replica remove <id> [--force]
  - 删除当前所有双副本同步任务。（这是为方便测试添加的便捷清理命令，需要先 stop；当 --force 启用时，强制停止并清除任务。）
具体使用：
1. 假定在机器 A 上运行，需要首先使用 `taosx replica start` 来配置 taosX，其输入参数是待同步的源端和目标端服务器地址 ，在完成配置后会自动启动同步服务和任务。此处假定 taosx 服务使用标准端口，同步任务使用原生连接。
2. 机器 B 上的步骤相同
3. 在完成对两台机器的服务启动后，双活系统即可提供服务
4. 在已经完成配置后，如果想要再次启动双活系统，请使用 restart 子命令

#### 用户名密码迁移

使用 privileges 子命令可支持用户名、密码、权限和白名单信息的在线迁移以及备份和导入。
基本的命令行使用方式如下：
```bash

## 1. From one to another cluster

taosx privileges -f "taos://root:taosdata@localhost" \
  -t "taos://other"

## 2. Export to a single file.

taosx privileges -f "taos+ws://root:taosdata@localhost:6041" \
  -o ./path/to/file

## 3. Import from backup file.

taosx privileges -i ./path/to/file -t "taos://other"
```

连接方式支持原生连接与 websocket.
仅 root 帐号或可导出或导入。
支持额外的选项以选择对哪些内容生效：
- `-u`：表示仅对用户名和密码进行导入导出。
- `-p`：表示仅对权限进行导入导出。
`-u` 和 `-p` 同时使用时与默认情况（无 `-u` 和 `-p`）一致。

#### 执行一次性任务

使用 `run` 子命令可执行一次性数据导入导出任务，基本用法如下：
```bash
Usage: taosx run [OPTIONS] --from <FROM> --to <TO>

Options:
  -f, --from <FROM>
          Input DSN(Data Source Name) string
  -t, --to <TO>
          Output DSN
  -p, --parser <PARSER>
          Parser
  -T, --transform <TRANSFORM>
          Transformer actions
  -c, --config <CONFIG>
          
  -v, --verbose...
          Increase logging verbosity
  -q, --quiet...
          Decrease logging verbosity
      --tracing-events <TRACING_EVENTS>
          [env: TRACING_EVENTS=] [default: none]
      --plugins-home <PLUGINS_HOME>
          [env: PLUGINS_HOME=]
      --data-dir <DATA_DIR>
          [env: TAOSX_DATA_DIR=]
      --instance-id <INSTANCE_ID>
          [env: INSTANCE_ID=]
      --logs-home <LOGS_HOME>
          [env: LOGS_HOME=]
  -d, --debug
          Enable debug will set the mod path as `file:line`
      --log-keep-days <LOG_KEEP_DAYS>
          Log keep days [env: LOG_KEEP_DAYS=]
      --no-log-to-files
          Not log to files
      --no-async-log
          Disable non-blocking writer for log file appender
  -j, --jobs <JOBS>
          Number of jobs, default to 0, will use `jobs` number of works for TMQ [default: 0]
      --otel
          Enable OpenTelemetry tracing and metrics exporter [env: ENABLE_OTEL=]
      --monitor-port <PORT>
          Port of taosKeeper service [env: MONITOR_PORT=] [default: 6043]
      --monitor-interval <INTERVAL>
          [env: MONITOR_INTERVAL=] [default: 10]
  -h, --help
          Print help (see more with '--help')

```

`FROM` 和 `TO` 分别表示来源或目的，典型的 DSN 如下：
```bash

## url-like

<driver>[+<protocol>]://[[<username>:<password>@]<host>:<port>][/<object>][?<p1>=<v1>[&<p2>=<v2>]]
|------|------------|---|-----------|-----------|------|------|----------|-----------------------|
|driver|   protocol |   | username  | password  | host | port |  object  |  params               |

// url 示例
tmq+ws://root:taosdata@localhost:6030/db1?timeout=never
```

`[]`中的数据都为可选参数。
1. 不同的驱动 (`driver`) 拥有不同的参数。`driver` 包含如下选项:
  - taos：使用查询接口从 TDengine 获取数据
  - tmq：启用数据订阅从 TDengine 获取数据
  - local：数据备份或恢复
  - pi: 启用 pi-connector从 pi 数据库中获取数据
  - opc：启用 opc-connector 从 opc-server 中获取数据
  - mqtt: 启用 mqtt-connector 获取 mqtt-broker 中的数据
  - kafka: 启用 Kafka 连接器从 Kafka Topics 中订阅消息写入
  - influxdb: 启用 influxdb 连接器从 InfluxDB 获取数据
  - csv：从 CSV 文件解析数据
1. `+protocol` 包含如下选项：
  - `+ws`: 当 driver 取值为 taos 或 tmq 时使用，表示使用 rest 获取数据。不使用 +ws 则表示使用原生连接获取数据，此时需要 taosx 所在的服务器安装 taosc。
  - `+ua`: 当 driver 取值为 opc 时使用，表示采集的数据的 opc-server 为 opc-ua
  - `+da`: 当 driver 取值为 opc 时使用，表示采集的数据的 opc-server 为 opc-da
1. host:port 表示数据源的地址和端口。
2. object 表示具体的数据源，可以是TDengine的数据库、超级表、表，也可以是本地备份文件的路径，也可以是对应数据源服务器中的数据库。
3. username 和 password 表示该数据源的用户名和密码。
4. params 代表了 dsn 的参数，一般用键值对表示。
`PARSER` 是一个 JSON 格式的字符串或文件（文件使用 `@` 符号开头，如 `@/path/to/demo.json`），用于 Kafka、MQTT、MySQL 等关系型数据库、MongoDB、CSV 等数据源入库配置:
```json
{
  "parse": {
    "ts": { "as": "TIMESTAMP(ms)" },
    "current": { "as": "FLOAT" },
    "voltage": { "as": "INT" },
    "phase": { "as": "FLOAT" },
    "groupid": { "as": "INT" },
    "location": { "as": "VARCHAR(24)" }
  },
  "model": {
    "name": "${tbname}",
    "using": "meters",
    "tags": ["groupid", "location"],
    "columns": ["ts", "current", "voltage", "phase"]
  }
}
```

##### 数据库持续同步

```bash
taosx run -f "tmq+ws://root:taosdata@hosta:6041/db1?group.id=demo&timeout=never" \
  -t "taos+ws://root:taosdata@hostb:6041/db1"
```

##### 数据库迁移

```bash
taosx run -f "taos+ws://root:taosdata@hosta:6041/db1?mode=history&schema=always" \
  -t "taos+ws://root:taosdata@hostb:6041/db2"
```

数据库迁移支持的参数见 [taosX Data Migration User Manual](https://taosdata.feishu.cn/wiki/wikcnlXBGv4UKBOGld94f6leHre)。

##### 导出查询结果为 CSV 文件

以智能电表数据为例：
```bash
taosx run -f "taos:///test?query=select tbname, * from meters" \
  -t "csv:./test.csv"
```

`query` 为指定的查询语句，`./test.csv` 为写入 CSV 文件名。
```plaintext
tbname,ts,current,voltage,phase,groupid,location
d0,2017-07-14T10:40:00+08:00,6.735898,248,149.5,3,California.Sunnyvale
...
```

##### CSV 文件导入 TDengine

CSV 文件可根据 Header 导入 TDengine，以智能电表数据为例，配置 Parser 文件如下所示：
```json
{
  "parse": {
    "ts": { "as": "TIMESTAMP(ms)" },
    "current": { "as": "FLOAT" },
    "voltage": { "as": "INT" },
    "phase": { "as": "FLOAT" },
    "groupid": { "as": "INT" },
    "location": { "as": "VARCHAR(24)" }
  },
  "model": {
    "name": "${tbname}",
    "using": "meters",
    "tags": ["groupid", "location"],
    "columns": ["ts", "current", "voltage", "phase"]
  }

```

执行命令如下，导入 TDengine：
```bash
taosx run -f "csv:./*.csv" \
  -t "taos:///db1" \
  -P "@./csv-parser.json"
```

##### Kafka/MQTT 主题导入 TDengine

Kafka/MQTT Payload 为 JSON 时，可以通过 taosx 直接导入 TDengine。
首先配置 JSON 解析文件：
```json
{
  "model": {
    "columns": ["ts", "id", "voltage"],
    "name": "d{id}",
    "tags": ["groupid", "location"],
    "using": "meters_mqtt"
  },
  "parse": {
    "payload": {
      "json": [
        {
          "alias": "id",
          "cast": "INT",
          "name": "id"
        },
        {
          "alias": "voltage",
          "cast": "INT",
          "name": "voltage"
        },
        {
          "alias": "groupid",
          "cast": "INT",
          "name": "groupid"
        },
        {
          "alias": "location",
          "cast": "INT",
          "name": "location"
        }
      ],
      "keep": true
    }
  }
}
```

配置数据源（以 MQTT 为例）：
```bash
taosx run -f "mqtt://root:taosdata@localhost:1883?topics=tp1,tp2" \
  -t "taos:///db1" \
  -P "@./mqtt-parser.json"
```


### 4.7 共享存储

#### 4.7.1 目录结构

高可用架构要求所有 XNode 配置相同的共享存储路径，目录结构如下：
```bash {wrap}
/taosx/data/
├── files/              # 上传文件目录
├── tasks/              # 任务目录
│   ├── 1/              # 任务 1 目录
│   │   ├── x.lock      # 任务锁文件
│   │   ├── checkpoints/# 检查点数据
│   │   ├── metrics/    # 运行指标缓存
│   │   └── shards/     # 分片数据目录
│   │       ├── dump/   # 数据转储
│   │       ├── archived/# 归档文件
│   │       └── cache/  # 缓存文件
│   ├── 2/
│   └── ...
└── logs/               # 日志目录
```

#### 4.7.2 配置方式

1. 在 `taos.cfg` 中添加：`taosxDataDir /nas/taosx/data/`
2. 或在 `taosx.toml` 中配置：`data_dir = "/nas/taosx/data/"`

#### 4.7.3 自动备份

在 `taos.cfg` 中配置：
```sql {wrap}
xNodeBackupCron "0 15 10 ? * MON-FRI"  # 每周一到周五上午 10:15 备份
xNodeBackupDir "/backup/taosx/"        # 备份存储路径
```

## 性能

1. 数据迁移在不做任何变换下，支持 1000 万数据点每秒迁移速度。
2. 数据同步支持 100 万点每秒同步速度。
3. 数据备份文件磁盘占用不高于 TSDB，落盘数据支持压缩。
4. CSV 数据导入支持 100 万点每秒导入速度。
5. **高可用架构性能指标**：
  - 任务分片切换时间：< 30 秒（XNode 故障后）
  - MNode 切换恢复时间：< 10 秒
  - 支持的任务分片数：单任务最多 1000 个分片
  - 支持的并发任务数：无硬性限制，取决于集群资源

## 安全

### 6.1 认证机制

#### 6.1.1 XNode 与 MNode 认证

XNode 与 MNode 之间采用双层认证机制：
1. **TLS 双向认证 (mTLS)**
  - XNode 和 MNode 各自持有客户端/服务端证书
  - 握手阶段验证对端证书有效性
  - 支持证书固定防止中间人攻击
1. **JWT Token 认证**
  - 认证信息：
  ```bash {wrap}
  pub struct XnodedId {
      pub cluster_id: String,
      pub leader_ep: String,
  }
  ```

  - Token 通过 HTTP Header `x-token` 传递
  - Token 使用 HMAC-SHA256 签名
  - Token 有效期可配置，默认 24 小时
1. 握手流程
```bash {wrap}
XNode                      MNode
  │                         │
  ├──── TLS Handshake ────►│ (证书交换)
  │◄──── TLS Established ──┤
  │                         │
  ├──── JWT Handshake ────►│ (x-token header)
  │◄──── Ack + Session ID ─┤
```

#### 6.1.2 Agent 与 XNode 认证

1. **Agent Token 认证**
  - Agent 启动时通过 `CREATE XNODE AGENT` 获取 Token
  - Token 格式：JWT (HS256)
  - Payload 包含 agent_id 和过期时间
  - 通过 HTTP Header `x-token` 传递
1. **版本兼容性检查**
  - 通过 HTTP Header `x-version` 传递版本信息
  - 服务端检查版本兼容性
  - 不兼容版本拒绝连接
1. **认证流程**
```java {wrap}
// Agent 侧
let mut client = FlightClient::new(channel);
client.add_header("x-version", PKG_VERSION)?;
client.add_header("x-token", &token)?;
client.handshake(token).await?;

// XNode 侧
let token = meta.get("x-token").ok_or(Status::unauthenticated("Missing token"))?;
let agent_id = validate_jwt(token)?;
```

### 6.2 权限控制

#### 6.2.1 SQL 权限矩阵

| **操作** | **所需权限** | **说明** |
| --- | --- | --- |
| CREATE XNODE | ROOT 或 WRITE 权限 | 仅管理员可创建节点 |
| DROP XNODE | ROOT 或 WRITE 权限 | 仅管理员可删除节点 |
| DRAIN XNODE | ROOT 或 WRITE 权限 | 仅管理员可排空节点 |
| CREATE XNODE TASK | 普通用户可执行 | 创建者拥有任务所有权 |
| ALTER XNODE TASK | 任务所有者或管理员 | 只能修改自己的任务 |
| DROP XNODE TASK | 任务所有者或管理员 | 只能删除自己的任务 |
| START/STOP XNODE TASK | 任务所有者或管理员 | 只能启停自己的任务 |
| SHOW XNODE TASKS | 所有用户可执行 | 只能查看自己的任务 |

#### 6.2.2 任务权限自动管理

1. **写入权限自动授权**
  - 任务创建时，系统自动检查目标数据库写入权限
  - 使用 `__xnode__` 用户执行写入操作
  - 无需手动为用户授权
1. **数据源访问权限**
  - 数据源访问凭证通过 DSN 传递
  - DSN 密码加密存储在 MNode
  - XNode 启动任务时获取解密后的 DSN

### 6.3 通信安全

#### 6.3.1 TLS 配置

1. **服务端配置 (taosx.toml)**
```bash {wrap}
[serve]

## TLS 证书配置

ssl_cert = "/path/to/tls/server.pem"
ssl_key = "/path/to/tls/server.key"
ssl_ca = "/path/to/tls/ca.pem"
```

1. **客户端配置**
  - Agent 启动时指定 CA 证书
  - 支持跳过证书验证（仅测试环境）
  - 生产环境强制验证证书
1. **TLS 版本要求**
  - 最低版本：TLS 1.2
  - 推荐版本：TLS 1.3
  - 禁用不安全的密码套件

#### 6.3.2 gRPC 安全选项

```java {wrap}
// 服务端 TLS 配置
let tls_config = ServerTlsConfig::new()
    .identity(identity)
    .client_ca_root(ca_cert); // 开启双向认证

let server = Server::builder()
    .tls_config(tls_config)?
    .add_service(flight_service)
    .serve(addr);

// 客户端 TLS 配置
let tls_config = ClientTlsConfig::new()
    .ca_certificate(ca_cert)
    .identity(client_identity); // 客户端证书

let channel = Endpoint::new(addr)
    .tls_config(tls_config)?
    .connect()
    .await?;
```

### 6.4 数据安全

#### 6.4.1 DSN 密码保护

1. **存储加密**
  - DSN 密码使用 AES-256-GCM 加密
  - 加密密钥存储在 MNode 安全配置中
1. **传输安全**
  - DSN 仅在 XNode 与 MNode 之间传输
  - 不落盘存储在共享存储

## 兼容性

- **TDengine 版本**：高可用功能需要 TDengine 3.4.0.0 及以上版本。
- **数据迁移**：数据源支持 2.0.20 及以上版本、2.2、2.4、2.6 版本，目标端支持 3.0、3.1、3.2 及以上版本。
- **数据订阅**：支持任意 3.0 以上低版本数据订阅写入到高版本集群。
- **数据导入**：支持 Windows、Linux、macOS 下的 CSV 文件。
- **备份恢复**：支持备份文件复制到另一个机器上读取并恢复到新集群，与运行系统无关。
- **数据导入**：支持 InfluxDB 1.8 及以上版本。
- **升级兼容**：
  - API 接口保持兼容，原有 REST API 和 Arrow Flight gRPC 接口仍然可用。
  - 旧版本 Agent 连接新版本 taosx 报错：不支持的版本，请升级 taosx agent。

## 运维

### 8.1 从旧版本升级

升级步骤：
1. **备份数据**：升级前请备份数据文件目录：`/var/lib/taos/taosx/`。
2. **升级 TDengine**：安装 TDengine 3.3.0.0 或更高版本。
3. **配置共享存储**：修改 `taos.cfg` 或 `taosx.toml`，配置共享存储路径。
4. **启动服务**：启动 taosd 和 taosx 服务。
5. **创建 XNode**：使用 `CREATE XNODE` 命令注册 XNode 节点。
6. **数据迁移**：使用 Explorer 导出旧任务，并在新的版本上导入。

### 8.2 重启服务

```sql {wrap}

## 重启 XNode 服务

systemctl restart taosx

## 重启 Agent 服务

systemctl restart taosx-agent

## 重启 TDengine（会触发 MNode 切换）

systemctl restart taosd
```

### 8.3 日志分析

通过 `task.id:<id>` 可以过滤指定任务的日志：
tail -f /var/log/taos/taosx.log | grep "task.id:1"
通过 `xnode.id:<id>` 可以过滤指定节点的日志：
tail -f /var/log/taos/taosx.log | grep "xnode.id:1"

### 8.4 常见问题排查

#### 8.4.1 XNode 无法注册

1. 检查 XNode 服务是否启动：`systemctl status taosx`
2. 检查网络连通性：`ping <mnode_host>`
3. 检查 TDengine 是否正常运行：`show cluster`
4. 检查用户权限：确保使用 root 或管理员用户执行 `CREATE XNODE`

#### 8.4.2 任务执行失败

1. 查看任务状态：`SHOW XNODE TASKS`
2. 查看分片状态：`SHOW XNODE JOBS`
3. 查看错误日志：`grep "task.id:<id>" /var/log/taos/taosx_16_<YYYYMMDD>.log`
4. 检查数据源可用性：验证 DSN 配置是否正确

#### 8.4.3 XNode 宕机后任务未迁移

1. 确认节点状态：`SHOW XNODES`
2. 确认其他 XNode 是否在线
3. 检查 MNode 是否正常工作：`show mnodes`
4. 手动触发重平衡：`REBALANCE XNODE JOBS WHERE xnode = <id>`

## 使用场景

### 9.1 边缘到云端数据同步

在边缘节点部署 XNode，通过数据订阅将边缘 TDengine 数据实时同步到云端 TDengine。
```sql {wrap}
-- 在云端创建任务
CREATE XNODE TASK 'edge_sync' FROM 'tmq+ws://edge:6041/db1?group.id=sync' TO DATABASE cloud_db;
START XNODE TASK 'edge_sync';
```

### 9.2 多节点高可用数据接入

部署多个 XNode 节点，Kafka 数据接入任务自动分片到多个节点并行处理，当某个节点故障时自动迁移。
```sql {wrap}
-- 创建 XNode 节点
CREATE XNODE 'x1:6050';
CREATE XNODE 'x2:6050';
CREATE XNODE 'x3:6050';

-- 创建 Kafka 接入任务（自动分片）
CREATE XNODE TASK 'kafka_ingest' FROM 'kafka://broker:9092?topics=metrics' TO DATABASE testdb  WITH PARSER '@kafka-parser.json';

START XNODE TASK 'kafka_ingest';
```

### 9.3 历史数据迁移

将 TDengine 2.x 的历史数据迁移到 3.x 集群。
```sql {wrap}
-- 创建迁移任务
CREATE XNODE TASK 'v2_to_v3' FROM 'taos://old:6030/db1?mode=history' TO DATABASE new_db;
START XNODE TASK 'v2_to_v3';

-- 查看进度
SHOW XNODE JOBS WHERE tid = 1;
```

### 9.4 跨机房双活

两个数据中心部署独立的 TDengine 集群，通过 XNode 实现双向同步。
```sql {wrap}
-- 数据中心 A
CREATE XNODE TASK 'dc_a_to_b' FROM 'tmq+ws://dc-a:6041/db1' TO 'taos+ws://dc-b:6041/db1';
START XNODE TASK 'dc_a_to_b';

-- 数据中心 B
CREATE XNODE TASK 'dc_b_to_a' FROM 'tmq+ws://dc-b:6041/db1' TO 'taos+ws://dc-a:6041/db1';
START XNODE TASK 'dc_b_to_a';
```

## 约束和限制

### 10.1 约束

- 高可用功能需要 TDengine 3.4.0.0 及以上版本。
- 所有 XNode 节点必须挂载同一共享存储路径。
- 一个 TDengine 集群仅允许一套 taosX 实例（TDengine 与 taosX 绑定部署）。

### 10.2 限制

- taosX 支持使用 taosc 客户端连接 TDengine，需要安装 TDengine 客户端。
- taosX 支持 WebSocket 连接 TDengine，需要服务端部署 taosAdapter 且 HTTP 连接可用。
- taosX 支持 OPC-DA 数据源，但仅支持 Windows 系统（使用 taosX Windows 版本，或者 Windows 上的 Agent 服务）。
- taosX 支持 PI 数据源，需要安装 PI AF SDK，仅支持 Windows 系统或 Windows 上的 Agent 服务。
- taosX 支持 MQTT 数据源 3.1 和 5.0 协议。
- taosX 支持 Kafka 0.10.0 及以上版本。
- taosX 支持 Oracle：
  - 连接 Oracle 数据库需要安装 [**ODPI-C**](https://oracle.github.io/odpi/) 库。
  - 支持 Oracle 客户端 11.2 以上。
  - 不支持自定义类型、XML 类型、JSON 类型。
- taosX 支持 MySQL/PostgreSQL 不含 socket 直连。
- taosX 支持 InfluxDB > 1.8.0，且 < 3.0.0。
- **高可用架构限制**：
  - PI、OPC-UA/DA、InfluxDB、OpenTSDB 数据源暂不支持分片，任务只能在单个 XNode 上执行。
  - TDengine 宕机时，taosX 不能创建、修改或启停任务。

## 常见错误和排查

### 11.1 数据源常见错误

- 使用 TDengine 错误码识别和分析错误来源。
- 使用 Task ID 过滤日志：`grep "task.id:1" /var/log/taos/taosx.log`
- 使用 Request ID 过滤单次写入错误。

### 11.2 XNode 管理错误

| **错误信息** | **可能原因** | **解决方案** |
| --- | --- | --- |
| MNode is not ready | MNode 未准备好或不是 Leader | 检查 show mnodes 状态 |
| XNode already exists | XNode 已注册 | 使用 SHOW XNODES 查看已注册节点 |
| Cannot drop XNode with running tasks | 节点上有运行中的任务 | 先停止任务或强制删除 |
| No available XNode | 没有可用的 XNode 节点 | 使用 SHOW XNODES 检查节点状态 |

### 11.3 任务执行错误

| **错误信息** | **可能原因** | **解决方案** |
| --- | --- | --- |
| Task name already exists | 任务名称已存在 | 使用新的任务名称或删除旧任务 |
| Invalid DSN format | DSN 格式错误 | 检查 DSN 格式是否正确 |
| Data source connection failed | 数据源连接失败 | 检查网络和数据源配置 |
| Shard execution failed | 分片执行失败 | 查看 SHOW XNODE JOBS 详情 |

### 11.4 用户名密码迁移

导入错误分为两类：
一类是 taosx 错误：
- 无导入导出权限：仅 root 用户可进行导入导出操作。
- 不支持版本 X 的备份文件恢复到 Y 版本：请按 **"约束和限制"** 中的版本约束说明使用。 
- 指定了导入项目（用户名密码或权限），但文件中不存在该信息。
一类是 taosc 错误：
- 导入用户名和密码时用户已存在（`import user` 报错）。
- 单独导入权限时，用户不存在。
- 导入的权限已在用户的权限列表中。
- 导入权限时权限所关联的对象不存在，包括数据库、Topic、表。

## 可观测性

taosX 会将监控指标上报给 taosKeeper，这些监控指标会被 taosKeeper 写入监控数据库，默认是 `log` 库，可以在 taoskeeper 配置文件中修改。

### 12.1 XNode 服务指标

| 字段 | 描述 |
| --- | --- |
| sys_cpu_cores | 系统 CPU 核数 |
| sys_total_memory | 系统总内存，单位：字节 |
| sys_used_memory | 系统已用内存, 单位：字节 |
| sys_available_memory | 系统可用内存, 单位：字节 |
| process_uptime | XNode 运行时长，单位：秒 |
| process_id | XNode 进程 ID |
| running_tasks | XNode 当前执行任务数 |
| completed_tasks | XNode 进程在一个监控周期（比如10s）内完成的任务数 |
| failed_tasks | XNode 进程在一个监控周期（比如10s）内失败的任务数 |
| process_cpu_percent | XNode 进程占用 CPU 百分比， 单位 % |
| process_memory_percent | XNode 进程占用内存百分比， 单位 % |
| process_disk_read_bytes | XNode 进程在一个监控周期（比如10s）内从硬盘读取的字节数的平均值，单位 bytes/s |
| process_disk_written_bytes | XNode 进程在一个监控周期（比如10s）内写到硬盘的字节数的平均值，单位 bytes/s |

### 12.2  Agent

| 字段 | 描述 |
| --- | --- |
| sys_cpu_cores | 系统 CPU 核数 |
| sys_total_memory | 系统总内存，单位：字节 |
| sys_used_memory | 系统已用内存, 单位：字节 |
| sys_available_memory | 系统可用内存, 单位：字节 |
| process_uptime | agent 运行时长，单位：秒 |
| process_id | agent 进程 id |
| process_cpu_percent | agent 进程占用 CPU 百分比 |
| process_memory_percent | agent 进程占用内存百分比 |
| process_uptime | 进程启动时间，单位秒 |
| process_disk_read_bytes | agent 进程在一个监控周期（比如10s）内从硬盘读取的字节数的平均值，单位 bytes/s |
| process_disk_written_bytes | agent 进程在一个监控周期（比如10s）内写到硬盘的字节数的平均值，单位 bytes/s |

### 12.3 Connector

| 字段 | 描述 |
| --- | --- |
| process_id | connector 进程 id |
| process_uptime | 进程启动时间，单位秒 |
| process_cpu_percent | 进程占用 CPU 百分比， 单位 % |
| process_memory_percent | 进程占用内存百分比， 单位 % |
| process_disk_read_bytes | connector 进程在一个监控周期（比如10s）内从硬盘读取的字节数的平均值，单位 bytes/s |
| process_disk_written_bytes | connector 进程在一个监控周期（比如10s）内写到硬盘的字节数的平均值，单位 bytes/s |

### 12.4 任务通用指标

| 字段 | 描述 |
| --- | --- |
| total_execute_time | 任务累计运行时间，单位毫秒 |
| total_written_rows | 成功写入 TDengine 的总行数（包括重复记录） |
| total_written_points | 累计写入成功点数 (等于数据块包含的行数乘以数据块包含的列数) |
| start_time | 任务启动时间 (每次重启任务会被重置) |
| written_rows | 本次运行此任务成功写入 TDengine 的总行数（包括重复记录） |
| written_points | 本次运行写入成功点数 (等于数据块包含的行数乘以数据块包含的列数) |
| execute_time | 任务本次运行时间，单位秒 |

### 12.5 taosX TDengine V2 任务

| 字段 | 描述 |
| --- | --- |
| read_concurrency | 并发读取数据源的数据 worker 数, 也等于并发写入 TDengine 的 worker 数 |
| total_stables | 需要迁移的超级表数据数量 |
| total_updated_tags | 累计更新 tag 数 |
| total_created_tables | 累计创建子表数 |
| total_tables | 需要迁移的子表数量 |
| total_finished_tables | 完成数据迁移的子表数 (任务中断重启可能大于实际值) |
| total_success_blocks | 累计写入成功的数据块数 |
| finished_tables | 本次运行完成迁移子表数 |
| success_blocks | 本次写入成功的数据块数 |
| created_tables | 本次运行创建子表数 |
| updated_tags | 本次运行更新 tag 数 |

### 12.6 taosX TDengine V3 任务

| 字段 | 描述 |
| --- | --- |
| total_messages | 通过 TMQ 累计收到的消息总数 |
| total_messages_of_meta | 通过 TMQ 累计收到的 Meta 类型的消息总数 |
| total_messages_of_data | 通过 TMQ 累计收到的 Data 和 MetaData 类型的消息总数 |
| total_write_raw_fails | 累计写入 raw meta 失败的次数 |
| total_success_blocks | 累计写入成功的数据块数 |
| topics | 通过 TMQ 订阅的主题数 |
| consumers | TMQ 消费者数 |
| messages | 本次运行通过 TMQ 收到的消息总数 |
| messages_of_meta | 本次运行通过 TMQ 收到的 Meta 类型的消息总数 |
| messages_of_data | 本次运行通过 TMQ 收到的 Data 和 MetaData 类型的消息总数 |
| write_raw_fails | 本次运行写入 raw meta 失败的次数 |
| success_blocks | 本次写入成功的数据块数 |

### 12.7 taosX 其他数据源 任务

这些数据源包括： InfluxDB，OpenTSDB，OPC UA，OPC DA，PI，CSV，MQTT，AVEVA Historian 和 Kafka。
| 字段 | 描述 |
| --- | --- |
| total_received_batches | 通过 IPC Stream 收到的数据总批数 |
| total_processed_batches | 已经处理的批数 |
| total_processed_rows | 已经处理的总行数（等于每批包含数据行数之和） |
| total_inserted_sqls | 执行的 INSERT SQL 总条数 |
| total_failed_sqls | 执行失败的 INSERT SQL 总条数 |
| total_created_stables | 创建的超级表总数（可能大于实际值） |
| total_created_tables | 尝试创建子表总数(可能大于实际值) |
| total_failed_rows | 写入失败的总行数 |
| total_failed_point | 写入失败的总点数 |
| total_written_blocks | 写入成功的 raw block 总数 |
| total_failed_blocks | 写入失败的 raw block 总数 |
| received_batches | 本次运行此任务通过 IPC Stream 收到的数据总批数 |
| processed_batches | 本次运行已处理批数 |
| processed_rows | 本次处理的总行数（等于包含数据的 batch 包含的数据行数之和） |
| received_records | 本次运行此任务通过 IPC Stream 收到的数据总行数 |
| inserted_sqls | 本次运行此任务执行的 INSERT SQL 总条数 |
| failed_sqls | 本次运行此任务执行失败的 INSERT SQL 总条数 |
| created_stables | 本次运行此任务尝试创建超级表数（可能大于实际值） |
| created_tables | 本次运行此任务尝试创建子表数(可能大于实际值) |
| failed_rows | 本次运行此任务写入失败的行数 |
| failed_points | 本次运行此任务写入失败的点数 |
| written_blocks | 本次运行此任务写人成功的 raw block 数 |
| failed_blocks | 本次运行此任务写入失败的 raw block 数 |

### 12.8 Kafka 数据源相关指标

| 字段 | 描述 |
| --- | --- |
| kafka_consumers | 本次运行任务 Kafka 消费者数 |
| kafka_total_partitions | Kafka 主题总分区数 |
| kafka_consuming_partitions | 本次运行任务正在消费的分区数 |
| kafka_consumed_messages | 本次运行任务已经消费的消息数 |
| total_kafka_consumed_messages | 累计消费的消息总数 |

### 12.9 SQL 查询监控

```sql {wrap}
-- 查看所有 XNode 状态
SHOW XNODES;

-- 查看所有任务状态
SHOW XNODE TASKS;

-- 查看指定任务的分片状态
SHOW XNODE JOBS WHERE tid = 1;

-- 查看任务详细指标（JSON 格式）
SELECT * FROM log.xnode_tasks WHERE id = 1;
```

## 安装和卸载

### 13.1 安装

XNode 随 TDengine 企业版安装包一同发布，安装方式与 TDengine 一致：
1. 下载 TDengine 企业版安装包（3.4.0.0 或更高版本）
2. 执行安装脚本
3. 配置共享存储路径
4. 启动 taosd 和 taosx 服务
5. 使用 `CREATE XNODE` 注册节点

### 13.2 卸载

卸载 TDengine 时会一同卸载 XNode。卸载前建议：
1. 停止所有运行中的任务
2. 备份任务数据（如需保留）
3. 执行卸载命令

## 文档

需要输出企业版帮助文档以下内容：
1. 参考手册 - 产品组件 - XNode 组件介绍、配置、使用等。
2. 参考手册 - SQL 手册 - XNode 管理相关 SQL 命令。
3. 高级功能 - 零代码数据写入 - 各数据源写入介绍、配置和使用说明等。
4. 运维指南 - XNode 高可用架构部署、监控和故障排查。
5. 监控指标和运维指南等。
需要修改官网文档：
- 更新 taosX 架构描述，增加高可用架构说明。
- 新增 XNode SQL 命令参考文档。

## 参考文档

- [DataX - 阿里云DataWorks数据集成的开源版本](https://github.com/alibaba/DataX)
- [SeaTunnel Docs](https://interestinglab.github.io/seatunnel-docs/)
- [OpenAPI Specification 3.0](https://swagger.io/specification/)
- [TDengine SQL 手册](https://docs.taosdata.com/reference/taos-sql/)
- [TDengine 数据接入文档](https://docs.taosdata.com/reference/taos-sql/datain/)

## 附录

无
