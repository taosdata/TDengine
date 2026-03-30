# TDenging MCP server - FS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-01-21 | 2026-01-21 | 0.1 | 谭雪峰 | 编写文档 |
| 2026-01-22 | 2026-01-22 | 0.2 | 谭雪峰 | 修改名称和执行程序名称 补充常见错误和排查 补充兼容性 补充可观测性 补充示例 |

## 2. 背景

做一个支持 TDengine TSDB 的 MCP 服务

## 3. 定义

MCP：模型上下文协议（Model Context Protocol，MCP），是由Anthropic推出的开源协议，旨在实现大语言模型与外部数据源和工具的集成，用来在大模型和数据源之间建立安全双向的连接。该协议通过相同的协议同时处理本地资源（例如数据库、文件、服务等）和远程资源（例如Slack或GitHub等API）。

## 4. 行为说明

系统通过 **结构化的 MCP Tool** 向外提供 TDengine 的信息与查询能力，避免直接暴露底层数据库接口。
所有数据库访问均在 **受控范围内执行**，并通过明确的参数定义和校验规则约束行为。
查询能力 **仅支持只读操作（SELECT）**，不允许任何形式的数据修改或管理类语句。
元数据与结构信息既可通过 **实时查询** 获取，也可通过 **预置的结构说明文件** 进行补充说明。
系统设计强调 **安全性、确定性与可理解性**，以适配 AI Agent 的自动化调用与推理需求。
所有执行 SQL 的命令返回结果均为 CSV 格式

### 4.1 show

`show` 是一个用于查询 **TDengine 元数据** 的 MCP Tool。
 该工具将结构化参数转换为合法的 TDengine `SHOW` SQL 语句，并执行查询后返回结果。
它统一封装了 TDengine 中 **80+ 种 SHOW 语句**，对调用方屏蔽 SQL 细节，仅需通过：
- 一个 `metadata` 枚举值
- 若干可选/必选参数
即可完成元数据查询。

#### 4.1.1 规则

metadate 支持以下 show 规则

| SHOW 规则 | 必选参数 | 可选参数 |
| --- | --- | --- |
| XNODES | 无 | 无 |
| XNODE | xnode_type | 无 |
| DNODES | 无 | 无 |
| USERS | 无 | 无 |
| USERS FULL | 无 | 无 |
| USER PRIVILEGES | 无 | 无 |
| ROLES | 无 | 无 |
| ROLE PRIVILEGES | 无 | 无 |
| ROLE COLUMN PRIVILEGES | 无 | 无 |
| DATABASES | 无 | 无 |
| USER DATABASES | 无 | 无 |
| SYSTEM DATABASES | 无 | 无 |
| TABLES | max_rows | database, like_pattern |
| VTABLES | max_rows | database, like_pattern |
| STABLES | max_rows | database, like_pattern |
| VGROUPS | 无 | database |
| MNODES | 无 | 无 |
| QNODES | 无 | 无 |
| ANODES | 无 | 无 |
| ANODES FULL | 无 | 无 |
| ARBGROUPS | 无 | 无 |
| FUNCTIONS | 无 | 无 |
| INDEXES | table | database |
| STREAMS | 无 | database |
| APPS | 无 | 无 |
| CONNECTIONS | max_rows | 无 |
| GRANTS | 无 | 无 |
| GRANTS FULL | 无 | 无 |
| GRANTS LOGS | 无 | 无 |
| INSTANCES | 无 | like_pattern |
| CLUSTER MACHINES | 无 | 无 |
| MOUNTS | 无 | 无 |
| CREATE DATABASE | database | 无 |
| CREATE TABLE | table | database |
| CREATE VTABLE | table | database |
| CREATE STABLE | table | database |
| ENCRYPTIONS | 无 | 无 |
| ENCRYPT_ALGORITHMS | 无 | 无 |
| ENCRYPT_STATUS | 无 | 无 |
| QUERIES | max_rows | 无 |
| SCORES | 无 | 无 |
| TOPICS | 无 | 无 |
| VARIABLES | 无 | database, like_pattern |
| CLUSTER VARIABLES | 无 | like_pattern |
| LOCAL VARIABLES | 无 | like_pattern |
| DNODE VARIABLES | dnode_id | like_pattern |
| SNODES | 无 | 无 |
| BNODES | 无 | 无 |
| CLUSTER | 无 | 无 |
| TRANSACTIONS | 无 | 无 |
| TRANSACTION | transaction_id | 无 |
| TABLE DISTRIBUTED | table | database |
| CONSUMERS | 无 | 无 |
| SUBSCRIPTIONS | 无 | 无 |
| TAGS | table | database |
| TABLE TAGS | table | database, tag_list |
| VNODES ON DNODE | dnode_id | 无 |
| VNODES | 无 | 无 |
| ALIVE | 无 | database |
| CLUSTER ALIVE | 无 | 无 |
| VIEWS | 无 | database, like_pattern |
| CREATE VIEW | view_name | database |
| COMPACTS | 无 | 无 |
| COMPACT | compact_id | 无 |
| DISK_INFO | 无 | database |
| SCANS | 无 | 无 |
| SCAN | scan_id | 无 |
| SSMIGRATES | 无 | 无 |
| TOKENS | 无 | 无 |
| CREATE RSMA | rsma_name | database |
| RSMAS | 无 | database |
| RETENTIONS | 无 | database |
| RETENTION | retention_id | 无 |
| TSMAS | 无 | database |

#### 4.1.2 参数

SHOW 规则中各类参数的作用说明列表

| Parameter | Type | Description | Required / Optional Rules |
| --- | --- | --- | --- |
| metadata | string (enum) | List of metadata items to show (e.g., databases, stables, tables, users). Acts as the selector for the SHOW rule. | **Required** (determines SHOW behavior) |
| database | string | Database name used to scope SHOW command. | **Required for**: CREATE DATABASE **Optional for**: TABLES, DISK_INFO, RETENTIONS, INDEXES, CREATE VIEW, TABLE DISTRIBUTED, VTABLES, STREAMS, CREATE VTABLE, STABLES, TABLE TAGS, VIEWS, VGROUPS, ALIVE, CREATE STABLE, CREATE TABLE, VARIABLES, TSMAS, TAGS, CREATE RSMA, RSMAS |
| table | string | Table name used to scope SHOW command. | **Required for**: TAGS, INDEXES, TABLE DISTRIBUTED, CREATE VTABLE, TABLE TAGS, CREATE STABLE, CREATE TABLE |
| like_pattern | string | LIKE pattern used to filter SHOW results. Supports '%' and '_' wildcards and is case-sensitive. | **Optional for**: VTABLES, INSTANCES, STABLES, VIEWS, CLUSTER VARIABLES, VARIABLES, LOCAL VARIABLES, DNODE VARIABLES, TABLES |
| dnode_id | number | ID of the dnode. | **Required for**: DNODE VARIABLES, VNODES ON DNODE |
| xnode_type | string (enum: tasks | agents | jobs) | Type of taosx node. | **Required for**: XNODE |
| transaction_id | number | ID of the transaction. | **Required for**: TRANSACTION |
| compact_id | number | ID of the compact. | **Required for**: COMPACT |
| scan_id | number | ID of the scan. | **Required for**: SCAN |
| retention_id | number | ID of the retention. | **Required for**: RETENTION |
| rsma_name | string | Name of the rsma used to scope SHOW command. | **Required for**: CREATE RSMA |
| view_name | string | View name used to scope SHOW command. | **Required for**: CREATE VIEW |
| tag_list | string | Comma-separated list of tags to show for a super table. | **Optional for**: TABLE TAGS |
| max_rows | number | Maximum number of rows to return. -1 means no limit. | **Required for**: TABLES, VTABLES, STABLES, CONNECTIONS, QUERIES |

#### 4.1.3 示例

1. 参数传递错误
```sql {wrap}
2026-01-22T09:53:10.479+08:00 [info] [mcp.config.usrlocalmcp.mcp-tdengine-tsdb] MCPServerManager#callTool (show): {"metadata":["USERS"]}
2026-01-22T09:53:10.479+08:00 [info] [mcp.config.usrlocalmcp.mcp-tdengine-tsdb] MCPServerManager#callTool (show) result: {"content":[{"type":"text","text":"metadata argument must be a string"}],"isError":true}
```

1. 参数传递正确
```sql {wrap}
2026-01-22T09:53:16.254+08:00 [info] [mcp.config.usrlocalmcp.mcp-tdengine-tsdb] MCPServerManager#callTool (show): {"metadata":"USERS"}
2026-01-22T09:53:16.276+08:00 [info] [mcp.config.usrlocalmcp.mcp-tdengine-tsdb] MCPServerManager#callTool (show) result: {"content":[{"type":"text","text":"name,super,enable,sysinfo,createdb,create_time,totp,allowed_host,allowed_datetime,roles\nroot,1,1,1,1,2026-01-15 17:14:50.22 +0800 CST,0,\"+127.0.0.1/32, +::1/128\u0000 \",+ALL,\"SYSAUDIT,SYSDBA,SYSSEC\u0000\"\n"}]}
```

### 4.2 info

`info` 用于获取 **TDengine 数据库服务器的基础运行信息**，包括当前时间、时区、客户端与服务器版本、服务器状态、当前用户以及当前数据库。
 该工具主要用于 **健康检查、环境确认、调试与诊断场景**。
当 MCP Client 调用 `info` 工具时：
- 不需要任何输入参数
- 工具始终返回当前连接上下文对应的 TDengine 实例信息
示例：
```sql {wrap}
2026-01-22T09:52:20.975+08:00 [info] [mcp.config.usrlocalmcp.mcp-tdengine-tsdb] MCPServerManager#callTool (info): {}
2026-01-22T09:52:21.007+08:00 [info] [mcp.config.usrlocalmcp.mcp-tdengine-tsdb] MCPServerManager#callTool (info) result: {"content":[{"type":"text","text":"current_time,timezone,client_version,server_version,server_status,current_user,current_database\n2026-01-22 09:52:20.994 +0800 CST,\"Asia/Shanghai (UTC, +0800)\",3.4.0.0.enterprise,3.4.0.0.enterprise,1,root@max,test\n"}]}
```

### 4.3 describe_table

`describe_table` 用于获取 **指定 TDengine 表的结构定义（schema）**。
 该工具返回表中各列的名称、类型、长度及相关属性，适用于 **表结构检查、调试、自动化建模与元数据分析** 场景。

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| table | string | Yes | The name of the table to describe |
| database | string | No | The name of the database containing the table |

示例：
```sql {wrap}
2026-01-22T09:56:18.249+08:00 [info] [mcp.config.usrlocalmcp.mcp-tdengine-tsdb] MCPServerManager#callTool (describe_table): {"database":"test","table":"meters"}
2026-01-22T09:56:18.255+08:00 [info] [mcp.config.usrlocalmcp.mcp-tdengine-tsdb] MCPServerManager#callTool (describe_table) result: {"content":[{"type":"text","text":"field,type,length,note,encode,compress,level\nts,TIMESTAMP,8,,delta-i,lz4,medium\ncurrent,FLOAT,4,,bss,lz4,medium\nvoltage,INT,4,,simple8b,lz4,medium\nphase,FLOAT,4,,bss,lz4,medium\ngroupid,INT,4,TAG,disabled,disabled,disabled\nlocation,VARCHAR,24,TAG,disabled,disabled,disabled\n"}]}
```

### 4.4 query

`query` 用于执行 **TDengine 的只读查询语句**，仅支持 `SELECT` 类型的 SQL。
 该工具主要用于 **数据查询、调试、分析以及 Agent 驱动的数据读取场景**，并通过参数限制避免对数据库造成过大负载。

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| query | string | Yes | The TDengine query to execute. Must be a SELECT statement. |
| max_rows | number | Yes (defaulted) | The maximum number of rows to return. Default is 500. `-1` means no limit. |

示例：
1. 错误语句
```sql {wrap}
2026-01-22T09:57:55.907+08:00 [info] [mcp.config.usrlocalmcp.mcp-tdengine-tsdb] MCPServerManager#callTool (query): {"query":"SELECT field, unit FROM information_schema.ins_columns WHERE table_name = 'meters' AND db_name = 'test'","max_rows":10}
2026-01-22T09:57:55.913+08:00 [info] [mcp.config.usrlocalmcp.mcp-tdengine-tsdb] MCPServerManager#callTool (query) result: {"content":[{"type":"text","text":"[0x2602] Invalid column name: field"}],"isError":true}
```

1. 非 select 语句
```sql {wrap}
2026-01-22T09:58:02.907+08:00 [info] [mcp.config.usrlocalmcp.mcp-tdengine-tsdb] MCPServerManager#callTool (query): {"query":"SHOW CREATE TABLE test.meters","max_rows":10}
2026-01-22T09:58:02.908+08:00 [info] [mcp.config.usrlocalmcp.mcp-tdengine-tsdb] MCPServerManager#callTool (query) result: {"content":[{"type":"text","text":"only select statements are allowed"}],"isError":true}
```

1. 正确语句
```sql {wrap}
2026-01-22T10:01:32.233+08:00 [info] [mcp.config.usrlocalmcp.mcp-tdengine-tsdb] MCPServerManager#callTool (query): {"query":"SELECT * FROM test.meters LIMIT 10","max_rows":10}
2026-01-22T10:01:32.249+08:00 [info] [mcp.config.usrlocalmcp.mcp-tdengine-tsdb] MCPServerManager#callTool (query) result: {"content":[{"type":"text","text":"ts,current,voltage,phase,groupid,location\n2017-07-14 10:40:00 +0800 CST,6.835898,253,148,10,California.SanDiego\n2017-07-14 10:40:00 +0800 CST,6.835898,253,148,3,California.Campbell\n2017-07-14 10:40:00 +0800 CST,6.835898,253,148,5,California.PaloAlto\n2017-07-14 10:40:00 +0800 CST,6.835898,253,148,10,California.Sunnyvale\n2017-07-14 10:40:00 +0800 CST,6.835898,253,148,5,California.SanDiego\n2017-07-14 10:40:00 +0800 CST,6.835898,253,148,9,California.Campbell\n2017-07-14 10:40:00 +0800 CST,6.835898,253,148,7,California.MountainView\n2017-07-14 10:40:00 +0800 CST,6.835898,253,148,6,California.PaloAlto\n2017-07-14 10:40:00 +0800 CST,6.835898,253,148,8,California.SanJose\n2017-07-14 10:40:00 +0800 CST,6.835898,253,148,9,California.SantaClara\n"}]}
```

### 4.5 get_schema_overview

`get_schema_overview` 用于返回 **TDengine 数据库结构的概要描述信息**，
 包括数据库、超级表、字段、标签等对象的 **定义性说明信息**（如名称、描述、单位等）。
该工具返回的内容为 **预先准备的静态描述文本**，并不保证覆盖数据库中的所有对象。
MCP Server 启动时：
- 从配置项 `SchemaOverviewFilePath`读取数据库结构描述文件
- 若配置项非空：
  - 读取文件内容，读取失败直接退出
  - 将其作为静态文本注入到 `get_schema_overview` Tool 中
- 若配置为空则不注册此工具
示例：
```sql {wrap}
2026-01-22T10:03:38.903+08:00 [info] [mcp.config.usrlocalmcp.mcp-tdengine-tsdb] MCPServerManager#callTool (get_schema_overview): {}
2026-01-22T10:03:38.903+08:00 [info] [mcp.config.usrlocalmcp.mcp-tdengine-tsdb] MCPServerManager#callTool (get_schema_overview) result: {"content":[{"type":"text","text":"[\r\n  {\r\n    \"db\": \"test\",\r\n    \"desc\": \"This is a test database.\",\r\n    \"stables\": [\r\n      {\r\n        \"name\": \"meters\",\r\n        \"desc\": \"Table for storing meter readings.\",\r\n        \"cols\": [\r\n          {\r\n            \"name\": \"ts\",\r\n            \"desc\": \"Timestamp of the meter reading.\",\r\n            \"unit\": \"ms\"\r\n          },\r\n          {\r\n            \"name\": \"current\",\r\n            \"desc\": \"Current reading in amperes.\",\r\n            \"unit\": \"A\"\r\n          },\r\n            {\r\n                \"name\": \"voltage\",\r\n                \"desc\": \"Voltage reading in volts.\",\r\n                \"unit\": \"V\"\r\n            },\r\n            {\r\n              \"name\": \"phase\",\r\n                \"desc\": \"Phase of the electrical system.\",\r\n            }\r\n        ],\r\n        \"tags\": [\r\n          {\r\n            \"name\": \"groupid\",\r\n            \"desc\": \"Identifier for the group.\"\r\n          },\r\n          {\r\n            \"name\": \"location\",\r\n            \"desc\": \"Location of the meter.\"\r\n          }\r\n        ]\r\n      }\r\n    ]\r\n  }\r\n]"}]}
```

### 4.6 MCP 配置

支持命令行和环境变量两种配置方式，命令行优先于环境变量，支持以下配置项

| Config Field | Command Line Flag | Environment Variable | Description |
| --- | --- | --- | --- |
| `Host` | `--host` | `TDENGINE_HOST` | TDengine server hostname |
| `User` | `--user` | `TDENGINE_USER` | TDengine username |
| `Pass` | `--pass` | `TDENGINE_PASS` | TDengine password |
| `Port` | `--port` | `TDENGINE_PORT` | TDengine server port |
| `DB` | `--db` | `TDENGINE_DB` | Default database name |
| `DSN` | `--dsn` | `TDENGINE_DSN` | Full TDengine DSN string |
| `SchemaOverviewFilePath` | `--schema_overview_file` | `TDENGINE_SCHEMA_OVERVIEW_FILE` | Path to schema overview description file |

## 5. 性能

无。

## 6. 安全

1. tdengine_show 限定了可以执行的 show 语句
2. tdengine_query 限制执行执行 select 语句，并可设置返回结果数量
3. 配置支持环境变量避免泄漏信息

## 7. 兼容性

支持 TDengine 3.3.6.0 及以上
MCP 版本兼容："2024-11-05"、"2025-03-26"、"2025-06-18"

## 8. 运维

无

## 9. 使用场景

主要面向 **LLM / Agent / 自动化系统**，通过标准化 Tool 接口，安全、可控地访问 TDengine 的 **元数据、结构信息与查询能力**。

## 10. 约束和限制

- `tdengine_query` **仅支持 SELECT 语句**
- 明确禁止以下类型的 SQL：
  - DDL（CREATE / DROP / ALTER）
  - DML（INSERT / UPDATE / DELETE）
  - 管理类语句（GRANT / REVOKE / COMPACT 等）
**约束原因**
- 防止数据被修改或破坏
- 降低 MCP Tool 被 LLM 误用的风险

## 11. 常见错误和排查

1. show command requires metadata argument 未指定 show 规则
2. metadata argument must be a string 传输show规则不是字符串
3. unsupported show metadata item: xxx 未知show规则
4. missing required parameter 'xxx' for show xxx  show规则缺少必要参数
5. xxx parameter is required 必要参数未提供
6. xxx  parameter must be a string 期待字符串参数非字符串
7. xxx  parameter is required and cannot be empty 必要字符串参数清除空白后为空
8. xxx  parameter can not parse to int, val: yyy 数字参数解析失败
9. xxx  parameter must be a number, val: yyy 数字参数类型错误
10. TDengine 错误见返回的错误码和错误内容

## 12. 可观测性

所有错误通过 mcp 请求结果返回，通过 mcp 客户端可见具体错误

## 13. 安装和卸载

下载 mcp-tdengine-tsdb 后在 AI 工具的 mcp 配置中添加如下配置
Command 为 go-mcp-tsdb 执行程序路径
Args 为启动参数，db 参数需要显示设置，其余支持的参数见 4.6 节
```json {wrap}
{
  "mcpServers": {
    "mcp-tdengine-tsdb": {
      "command": "E:\\self\\go-mcp-tsdb\\mcp-tdengine-tsdb.exe",
      "args": [
        "-db",
        "test",
        "-schema_overview_file",
        "E:\\self\\go-mcp-tsdb\\test\\desc.json"
      ]
    }
  }
}
```

## 14. 文档

需要修改文档

## 15. 参考文档

## 16. 附录
