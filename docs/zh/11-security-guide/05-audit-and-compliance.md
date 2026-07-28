---
sidebar_label: 审计与合规
title: 审计与合规
description: TDengine 审计日志配置与查看，以及安全公告入口
toc_max_heading_level: 4
---

TDengine 先对用户操作进行记录和管理，然后将这些作为审计日志发送给 `taosKeeper`，再由 `taosKeeper` 保存至任意 TDengine 集群。管理员可通过审计日志进行安全监控、历史追溯。自 `v3.4.1.0` 起，也可将审计保存在本集群（`auditSaveInSelf`），而不发送给 `taosKeeper`。审计日志功能的开启和关闭操作非常简单，只须修改 TDengine 的配置文件后重启服务（部分参数亦可按支持情况通过 SQL 动态修改）。参数权威说明见 [taosd](../12-operations-and-tooling/03-components/01-taosd.md)。

本文中的“合规”指：通过可配置的审计轨迹支撑内部审计与运维追溯，以及及时跟进 [安全公告](./07-security-advisories.md) 中的漏洞修复。文档不声称特定外部认证或法规符合性结论。

## 审计日志

### taosd 配置

审计日志由数据库服务 `taosd` 产生，其相应参数要配置在 `taos.cfg` 配置文件中，详细参数如下表。

| 参数名称 | 参数含义 |
| --- | --- |
| `audit`             | 是否打开审计日志，`1` 为开启，`0` 为关闭。企业版默认开启（`1`） |
| `monitorFqdn`       | 接收审计日志的 `taosKeeper` 所在服务器的 FQDN |
| `monitorPort`       | 接收审计日志的 `taosKeeper` 服务所用端口 |
| `monitorCompaction` | 上报数据时是否进行压缩 |
| `auditLevel`        | 审计级别，不同级别记录不同的审计操作，具体参看操作列表；默认 `3`（数据库级），自 `v3.4.0.0` |
| `auditHttps`        | 发送审计记录给 `taosKeeper` 时是否使用 HTTPS 协议；默认 `0`，自 `v3.4.0.0` |
| `auditUseToken`     | 上报时是否使用 Token；默认 `1`，自 `v3.4.0.0` |
| `auditCreateTable`  | 是否对创建子表开启审计；默认 `1` |
| `auditSaveInSelf`   | 是否将审计保存在本集群而不发给 `taosKeeper`；默认 `0`，自 `v3.4.1.0` |

完整类型、取值范围与动态修改说明见 [taosd 参考手册](../12-operations-and-tooling/03-components/01-taosd.md)。

### 创建审计库

在打开审计开关后，需要创建审计库，在创建时需要指定 `IS_AUDIT` 参数。

```sql
CREATE DATABASE [IF NOT EXISTS] db_name [database_options] IS_AUDIT 1;

database_options:
    database_option ...

database_option: {
  DURATION value
}

database_option: {
  WAL_LEVEL value
}

database_option: {
  ENCRYPT_ALGORITHM value
}
```

另外，作为审计库，`KEEP` 默认为 `1825d`，如果指定 `KEEP`，要求大于 `1825d`；`WAL_LEVEL` 默认为 `2`，不能更改；`ENCRYPT_ALGORITHM` 不能指定为 `none`，可以选择任意一种 CBC 模式的对称加密算法；`PRECISION` 默认为纳秒（`ns`），不能更改为其他精度。

在 `v3.4.0.0` 之前版本创建的审计库，与 `v3.4.0.0` 及之后版本的审计库不兼容。`v3.4.0.0` 之前版本的审计库无法开启 `IS_AUDIT` 参数，因此不会对 `DURATION`、`WAL_LEVEL`、`ENCRYPT_ALGORITHM` 做强制要求。对于 `v3.4.0.0` 之前创建的审计库，如需使用新版本的审计能力，建议先 `DROP` 该审计库后再重新创建。如果要在 `v3.4.0.0` 之后的版本中继续使用由 `v3.4.0.0` 之前版本创建的审计库，则需要将 `auditUseToken` 关闭（设置为 `0`）。

在 `v3.4.1.0` 之后的版本可以将审计信息保存在自身，而不发送给 `taosKeeper`。若要使用该功能，需要将参数 `auditSaveInSelf` 设置为 `1`，并且在使用该功能时，创建的审计库的 `vgroups` 数量只能为 `1`。

### taosKeeper 配置

在 `taosKeeper` 的配置文件 `keeper.toml` 中配置与审计日志有关的配置参数，如下表所示。

| 参数名称 | 参数含义 |
| --- | --- |
| `auditDB` | 用于存放审计日志的数据库的名字，默认值为 `"audit"`。`taosKeeper` 在收到上报的审计日志后会判断该数据库是否存在，如果不存在会自动创建 |

### 数据格式

上报的审计日志格式如下：

```json
{
    "ts": timestamp,
    "cluster_id": string,
    "user": string,
    "operation": string,
    "db": string,
    "resource": string,
    "client_add": string,
    "details": string,
    "affected_rows": integer,
    "duration": double
}
```

### 表结构

`taosKeeper` 会依据上报的审计数据在相应的数据库中自动建立超级表用于存储数据。该超级表的定义如下：

```sql
CREATE STABLE operations (
  ts TIMESTAMP,
  user_name VARCHAR(25),
  operation VARCHAR(20),
  db VARCHAR(65),
  resource VARCHAR(193),
  client_address VARCHAR(64),
  details VARCHAR(50000)
) TAGS (cluster_id VARCHAR(64));
```

其中：

1. `db` 为操作涉及的 database，`resource` 为操作涉及的资源。
2. `user_name` 和 `operation` 为数据列，表示哪个用户在该对象上进行了什么操作。
3. `ts` 为时间戳列，表示操作发生时的时间。
4. `details` 为该操作的一些补充细节，在大多数操作下是所执行的操作的 SQL 语句。
5. `client_address` 为客户端地址，包括 IP 和端口。

### 操作列表

目前审计日志中所记录的操作列表以及每个操作中各字段的含义如下（因为每个操作的施加者，即 `user`、`client_add`、时间戳字段在所有操作中的含义相同，下表不再描述）。级别越高，在较低级别基础上覆盖更多对象。

`auditLevel = 1`（`AUDIT_LEVEL_SYSTEM`）

| 操作 | Operation | DB | Resource | Details |
| --- | --- | --- | --- | --- |
| create dnode | createDnode | NULL | IP:Port 或 FQDN:Port | SQL |
| drop dnode | dropDnode | NULL | dnodeId | SQL |
| alter dnode | alterDnode | NULL | dnodeId | SQL |
| create mnode | createMnode | NULL | dnodeId | SQL |
| drop mnode | dropMnode | NULL | dnodeId | SQL |
| create qnode | createQnode | NULL | dnodeId | SQL |
| drop qnode | dropQnode | NULL | dnodeId | SQL |
| restore dnode | restoreDnode | NULL | dnodeId | SQL |

`auditLevel = 2`（`AUDIT_LEVEL_CLUSTER`）

| 操作 | Operation | DB | Resource | Details |
| --- | --- | --- | --- | --- |
| alter cluster | alterCluster | NULL | NULL | SQL |
| balance vgroup leader | balanceVgroupLead | NULL | NULL | SQL |
| redistribute vgroup | redistributeVgroup | NULL | vgroupId | SQL |
| balance vgroup | balanceVgroup | NULL | vgroupId | SQL |
| assign leader | assignLeader | NULL | NULL | SQL |
| grant privileges | grantPrivileges | NULL | 所授予的用户 | SQL |
| revoke privileges | revokePrivileges | NULL | 被收回权限的用户 | SQL |
| login | login | NULL | NULL | appName |
| create user | createUser | NULL | 被创建的用户名 | 用户属性参数（password 除外） |
| alter user | alterUser | NULL | 被修改的用户名 | 修改密码记录被修改的参数和新值（password 除外），其他操作记录 SQL |
| drop user | dropUser | NULL | 被删除的用户名 | SQL |
| create mount | createMount | mountName | NULL | SQL |
| drop mount | dropMount | mountName | NULL | SQL |
| kill retention | killRetention | db name | NULL | SQL |
| auto trimDB | autoTrimDB | db name | NULL | SQL |
| create encrypt algr | createEncryptAlgr | NULL | algorithmId | SQL |
| drop encrypt algr | dropEncryptAlgr | NULL | algorithmId | SQL |

`auditLevel = 3`（`AUDIT_LEVEL_DATABASE`）

| 操作 | Operation | DB | Resource | Details |
| --- | --- | --- | --- | --- |
| create database | createDB | db name | NULL | SQL |
| alter database | alterDB | db name | NULL | SQL |
| drop database | dropDB | db name | NULL | SQL |
| compact database | compact | database name | NULL | SQL |
| kill compact | killCompact | db name | NULL | SQL |
| create stable | createStb | db name | stable name | SQL |
| alter stable | alterStb | db name | stable name | SQL |
| drop stable | dropStb | db name | stable name | SQL |
| create stream | createStream | NULL | stream 名 | SQL |
| drop stream | dropStream | NULL | stream 名 | SQL |
| recalc stream | recalcStream | streamName | recalcName | SQL |
| create topic | createTopic | topic 所在 DB | topic 名 | SQL |
| drop topic | dropTopic | topic 所在 DB | topic 名 | SQL |
| reload topic | reloadTopic | topic 所在 DB | topic 名 | SQL |
| create Rsma | createRsma | Rsma name | NULL | SQL |
| alter Rsma | alterRsma | Rsma name | Table name | SQL |
| drop Rsma | dropRsma | Rsma name | NULL | SQL |
| create View | createView | Db name | NULL | SQL |
| drop View | dropView | Db name | view name | SQL |

`auditLevel = 4`（`AUDIT_LEVEL_CHILDTABLE`）

| 操作 | Operation | DB | Resource | Details |
| --- | --- | --- | --- | --- |
| create table | createTable | db name | table name | SQL |
| drop table | dropTable | db name | table name | SQL |

### 查看审计日志

在 `taosd` 和 `taosKeeper`（或已启用 `auditSaveInSelf`）都正确配置并启动之后，随着系统的不断运行，系统中的各种操作（如上表所示）会被实时记录并上报。你可以登录 taosExplorer，点击**系统管理** → **审计**页面，即可查看审计日志；也可以在 `taos` shell 中直接查询相应的库和表。

## 安全公告与漏洞披露

已知安全漏洞、受影响版本与修复版本统一发布在 [安全公告](./07-security-advisories.md)。若发现未公开漏洞，请按该页说明的私密渠道报告，勿在公开论坛或 Issue 中讨论未修复问题。

配置加固与部署建议见 [安全部署配置建议](./06-security-suggestions.md)。
