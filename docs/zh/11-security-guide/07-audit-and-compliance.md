---
sidebar_label: 审计与合规
title: 审计与合规
description: TDengine 审计日志配置与查看，以及安全公告入口
toc_max_heading_level: 4
---

import { Enterprise } from './resources/_resources.mdx';

<Enterprise/>

TDengine 企业版可将用户与系统操作记录为审计日志，供安全监控与历史追溯。审计日志可经 `taosKeeper` 写入目标集群的审计库，也可自 `v3.4.1.0` 起保存在本集群（`auditSaveInSelf`）。开启或关闭审计、调整级别等，可修改 `taos.cfg` 后重启相应 `taosd`；多数 `audit*` 参数亦支持 SQL 动态修改（见下文）。参数权威说明见 [taosd](../12-operations-and-tooling/03-components/01-taosd.md)。

本文中的“合规”指：通过可配置的审计轨迹支撑内部审计与运维追溯，以及及时跟进 [安全公告](./09-security-advisories.md) 中的漏洞修复。文档不声称特定外部认证或法规符合性结论。

审计库权限与三权分立（`SYSAUDIT` / `SYSAUDIT_LOG`）详见 [权限管理 · 审计数据库](../05-tdengine-sql/07-user-and-privilege/02-grant.md#审计数据库)。审计库加密要求见 [静态数据保护](./06-data-security.md)。落盘侧防篡改要点亦见 [全链路高可靠](./03-full-trace-reliability.md)。

## 版本与能力演进

| 版本 | 能力 |
|------|------|
| `v3.1.0.0` / `v3.1.1.0` | 企业版审计开关与上报间隔；经 `taosKeeper` 落库 |
| `v3.3.9.0` | 引入 `IS_AUDIT` 关键字（审计库标识） |
| `v3.4.0.0` | 审计级别 1–5、`auditHttps` / `auditUseToken`；审计库强制加密 / `KEEP` / `WAL_LEVEL` / 纳秒精度；`SYSAUDIT` 模型 |
| `v3.4.1.0` | `auditSaveInSelf`：本集群直写，可不经 `taosKeeper` |

建议使用最新企业版。社区版不提供审计能力。

## 1. 落库路径概览

有两条互斥的主路径：

| 路径 | 条件 | 说明 |
|------|------|------|
| 经 `taosKeeper` | `auditSaveInSelf = 0`（默认） | `taosd` 按 `auditInterval` 上报 → Keeper → 目标集群审计库（可为异地集群） |
| 记录到本集群 | `auditSaveInSelf = 1`（`v3.4.1.0+`） | 经集群内部 RPC 写入本集群审计库；`monitorFqdn` / `monitorPort` / `monitorCompaction` / `auditHttps` / `auditUseToken` **不再生效**；**不支持**把审计写到其他集群 |

> **与监控指标区分**：`taosKeeper` 指标默认写入 `log` 库；审计写入带 `IS_AUDIT` 标识的审计库（默认名常为 `audit`）。二者不是同一库。

### 1.1 经 taosKeeper 上报

1. `taosd` 配置 `audit = 1`，并设置 `monitorFqdn` / `monitorPort` 指向 Keeper。
2. 目标侧创建符合约束的审计库，或由 Keeper 按配置自动创建（见下文）。
3. 如需 Token 上报，配置 `auditUseToken = 1`，并为写入侧准备具备 `SYSAUDIT_LOG` 的账号 / Token（见 [权限管理](../05-tdengine-sql/07-user-and-privilege/02-grant.md)）。

### 1.2 记录到本集群

```sql
-- 创建加密审计库（VGROUPS 必须为 1）
CREATE DATABASE audit VGROUPS 1 IS_AUDIT 1
  ENCRYPT_ALGORITHM 'SM4-CBC' WAL_LEVEL 2 PRECISION 'ns';

ALTER ALL DNODES 'audit' '1';
ALTER ALL DNODES 'auditSaveInSelf' '1';
ALTER ALL DNODES 'auditLevel' '3';   -- 按需调高至 4 / 5

-- 查询（库名以实际创建为准）
SELECT * FROM audit.operations ORDER BY ts DESC LIMIT 100;
```

## 2. taosd 参数

| 参数 | 含义 | 企业版默认 | 引入版本 |
|-------------------------------|------|------------|----------|
| `audit`                       | 审计总开关（`0`/`1`） | `1` | `v3.1.0.0` |
| `auditInterval`               | 上报间隔（毫秒） | `5000` | `v3.1.0.0` |
| `auditLevel`                  | 审计级别：`0` 关级别；`1` 系统～`5` 数据；高级别包含低级别 | `3`（库级） | `v3.4.0.0` |
| `auditHttps`                  | 上报 Keeper 是否 HTTPS | `0` | `v3.4.0.0` |
| `auditUseToken`               | 上报是否使用 Token | `1` | `v3.4.0.0` |
| `auditCreateTable`            | 是否对**创建子表**记审计（须同时满足 `auditLevel ≥ 4`） | `1` | `v3.1.0.0` |
| `auditSaveInSelf`             | 是否本集群直写、不发给 Keeper | `0` | `v3.4.1.0` |
| `monitorFqdn` / `monitorPort` | Keeper 地址（经 Keeper 路径使用） | — | — |
| `monitorCompaction`           | 上报是否压缩 | — | — |

完整类型、取值范围与动态修改说明见 [taosd](../12-operations-and-tooling/03-components/01-taosd.md)。多数 `audit*` 参数支持 SQL 动态修改，例如：

```sql
ALTER ALL DNODES 'auditLevel' '5';
SHOW VARIABLES LIKE 'audit%';
```

:::note
`enableAuditSelect` / `enableAuditInsert` / `enableAuditDelete` 等细粒度开关若出现在配置说明中，属内部/测试向参数；公开部署以 `auditLevel = 5` 覆盖 insert/select/delete 即可，不必单独配置。
:::

## 3. 创建审计库

打开审计后须存在审计库，创建时指定 `IS_AUDIT 1`：

```sql
CREATE DATABASE [IF NOT EXISTS] db_name [database_options] IS_AUDIT 1;
```

示例（本集群直写场景常用组合）：

```sql
CREATE DATABASE audit VGROUPS 1 IS_AUDIT 1
  ENCRYPT_ALGORITHM 'SM4-CBC' WAL_LEVEL 2 PRECISION 'ns' KEEP 1825d;
```

约束（`v3.4.0.0+`，由服务端强制）：

| 项 | 要求 |
|----|------|
| 数量 | 集群内仅允许一个审计库（以 `is_audit` 标识，名称不固定） |
| `VGROUPS`           | 必须为 `1`（经 Keeper 与本集群直写均如此） |
| `KEEP`              | 默认 `1825d`（5 年）；若指定，须 ≥ `1825d` |
| `WAL_LEVEL`         | 默认 `2`，不可改为其他值 |
| `ENCRYPT_ALGORITHM` | 不能为 `none`，须为 CBC 对称算法（如 `'SM4-CBC'`） |
| `PRECISION`         | 默认 `ns`，不可改为其他精度 |
| `ALLOW_DROP`        | 审计库默认 `0`；删除前须改为 `1`，且仅 `SYSAUDIT` 可删改审计库 |

创建审计库时，系统会在同一事务中自动创建 `operations` 超级表（经 Keeper 路径则由 Keeper 自动建表 / 补列）。

**升级兼容**

- `v3.4.0.0` 之前创建的审计库与新规则不兼容：旧库无法按新语义开启 `IS_AUDIT`，也不强制上述 `DURATION` / `WAL` / 加密约束。建议 `DROP` 后按新规则重建。
- 若必须在 `v3.4.0.0+` 继续消费旧库数据，可将 `auditUseToken` 设为 `0`（权宜措施）。

DDL 入口见 [数据库 · IS_AUDIT](../05-tdengine-sql/02-ddl/01-database.md)。

## 4. taosKeeper 配置

配置文件一般为 `/etc/taos/taoskeeper.toml`（企业版示例见安装包内 `taoskeeper_enterprise.toml`）。与审计相关的段落示意：

```toml
[audit]
enable = true
[audit.database]
name = "audit"
[audit.database.options]
vgroups = 1
buffer = 16
cachemodel = "both"
```

| 配置 | 含义 |
|--------------------------|------|
| `audit.enable`           | 是否启用审计接收 |
| `audit.database.name`    | 审计库名，默认 `"audit"`；不存在时可自动创建 |
| `audit.database.options` | 建库选项（如 `vgroups = 1`） |

Keeper 同时负责监控指标（默认写入 `log` 库），与审计库分离。组件说明见 [taosKeeper](../12-operations-and-tooling/03-components/05-taoskeeper.md)。

## 5. 数据格式与表结构

上报 JSON 示意：

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

落库超级表（与当前 Keeper / `auditSaveInSelf` 路径一致）：

```sql
CREATE STABLE IF NOT EXISTS operations (
  ts TIMESTAMP,
  user_name VARCHAR(25),
  operation VARCHAR(20),
  db VARCHAR(65),
  resource VARCHAR(193),
  client_address VARCHAR(64),
  details VARCHAR(50000),
  affected_rows BIGINT UNSIGNED,
  `duration` DOUBLE
) TAGS (cluster_id VARCHAR(64));
```

说明：

- JSON 字段 `client_add` 对应列 `client_address`（含 IP 与端口）。
- `db` / `resource` 为操作涉及的库与对象；`details` 多数情况下为 SQL（密码等敏感字段会省略）。
- `affected_rows` / `duration` 主要用于数据级（级别 5）等场景；旧表可能由 Keeper 自动 `ALTER` 补列。

## 6. 操作列表

级别越高，在较低级别基础上覆盖更多对象。`auditLevel = N` 表示记录级别 `1…N` 的操作。因操作施加者（JSON 的 `user` / `client_add`）与时间戳在各行含义相同，下表不再重复说明。

### 6.1 `auditLevel = 1`（系统）

| 操作 | Operation | DB | Resource | Details |
| ------------- | ------------ | ---- | ------- | --- |
| create dnode  | createDnode  | NULL | IP:Port 或 FQDN:Port | SQL |
| drop dnode    | dropDnode    | NULL | dnodeId | SQL |
| alter dnode   | alterDnode   | NULL | dnodeId | SQL |
| create mnode  | createMnode  | NULL | dnodeId | SQL |
| drop mnode    | dropMnode    | NULL | dnodeId | SQL |
| create qnode  | createQnode  | NULL | dnodeId | SQL |
| drop qnode    | dropQnode    | NULL | dnodeId | SQL |
| restore dnode | restoreDnode | NULL | dnodeId | SQL |

### 6.2 `auditLevel = 2`（集群）

| 操作 | Operation | DB | Resource | Details |
| --------------------- | ------------------ | --------- | --- | --- |
| alter cluster         | alterCluster       | NULL      | NULL | SQL |
| balance vgroup leader | balanceVgroupLead  | NULL      | NULL | SQL |
| redistribute vgroup   | redistributeVgroup | NULL      | vgroupId | SQL |
| balance vgroup        | balanceVgroup      | NULL      | vgroupId | SQL |
| assign leader         | assignLeader       | NULL      | NULL | SQL |
| grant privileges      | grantPrivileges    | NULL      | 所授予的用户 | SQL |
| revoke privileges     | revokePrivileges   | NULL      | 被收回权限的用户 | SQL |
| login                 | login              | NULL      | NULL | appName |
| create user           | createUser         | NULL      | 被创建的用户名 | 用户属性（password 除外） |
| alter user            | alterUser          | NULL      | 被修改的用户名 | 改密记参数名与新值（password 除外），其他记 SQL |
| drop user             | dropUser           | NULL      | 被删除的用户名 | SQL |
| create mount          | createMount        | mountName | NULL | SQL |
| drop mount            | dropMount          | mountName | NULL | SQL |
| kill retention        | killRetention      | db name   | NULL | SQL |
| auto trimDB           | autoTrimDB         | db name   | NULL | SQL |
| create encrypt algr   | createEncryptAlgr  | NULL      | algorithmId | SQL |
| drop encrypt algr     | dropEncryptAlgr    | NULL      | algorithmId | SQL |

### 6.3 `auditLevel = 3`（数据库）

| 操作 | Operation | DB | Resource | Details |
| --------------------- | ------------ | --- | --- | --- |
| create database       | createDB     | db name | NULL | SQL |
| alter database        | alterDB      | db name | NULL | SQL |
| drop database         | dropDB       | db name | NULL | SQL |
| compact database      | compact      | database name | NULL | SQL |
| kill compact          | killCompact  | db name | NULL | SQL |
| create stable         | createStb    | db name | stable name | SQL |
| alter stable          | alterStb     | db name | stable name | SQL |
| drop stable           | dropStb      | db name | stable name | SQL |
| create stream         | createStream | NULL | stream 名 | SQL |
| drop stream           | dropStream   | NULL | stream 名 | SQL |
| recalc stream         | recalcStream | streamName | recalcName | SQL |
| create topic          | createTopic  | topic 所在 DB | topic 名 | SQL |
| drop topic            | dropTopic    | topic 所在 DB | topic 名 | SQL |
| reload topic          | reloadTopic  | topic 所在 DB | topic 名 | SQL |
| create Rsma           | createRsma   | Rsma name | NULL | SQL |
| alter Rsma            | alterRsma    | Rsma name | Table name | SQL |
| drop Rsma             | dropRsma     | Rsma name | NULL | SQL |
| create View           | createView   | Db name | NULL | SQL |
| drop View             | dropView     | Db name | view name | SQL |

### 6.4 `auditLevel = 4`（子表）

| 操作 | Operation | DB | Resource | Details |
| ------------ | ----------- | ------- | ---------- | --- |
| create table | createTable | db name | table name | SQL |
| drop table   | dropTable   | db name | table name | SQL |

创建子表还受 `auditCreateTable` 控制：该参数为 `0` 时即使 `auditLevel ≥ 4` 也不记录建子表。

### 6.5 `auditLevel = 5`（数据）

| 操作 | Operation | DB | Resource | Details |
| ------ | ------ | ------- | ---------- | --- |
| insert | insert | db name | table name | SQL |
| select | select | db name | table name | SQL |
| delete | delete | db name | table name | SQL |

数据级审计由客户端在语句成功后上报，写入量可能显著增加，请按合规要求与性能权衡。

## 7. 查看与运维

在 `taosd`（及 Keeper，若未启用本集群直写）配置正确并启动后，可用下列方式查看：

- **taosExplorer**：系统管理 → 审计。
- **SQL**：

```sql
SHOW VARIABLES LIKE 'audit%';
SELECT name, is_audit, allow_drop, `encrypt_algorithm`, precision
  FROM information_schema.ins_databases
  WHERE is_audit = 1;
SELECT ts, user_name, operation, db, resource, client_address, details
  FROM audit.operations
  ORDER BY ts DESC
  LIMIT 100;
```

删除审计库前（仅 `SYSAUDIT`）：

```sql
ALTER DATABASE audit ALLOW_DROP 1;
DROP DATABASE audit;
```

## 8. 权限与防篡改要点

- 写入审计表：`SYSAUDIT_LOG`；查看审计表：`SYSAUDIT`。普通业务账号不应具备审计库写权限。
- 不允许删除/修改审计表或其数据行（权限模型强制）。
- 审计库默认 `ALLOW_DROP = 0`，防止误删。
- 三权分立下将 `SYSDBA` / `SYSSEC` / `SYSAUDIT` 分给不同人员。完整规则见 [权限管理](../05-tdengine-sql/07-user-and-privilege/02-grant.md)。

## 9. 安全公告与漏洞披露

已知安全漏洞、受影响版本与修复版本统一发布在 [安全公告](./09-security-advisories.md)。若发现未公开漏洞，请按该页说明的私密渠道报告，勿在公开论坛或 Issue 中讨论未修复问题。

配置加固与部署建议见 [安全加固建议](./08-security-hardening.md)。

## 10. 相关查阅

| 主题 | 文档 |
|------|------|
| taosd 审计参数 | [taosd](../12-operations-and-tooling/03-components/01-taosd.md) |
| taosKeeper | [taosKeeper](../12-operations-and-tooling/03-components/05-taoskeeper.md) |
| 审计库 DDL / 权限 | [数据库](../05-tdengine-sql/02-ddl/01-database.md)、[权限管理](../05-tdengine-sql/07-user-and-privilege/02-grant.md#审计数据库) |
| 审计库加密 | [静态数据保护](./06-data-security.md) |
| 全链路认证中的审计摘要 | [全链路认证](./01-full-trace-auth.md) |
