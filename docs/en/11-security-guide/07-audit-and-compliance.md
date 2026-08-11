---
sidebar_label: Audit and Compliance
title: Audit and Compliance
description: TDengine audit log configuration and viewing, plus security advisory entry points
toc_max_heading_level: 4
---

import { Enterprise } from './resources/_resources.mdx';

<Enterprise/>

TDengine Enterprise can record user and system operations as audit logs for security monitoring and historical traceability. Audit logs can be written by `taosKeeper` to an audit database in a target cluster, or stored in the local cluster starting with `v3.4.1.0` by enabling `auditSaveInSelf`. To enable or disable auditing, adjust the audit level, and change related behavior, you can modify `taos.cfg` and restart the corresponding `taosd`; most `audit*` parameters also support dynamic SQL modification, as described below. For authoritative parameter details, see [taosd](../12-operations-and-tooling/03-components/01-taosd.md).

In this document, "compliance" means using configurable audit trails to support internal audits and operational traceability, and promptly applying vulnerability fixes from [Security Advisories](./09-security-advisories.md). This document does not claim compliance with any specific external certification or regulation.

For audit database privileges and separation of duties (`SYSAUDIT` / `SYSAUDIT_LOG`), see [Privileges · Audit Database](../05-tdengine-sql/07-user-and-privilege/02-grant.md#audit-database). For audit database encryption requirements, see [Data-at-Rest Protection](./06-data-security.md). For tamper-resistance notes on persisted data, see [Full-Trace Reliability](./03-full-trace-reliability.md).

## Version and Capability Evolution

| Version | Capability |
|---|---|
| `v3.1.0.0` / `v3.1.1.0` | Enterprise audit switch and reporting interval; records are written through `taosKeeper`. |
| `v3.3.9.0` | Introduced the `IS_AUDIT` keyword to mark an audit database. |
| `v3.4.0.0` | Audit levels 1-5, `auditHttps` / `auditUseToken`; audit database requirements for encryption / `KEEP` / `WAL_LEVEL` / nanosecond precision; `SYSAUDIT` model. |
| `v3.4.1.0` | `auditSaveInSelf`: direct local-cluster writes without `taosKeeper`. |

Use the latest Enterprise edition when possible. The Community edition does not provide audit capabilities.

## 1. Write Path Overview

There are two mutually exclusive primary paths:

| Path | Condition | Description |
|---|---|---|
| Through `taosKeeper` | `auditSaveInSelf = 0` (default) | `taosd` reports according to `auditInterval` -> Keeper -> audit database in the target cluster, which can be a remote cluster. |
| Stored in the local cluster | `auditSaveInSelf = 1` (`v3.4.1.0+`) | Written to the local audit database through internal cluster RPC. `monitorFqdn` / `monitorPort` / `monitorCompaction` / `auditHttps` / `auditUseToken` **no longer take effect**. Writing audit logs to another cluster is **not supported**. |

> **Different from monitoring metrics**: `taosKeeper` metrics are written to the `log` database by default; audit records are written to an audit database marked with `IS_AUDIT`, often named `audit` by default. They are not the same database.

### 1.1 Reporting Through taosKeeper

1. Configure `audit = 1` on `taosd`, and set `monitorFqdn` / `monitorPort` to point to Keeper.
2. Create an audit database that satisfies the constraints on the target side, or let Keeper create it automatically according to its configuration.
3. If token-based reporting is required, set `auditUseToken = 1` and prepare an account or token with `SYSAUDIT_LOG` for the write side; see [Privileges](../05-tdengine-sql/07-user-and-privilege/02-grant.md).

### 1.2 Stored in the Local Cluster

```sql
-- Create an encrypted audit database; VGROUPS must be 1
CREATE DATABASE audit VGROUPS 1 IS_AUDIT 1
  ENCRYPT_ALGORITHM 'SM4-CBC' WAL_LEVEL 2 PRECISION 'ns';

ALTER ALL DNODES 'audit' '1';
ALTER ALL DNODES 'auditSaveInSelf' '1';
ALTER ALL DNODES 'auditLevel' '3';   -- Increase to 4 / 5 as needed

-- Query; use the actual database name you created
SELECT * FROM audit.operations ORDER BY ts DESC LIMIT 100;
```

## 2. taosd Parameters

| Parameter | Meaning | Enterprise Default | Introduced In |
|---|---|---|---|
| `audit` | Master audit switch (`0`/`1`). | `1` | `v3.1.0.0` |
| `auditInterval` | Reporting interval in milliseconds. | `5000` | `v3.1.0.0` |
| `auditLevel` | Audit level: `0` disables levels; `1` system through `5` data. Higher levels include lower levels. | `3` (database) | `v3.4.0.0` |
| `auditHttps` | Whether reporting to Keeper uses HTTPS. | `0` | `v3.4.0.0` |
| `auditUseToken` | Whether reporting uses token authentication. | `1` | `v3.4.0.0` |
| `auditCreateTable` | Whether to audit **child-table creation**; requires `auditLevel >= 4`. | `1` | `v3.1.0.0` |
| `auditSaveInSelf` | Whether to write audit records to the local cluster instead of Keeper. | `0` | `v3.4.1.0` |
| `monitorFqdn` / `monitorPort` | Keeper address, used by the Keeper path. | - | - |
| `monitorCompaction` | Whether reporting is compressed. | - | - |

For complete types, value ranges, and dynamic modification support, see [taosd](../12-operations-and-tooling/03-components/01-taosd.md). Most `audit*` parameters support dynamic SQL modification, for example:

```sql
ALTER ALL DNODES 'auditLevel' '5';
SHOW VARIABLES LIKE 'audit%';
```

:::note
If fine-grained switches such as `enableAuditSelect`, `enableAuditInsert`, or `enableAuditDelete` appear in configuration references, they are internal/test-oriented parameters. For public deployment, `auditLevel = 5` is sufficient to cover insert/select/delete; separate configuration is not required.
:::

## 3. Create an Audit Database {#create-audit-database}

After audit is enabled, an audit database must exist. Create it with `IS_AUDIT 1`:

```sql
CREATE DATABASE [IF NOT EXISTS] db_name [database_options] IS_AUDIT 1;
```

Common combination for local-cluster writes:

```sql
CREATE DATABASE audit VGROUPS 1 IS_AUDIT 1
  ENCRYPT_ALGORITHM 'SM4-CBC' WAL_LEVEL 2 PRECISION 'ns' KEEP 1825d;
```

Constraints, enforced by the server in `v3.4.0.0+`:

| Item | Requirement |
|---|---|
| Quantity | Only one audit database is allowed in a cluster, identified by `is_audit`; the name is not fixed. |
| `VGROUPS` | Must be `1`, both for Keeper and local-cluster write paths. |
| `KEEP` | Default `1825d` (5 years); if specified, it must be &gt;= `1825d`. |
| `WAL_LEVEL` | Default `2`; cannot be changed to another value. |
| `ENCRYPT_ALGORITHM` | Cannot be `none`; must be a CBC symmetric algorithm such as `'SM4-CBC'`. |
| `PRECISION` | Default `ns`; cannot be changed to another precision. |
| `ALLOW_DROP` | Audit database default is `0`; set it to `1` before deletion, and only `SYSAUDIT` can modify or drop the audit database. |

When the audit database is created, the system automatically creates the `operations` supertable in the same transaction. In the Keeper path, Keeper creates the table or adds missing columns automatically.

**Upgrade compatibility**

- Audit databases created before `v3.4.0.0` are incompatible with the new rules. Old databases cannot be enabled with `IS_AUDIT` under the new semantics, and they do not enforce the new `DURATION` / `WAL` / encryption constraints. Recreate them under the new rules when possible.
- If you must continue consuming old audit database data in `v3.4.0.0+`, set `auditUseToken` to `0` as a workaround.

For the DDL entry point, see [Databases · IS_AUDIT](../05-tdengine-sql/02-ddl/01-database.md).

## 4. taosKeeper Configuration

The configuration file is usually `/etc/taos/taoskeeper.toml`; the Enterprise package includes an example named `taoskeeper_enterprise.toml`. Audit-related sections look like this:

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

| Configuration | Meaning |
|---|---|
| `audit.enable` | Whether to enable audit reception. |
| `audit.database.name` | Audit database name; default `"audit"`. It can be created automatically if absent. |
| `audit.database.options` | Database creation options, such as `vgroups = 1`. |

Keeper also handles monitoring metrics, which are written to the `log` database by default and are separate from the audit database. For component details, see [taosKeeper](../12-operations-and-tooling/03-components/05-taoskeeper.md).

## 5. Data Format and Table Schema

Reported JSON example:

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

Supertable schema, aligned with the current Keeper and `auditSaveInSelf` paths:

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

Notes:

- JSON field `client_add` maps to column `client_address`, including IP and port.
- `db` / `resource` identify the database and object involved in the operation. `details` is usually SQL, with sensitive fields such as passwords omitted.
- `affected_rows` / `duration` are mainly used for data-level audit scenarios such as level 5. Old tables may be altered automatically by Keeper to add missing columns.

## 6. Operation List

Higher levels cover more objects in addition to lower levels. `auditLevel = N` records operations for levels `1...N`. Because the actor (`user` / `client_add`) and timestamp have the same meaning in each row, they are not repeated in the tables below.

### 6.1 `auditLevel = 1` (System)

| Operation | Operation | DB | Resource | Details |
|---|---|---|---|---|
| create dnode | createDnode | NULL | IP:Port or FQDN:Port | SQL |
| drop dnode | dropDnode | NULL | dnodeId | SQL |
| alter dnode | alterDnode | NULL | dnodeId | SQL |
| create mnode | createMnode | NULL | dnodeId | SQL |
| drop mnode | dropMnode | NULL | dnodeId | SQL |
| create qnode | createQnode | NULL | dnodeId | SQL |
| drop qnode | dropQnode | NULL | dnodeId | SQL |
| restore dnode | restoreDnode | NULL | dnodeId | SQL |

### 6.2 `auditLevel = 2` (Cluster)

| Operation | Operation | DB | Resource | Details |
|---|---|---|---|---|
| alter cluster | alterCluster | NULL | NULL | SQL |
| balance vgroup leader | balanceVgroupLead | NULL | NULL | SQL |
| redistribute vgroup | redistributeVgroup | NULL | vgroupId | SQL |
| balance vgroup | balanceVgroup | NULL | vgroupId | SQL |
| assign leader | assignLeader | NULL | NULL | SQL |
| grant privileges | grantPrivileges | NULL | user granted | SQL |
| revoke privileges | revokePrivileges | NULL | user whose privileges were revoked | SQL |
| login | login | NULL | NULL | appName |
| create user | createUser | NULL | user being created | User attributes, excluding password |
| alter user | alterUser | NULL | user being modified | Password change logs parameter names and new values, excluding password; other changes log SQL |
| drop user | dropUser | NULL | user being deleted | SQL |
| create mount | createMount | mountName | NULL | SQL |
| drop mount | dropMount | mountName | NULL | SQL |
| kill retention | killRetention | db name | NULL | SQL |
| auto trimDB | autoTrimDB | db name | NULL | SQL |
| create encrypt algr | createEncryptAlgr | NULL | algorithmId | SQL |
| drop encrypt algr | dropEncryptAlgr | NULL | algorithmId | SQL |

### 6.3 `auditLevel = 3` (Database)

| Operation | Operation | DB | Resource | Details |
|---|---|---|---|---|
| create database | createDB | db name | NULL | SQL |
| alter database | alterDB | db name | NULL | SQL |
| drop database | dropDB | db name | NULL | SQL |
| compact database | compact | database name | NULL | SQL |
| kill compact | killCompact | db name | NULL | SQL |
| create stable | createStb | db name | stable name | SQL |
| alter stable | alterStb | db name | stable name | SQL |
| drop stable | dropStb | db name | stable name | SQL |
| create stream | createStream | NULL | stream name | SQL |
| drop stream | dropStream | NULL | stream name | SQL |
| recalc stream | recalcStream | streamName | recalcName | SQL |
| create topic | createTopic | topic database | topic name | SQL |
| drop topic | dropTopic | topic database | topic name | SQL |
| reload topic | reloadTopic | topic database | topic name | SQL |
| create Rsma | createRsma | Rsma name | NULL | SQL |
| alter Rsma | alterRsma | Rsma name | Table name | SQL |
| drop Rsma | dropRsma | Rsma name | NULL | SQL |
| create View | createView | Db name | NULL | SQL |
| drop View | dropView | Db name | view name | SQL |

### 6.4 `auditLevel = 4` (Child Table)

| Operation | Operation | DB | Resource | Details |
|---|---|---|---|---|
| create table | createTable | db name | table name | SQL |
| drop table | dropTable | db name | table name | SQL |

Child-table creation is also controlled by `auditCreateTable`: when it is `0`, child-table creation is not recorded even if `auditLevel >= 4`.

### 6.5 `auditLevel = 5` (Data)

| Operation | Operation | DB | Resource | Details |
|---|---|---|---|---|
| insert | insert | db name | table name | SQL |
| select | select | db name | table name | SQL |
| delete | delete | db name | table name | SQL |

Data-level audit records are reported by the client after statements succeed. This can significantly increase write volume, so weigh compliance requirements against performance cost.

## 7. Viewing and Operations

After `taosd` and Keeper are configured and started correctly, unless local-cluster writes are enabled, audit logs can be viewed in the following ways:

- **taosExplorer**: System Management -> Audit.
- **SQL**:

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

Before deleting an audit database, as `SYSAUDIT` only:

```sql
ALTER DATABASE audit ALLOW_DROP 1;
DROP DATABASE audit;
```

## 8. Privileges and Tamper-Resistance Notes

- Writing audit tables requires `SYSAUDIT_LOG`; viewing audit tables requires `SYSAUDIT`. Ordinary business accounts should not have audit database write privileges.
- Deleting or modifying audit tables and rows is not allowed by the privilege model.
- Audit databases default to `ALLOW_DROP = 0` to prevent accidental deletion.
- Under separation of duties, assign `SYSDBA`, `SYSSEC`, and `SYSAUDIT` to different personnel. For complete rules, see [Privileges](../05-tdengine-sql/07-user-and-privilege/02-grant.md).

## 9. Security Advisories and Vulnerability Disclosure

Known security vulnerabilities, affected versions, and fixed versions are published on [Security Advisories](./09-security-advisories.md). If you discover an undisclosed vulnerability, report it privately using the channels on that page. Do not discuss unfixed issues in public forums or issues.

For hardening and deployment guidance, see [Security Hardening](./08-security-hardening.md).

## 10. Related Reading

| Topic | Documentation |
|---|---|
| taosd audit parameters | [taosd](../12-operations-and-tooling/03-components/01-taosd.md) |
| taosKeeper | [taosKeeper](../12-operations-and-tooling/03-components/05-taoskeeper.md) |
| Audit database DDL / privileges | [Databases](../05-tdengine-sql/02-ddl/01-database.md), [Privileges](../05-tdengine-sql/07-user-and-privilege/02-grant.md#audit-database) |
| Audit database encryption | [Data-at-Rest Protection](./06-data-security.md) |
| Audit summary in full-trace authentication | [Full-Trace Authentication](./01-full-trace-auth.md) |
