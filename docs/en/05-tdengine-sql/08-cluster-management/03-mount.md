---
sidebar_label: Data Mount
title: Data Mount
description: Mount a source cluster data directory read-only into the host cluster for querying
toc_max_heading_level: 4
---

Starting from `v3.3.7.0`, TDengine Enterprise provides data mount. Using SQL on a host cluster, you can mount a source cluster (for example an aircraft, vehicle/edge device, another cluster in the same room or data center, or historical backup data) so the host recognizes and accesses the source cluster’s databases, tables, and data. After mounting, you can query without migrating data. To prevent tampering with source data, only read-only mounts are supported.

## Syntax

### Create Mount

```sql
CREATE MOUNT [IF NOT EXISTS] mountName ON DNODE dnodeId FROM TDenginePath;
```

**Notes**

- `mountName`: mount name; must not contain underscore `_`. Mounted database names become `mountName_<dbname>`.
- `dnodeId`: ID of the host-cluster dnode where the source cluster data directory is visible.
- `TDenginePath`: absolute path of the source cluster data directory, enclosed in single or double quotes.

**Limits**

- Within one host cluster, `mountName` must be unique.
- `mountName` must not collide with an existing database name on the host cluster.
- `mountName` and `TDenginePath` are one-to-one.
- Currently only mounting on a single dnode is supported. If the source cluster uses multi-tier storage, manually adjust non-primary mount paths in `local.json` (no change needed without multi-tier storage).
- The same source cluster can be mounted by only one host cluster at a time.
- A host cluster cannot mount its own data directory.
- Source database IDs (`dbid`) must not collide with existing `dbid` values on the host.
- While mounted databases exist on the host, creating new databases is not allowed (to avoid name conflicts).
- At mount time: multi-replica source clusters, encrypted databases, and sources with no databases are not supported yet.
- On the host, mounted databases support only time-series queries: no writes or metadata updates; virtual-table time-series queries are not supported (virtual-table metadata queries are); streaming, subscription, and views are not supported; management operations such as `COMPACT` / `TRIM` / `REDISTRIBUTE` / `SPLIT` / `RESTORE` are not supported.
- Only the superuser can perform mount operations.

**Example**

On host dnode 1, mount source directory `/var/lib/taos_1` as `mount1`. Afterward, access source databases as `mount1_<dbname>`:

```sql
CREATE MOUNT mount1 ON DNODE 1 FROM '/var/lib/taos_1';
```

### Show Mounts

```sql
SHOW MOUNTS;
```

**Example**

```sql
SHOW MOUNTS;
```

Sample output:

```text
   name  | dnode |       create_time       |      path       |
==============================================================
  mount1 |   1   | 2025-07-17 18:06:16.298 | /var/lib/taos_1 |
```

**Notes**

- `name`: mount name
- `dnode`: host-cluster dnode that performed the mount
- `create_time`: when the mount was created on the host
- `path`: absolute path of the source data directory
- Only users with system-info view permission can list mounts

### Drop Mount

```sql
DROP MOUNT [IF EXISTS] mountName;
```

**Notes**

- Clears host-cluster metadata related to the mount
- Restores configuration in the source data directory and removes the host’s access association to the source
- Does not delete actual data files on the source cluster
- Only the superuser can perform this operation

## Technical Characteristics

### Fast Data Access

- No data migration: mount brings source data into the host for direct querying
- Keeps host-cluster query workflows convenient

### Lightweight Implementation

- Access is established via metadata mapping; source data files are not copied to the host—only mount association info is recorded

### Real-Time Query

- The host reads the source’s original data files directly, avoiding migration latency

### Security

- Mounted data is read-only on the host, protecting source data from tampering
- Mount creates only a logical access relationship; read-only limits and uniqueness checks protect data and stability
- After unmount, source configuration is restored automatically; mount does not affect source data integrity

## Typical Scenarios

Useful when you must frequently access multiple source clusters without migrating data:

- **Ground analysis of device data**: Mount collected data into an analytics cluster and query without export/migration
- **Cross-cluster comparison**: Mount multiple business clusters into one analytics cluster and compare there
- **Historical data query**: Mount offline historical clusters into an online business cluster without restoring data first
- **Backup verification**: Mount a backup storage cluster into a verification cluster and check backup integrity directly
