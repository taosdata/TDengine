---
sidebar_label: Static Data Protection
title: Data-at-Rest Protection
description: Transparent data encryption (TDE), key management, and secure delete (SECURE_DELETE)
toc_max_heading_level: 4
---

This section describes Enterprise **data-at-rest protection**: Transparent Data Encryption (TDE) and secure delete (`SECURE_DELETE`). They complement each other: TDE reduces the risk of directly interpreting disks or files, while secure delete focuses on physically overwriting residual data blocks after deletion. This document does not claim compliance with any specific external regulation or certification.

## Version and Capability Evolution

| Version | Capability |
|---|---|
| `v3.3.0.0` | Enterprise first introduced database-level data encryption (`ENCRYPT_ALGORITHM`) and related cluster key mechanisms. |
| `v3.3.7.0` | User passwords stored on disk can be additionally encrypted; this is related to the data key, as described below. |
| `v3.4.0.0` | Hierarchical keys plus full transparent encryption through `taosk` for configuration, metadata, and time-series data; encryption algorithms were expanded and custom algorithms added. |
| `v3.4.1.0` | `SECURE_DELETE` and the global parameter `secureEraseMode`. |
| `v3.4.2.0` | `encryptScope` adds `query_spill` for encrypting temporary files spilled by queries. |

Use the latest Enterprise edition when possible. For database DDL options such as `ENCRYPT_ALGORITHM`, `IS_AUDIT`, `SECURE_DELETE`, and `SECURITY_LEVEL`, see [Databases](../05-tdengine-sql/02-ddl/01-database.md). For `SECURITY_LEVEL` (MAC), see [Privileges · Mandatory Access Control (MAC)](../05-tdengine-sql/07-user-and-privilege/02-grant.md#mandatory-access-control-mac). For `IS_AUDIT` constraints, see [Audit and Compliance](./07-audit-and-compliance.md).

:::note
**Relationship between v3.3 and v3.4+**: `v3.3` mainly uses database-level `ENCRYPT_ALGORITHM` values such as `'sm4'` and cluster-side encryption keys. Starting with `v3.4.0.0`, the recommended path is to generate hierarchical keys with `taosk`, then create encrypted databases with algorithm IDs such as `ENCRYPT_ALGORITHM 'SM4-CBC'` or `'AES-128-CBC'`. The following sections focus on **v3.4+ / `taosk`**. For upgrade and compatibility notes, see [Version Compatibility](#version-compatibility).
:::

## 1. Storage Security (TDE)

TDengine supports Transparent Data Encryption (TDE), which encrypts static data files and reduces the risk that an attacker can bypass the database and read sensitive information directly from the file system. Applications are unaware of encryption and do not need business-code changes. Built-in support includes symmetric algorithms such as SM4 and AES in CBC mode.

Key management uses **machine-code binding**: keys are protected with the machine code and stored locally, rather than in a third-party KMS. If data files are copied to another machine, the changed machine code prevents the keys from being decrypted and therefore prevents the files from being interpreted. Encryption covers write-ahead logs, metadata, and time-series data files. Compression ratio is unchanged after encryption, and write/query performance usually decreases only slightly.

:::note
Storage security depends on the machine code. Some virtualization or container environments may not provide a usable machine code; verify this before deployment.
:::

### 1.1 Recommended Enablement Flow (v3.4+)

1. Stop business writes during the maintenance window; when generating keys offline, stop `taosd` when possible.
2. Use `taosk` to generate hierarchical keys. At least `DATA_KEY` is required before creating an encrypted database.
3. Start `taosd` and create an encrypted database with `ENCRYPT_ALGORITHM 'SM4-CBC'`, `'AES-128-CBC'`, or another supported algorithm.
4. Validate with `SHOW ENCRYPT_STATUS` / `ins_encrypt_status` and `ins_databases.encrypt_algorithm`.

```shell
taosk -c /etc/taos \
  --encrypt-server \
  --encrypt-database \
  --encrypt-config \
  --encrypt-metadata \
  --encrypt-data
```

```sql
CREATE DATABASE secure_db ENCRYPT_ALGORITHM 'SM4-CBC';
SELECT * FROM information_schema.ins_encrypt_status;
SELECT name, `encrypt_algorithm` FROM information_schema.ins_databases;
```

Starting with `v3.4.0.0`, audit databases require `ENCRYPT_ALGORITHM` to be non-`none`; see [Audit and Compliance · Create an Audit Database](./07-audit-and-compliance.md#create-audit-database).

### 1.2 Key Hierarchy

| Key | Purpose | Updatable |
|---|---|---|
| `SVR_KEY` (server master key) | Encrypts the database master key and system-level information, and binds them to machine hardware. | Yes |
| `DB_KEY` (database master key) | Encrypts derived keys. | Yes |
| `CFG_KEY` (configuration encryption key) | Encrypts configuration files. | No, after generation |
| `META_KEY` (metadata encryption key) | Encrypts metadata files. | No, after generation |
| `DATA_KEY` (time-series data encryption key) | Encrypts time-series data files and related logs. | No, after generation |

The dependency chain is: `SVR_KEY` -> `DB_KEY` -> (`CFG_KEY` / `META_KEY` / `DATA_KEY`).

### 1.3 Generate Keys

Use the Enterprise `taosk` tool to generate keys:

```shell
taosk -c /etc/taos \
  --set-cfg-algorithm sm4 \
  --set-meta-algorithm sm4 \
  --encrypt-server [svr_key] \
  --encrypt-database [db_key] \
  --encrypt-config \
  --encrypt-metadata \
  --encrypt-data [data_key]
```

Main parameters:

| Parameter | Description |
|---|---|
| `-c` | Configuration directory; default `/etc/taos`. |
| `-d` | Data directory (`dataDir`); can be omitted if the configuration referenced by `-c` already contains the correct `dataDir`. |
| `--set-cfg-algorithm` | Configuration-file encryption algorithm: `sm4` or `aes`; default `sm4`. |
| `--set-meta-algorithm` | Metadata encryption algorithm: `sm4` or `aes`; default `sm4`. |
| `--encrypt-server` | Enables the server master key; `SVR_KEY` can be specified, otherwise it is generated automatically. |
| `--encrypt-database` | Enables the database master key; `DB_KEY` can be specified, otherwise it is generated automatically. |
| `--encrypt-config` | Enables configuration-file encryption and generates `CFG_KEY`. |
| `--encrypt-metadata` | Enables metadata encryption and generates `META_KEY`. |
| `--encrypt-data` | Enables data-file encryption; `DATA_KEY` can be specified, otherwise it is generated automatically. |

Examples:

```shell
# Generate all keys with the default SM4 algorithm
taosk -c /etc/taos \
  --encrypt-server \
  --encrypt-database \
  --encrypt-config \
  --encrypt-metadata \
  --encrypt-data

# Specify keys and mix algorithms
taosk -c /etc/taos \
  --set-cfg-algorithm aes \
  --set-meta-algorithm sm4 \
  --encrypt-server mysvr123 \
  --encrypt-database mydb4567 \
  --encrypt-config \
  --encrypt-metadata \
  --encrypt-data oldkey123
```

Key file locations:

- `{dataDir}/dnode/config/master.bin`: `SVR_KEY`, `DB_KEY`
- `{dataDir}/dnode/config/derived.bin`: `CFG_KEY`, `META_KEY`, `DATA_KEY`

### 1.4 View and Edit Encrypted Configuration Files

```shell
# View, automatically loading keys and decrypting for display
taosk -d /var/lib/taos --view-config /path/to/encrypted_config.json

# Edit, decrypting to an editor and writing back only if changed
taosk -d /var/lib/taos --edit-file /path/to/encrypted_config.json
```

The edit flow loads `CFG_KEY` from the data directory, decrypts to a temporary file with `0600` permissions, opens `$EDITOR` or `vi`, detects changes with SHA-256, writes back with encryption if changed, and cleans up the temporary file. `CFG_KEY` must already have been generated with `--encrypt-config`. You can set the editor with `EDITOR=nano`. If you exit without saving, the file is not modified.

### 1.5 View Encryption Status

```sql
SHOW ENCRYPT_STATUS;
-- Equivalent
SELECT * FROM information_schema.ins_encrypt_status;
```

Example output:

```text
         encrypt_scope          |           algorithm            |       status       |
=======================================================================================
 config                         | AES-128-CBC                    | enabled            |
 metadata                       | AES-128-CBC                    | enabled            |
 data                           | SM4-CBC:SM4                    | enabled            |
```

| Field | Description |
|---|---|
| `encrypt_scope` | Scope: `config` / `metadata` / `data`. |
| `algorithm` | Algorithm in use. |
| `status` | `enabled` or `disabled`. |

For system table fields, see [INS_ENCRYPT_STATUS](../05-tdengine-sql/09-system-info/01-meta.md#ins_encrypt_status). For historical key status, see [SHOW ENCRYPTIONS](../05-tdengine-sql/09-system-info/03-show.md#show-encryptions).

### 1.6 Update Keys

Only `SVR_KEY` and `DB_KEY` can be updated. `CFG_KEY`, `META_KEY`, and `DATA_KEY` cannot be changed after generation.

**Offline (`taosk`)**:

```shell
systemctl stop taosd
taosk -c /etc/taos --update-svrkey new_svr_key --update-dbkey new_db_key
systemctl start taosd
```

**Online (SQL, administrator privileges required)**:

```sql
ALTER SYSTEM SET SVR_KEY 'new_svr_key';
ALTER SYSTEM SET DB_KEY 'new_db_key';
```

### 1.7 Key Backup and Recovery

Backup generates a portable copy that is **not bound to the machine code**, making migration easier. Restore binds it to the current machine code.

```shell
# Backup; requires the correct SVR_KEY for verification
taosk -c /etc/taos --backup --svr-key your_svr_key
# Generated under {dataDir}/dnode/config/master.bin.backup.{timestamp}

# Restore on a new machine
taosk -c /etc/taos \
  --restore \
  --machine-code /path/to/backup_file \
  --svr-key your_svr_key
```

### 1.8 Key Expiration Policy

```sql
ALTER SYSTEM SET KEY_EXPIRATION 90 DAYS STRATEGY 'ALARM';
```

Current strategy option: `ALARM`, which emits an alarm in logs when the key expires.

### 1.9 Configuration File Behavior Changes

After enabling configuration encryption (`CFG_KEY`):

1. **Configuration files usually take effect only on first startup**: later direct edits to `taos.cfg` often no longer take effect.
2. **Runtime configuration changes go through SQL**: for example, `ALTER DNODE 1 'debugFlag' '143';` with the required privileges.

Use `taosk --view-config` or `--edit-file` to view or modify encrypted configuration files.

### 1.10 Transparent Encryption Scope

| Scope | Required Key | Typical Objects |
|---|---|---|
| Configuration files | `CFG_KEY` | `dnode.info` / `dnode.json`, `mnode.json`, `raft_*.json`, `vnodes.json` / `vnode.json`, and similar files. |
| Metadata | `META_KEY` | mnode SDB, snode checkpoints, and similar metadata. |
| Data files | `DATA_KEY` | TSDB, WAL, STT, TDB / BSE, and other index files. |

Encrypted files may start with the plaintext marker `tdEncrypt`, which is used to identify encrypted files and avoid repeated encryption.

#### 1.10.1 Relationship with `encryptAlgorithm` / `encryptScope`

The Enterprise parameters [encryptAlgorithm](../12-operations-and-tooling/03-components/01-taosd.md) and [encryptScope](../12-operations-and-tooling/03-components/01-taosd.md), introduced in `v3.3.0.0`, still exist in `taos.cfg`. They declare algorithm and encryption-scope combinations such as `tsdb`, `vnode_wal`, `sdb`, `mnode_wal`, and `all`. Starting with `v3.4.2.0`, `encryptScope` also supports `query_spill` to encrypt temporary files spilled by queries when memory is insufficient.

**For v3.4+ deployments, use `taosk` hierarchical keys plus database-level `ENCRYPT_ALGORITHM` as the main path**. `encryptAlgorithm` / `encryptScope` remain compatibility and scope-declaration parameters; do not treat them as a separate equivalent main workflow. For custom algorithm SO paths, see [encryptExtDir](../12-operations-and-tooling/03-components/01-taosd.md).

### 1.11 Version Compatibility {#version-compatibility}

- Upgrading from a version that does not support storage security to a version that does is generally supported.
- Encrypted databases from historical versions can be migrated compatibly by specifying `DATA_KEY` and related options, depending on the actual upgrade notes.
- After storage security is enabled, **do not roll back** to historical versions that do not support storage security.
- The `v3.3`-era single-key flow such as `CREATE ENCRYPT_KEY` / `taosd -y` is retained only for compatibility understanding. New deployments should use `taosk`.

### 1.12 Encryption Algorithm Management

View built-in and custom algorithms:

```sql
SHOW ENCRYPT_ALGORITHMS;
-- More complete fields are available in information_schema.ins_encrypt_algorithms
```

Example output:

```text
id | algorithm_id | name | desc                        | type                        | source   | ossl_algr_name |
1  | SM4-CBC      | SM4  | SM4 symmetric encryption    | Symmetric Ciphers CBC mode  | build-in | SM4-CBC:SM4    |
2  | AES-128-CBC  | AES  | AES symmetric encryption    | Symmetric Ciphers CBC mode  | build-in | AES-128-CBC    |
```

| Field | Description |
|---|---|
| `id` | Numeric identifier; built-in algorithms start from 1, custom algorithms from 101. |
| `algorithm_id` | Globally unique identifier used when creating databases. |
| `name` / `desc` | Name and description. |
| `type` | For example Symmetric Ciphers CBC mode, Asymmetric Ciphers, or Digests. |
| `source` | `built-in` / `customized`; some output may display `build-in`, so use the actual query result. |
| `ossl_algr_name` | Name in OpenSSL or the custom provider. |

Add a custom algorithm:

```sql
CREATE ENCRYPT_ALGR 'vigenere' ALGR_NAME 'vigenere' DESC 'my custom algr'
  ALGR_TYPE 'Symmetric_Ciphers_CBC_mode' OSSL_ALGR_NAME 'vigenere';
```

Custom algorithms must be implemented as SO files following the OpenSSL provider interface and are loaded when `taosd` starts. One SO can contain multiple algorithms, each mapped through `OSSL_ALGR_NAME`. See [OpenSSL provider](https://docs.openssl.org/master/man7/provider/) and [OSSL_PROVIDER-default](https://docs.openssl.org/master/man7/OSSL_PROVIDER-default/). `encryptExtDir` specifies the SO path; currently only one file can be loaded.

Delete a custom algorithm after ensuring no database references it, for example by dropping databases that use it:

```sql
DROP ENCRYPT_ALGR 'vigenere';
```

Built-in algorithms cannot be deleted.

### 1.13 Create an Encrypted Database {#create-encrypted-database}

```sql
CREATE DATABASE [IF NOT EXISTS] db_name [database_options]

database_option: {
  ENCRYPT_ALGORITHM {'none' | 'SM4-CBC' | 'AES-128-CBC' | ...}
}
```

- `ENCRYPT_ALGORITHM`: default `none`. To encrypt, use an `algorithm_id` whose type is Symmetric Ciphers CBC mode from `SHOW ENCRYPT_ALGORITHMS`. For full DDL, see [Databases](../05-tdengine-sql/02-ddl/01-database.md).
- The encryption algorithm **cannot be changed after database creation**.
- Before creating an encrypted database, generate `DATA_KEY` with `taosk --encrypt-data`.

```sql
CREATE DATABASE db1 ENCRYPT_ALGORITHM 'SM4-CBC';
CREATE DATABASE db2 ENCRYPT_ALGORITHM 'AES-128-CBC';
CREATE DATABASE db3;   -- Not encrypted
```

View database-level configuration:

```sql
SELECT name, `encrypt_algorithm` FROM information_schema.ins_databases;
```

Displayed values can vary by version, such as `SM4-CBC` or `sm4`; use the actual query result.

### 1.14 Encrypt Stored User Passwords

By default, user passwords are written to metadata after being digested, for example with MD5 or SCRAM-related hashing. When the cluster has loaded **`DATA_KEY`** (`taosk --encrypt-data`, or an equivalent compatible data-encryption key), the server additionally protects the on-disk password digest with **SM4** and writes a salt. During login verification, the server decrypts and compares only if `DATA_KEY` is available and the stored password is marked as encrypted. This reduces the risk that metadata-file disclosure directly exposes password material.

Enablement notes:

1. First generate keys as described above and ensure `DATA_KEY` exists and can be loaded by `taosd`; confirm that the `data` scope is `enabled` through `SHOW ENCRYPT_STATUS` / `ins_encrypt_status`.
2. After that, newly created users or password changes are written through the encrypted-storage path. Existing users whose passwords were not stored encrypted are not backfilled automatically; change the password or recreate the user to store it encrypted.
3. `encryptPassAlgorithm`: `taos.cfg` / [taosd](../12-operations-and-tooling/03-components/01-taosd.md) may still list this parameter, introduced in `v3.3.7.0`, with values such as `sm4`. It belongs to the earlier "stored-password encryption switch plus single key" model. For new `v3.4+` deployments, whether `DATA_KEY` has been generated and loaded is the main path. Do not treat it and `taosk` hierarchical keys as two separate, equivalent workflows that both must be configured. For upgraded environments still relying on old parameters and `CREATE ENCRYPT_KEY`, follow implementation behavior and release notes.

## 2. Secure Delete {#secure-delete}

The database option `SECURE_DELETE` (`0` / `1`, default `0`) controls whether delete operations, in addition to writing delete markers, physically overwrite on-disk data blocks. For DDL syntax, see [Databases · SECURE_DELETE](../05-tdengine-sql/02-ddl/01-database.md#secure_delete). A single delete statement can also append the `SECURE_DELETE` keyword; see [Data Deletion](../05-tdengine-sql/03-data-write/02-delete.md). Tables and supertables can also set the same option; database-level, table-level, and statement-level settings are OR-combined.

- **Off (`0`)**: only delete markers are written. Queries no longer return deleted data, but the corresponding file blocks can remain on disk until later compaction or reclamation.
- **On (`1`)**: in addition to delete markers, matching data blocks in DATA / STT and other on-disk files for the `(table, time range)` are overwritten at the file level, reducing the risk of reading deleted content directly from the file system.

Behavior notes:

- The effective condition is any of database-level `SECURE_DELETE=1`, secure-delete metadata on the table/supertable, or statement-level `DELETE ... SECURE_DELETE`; implementation combines them with bitwise OR.
- Physical overwrite runs after the delete marker is written. If overwrite fails, the server logs it; query semantics still follow the delete marker, so deleted data does not become visible again because overwrite failed.
- The current implementation targets the newer TSDB file format. Old-format files skip file-level overwrite and rely on later compaction paths for reclamation.
- In multi-replica scenarios, file-level overwrite runs on the Raft Leader. Followers replay the logical delete through WAL and do not automatically repeat the same physical overwrite.
- WAL can still retain original write records until WAL trimming after a checkpoint. OS page cache and SSD wear leveling can also make old content briefly visible on physical media. This feature is not hardware-level Secure Erase / Sanitize and is not equivalent to "data-at-rest encryption plus key destruction".
- Enabling it increases delete-path I/O and latency; weigh residual-data removal requirements against performance cost.
- It complements TDE: TDE reduces the risk of directly interpreting static files, while secure delete focuses on overwriting residual blocks after deletion.
- The global parameter `secureEraseMode` (default `0`) controls the fill mode when a full block can be overwritten directly: `0` is zero-fill, `1` is random bytes. Partially overlapping blocks are always zero-filled to preserve in-place write-back. See [taosd · secureEraseMode](../12-operations-and-tooling/03-components/01-taosd.md).

Examples:

```sql
-- Database level
CREATE DATABASE db SECURE_DELETE 1;
ALTER DATABASE db SECURE_DELETE 1;

-- Supertable / table level, OR-combined with database and statement levels
CREATE STABLE meters (
  ts TIMESTAMP, current FLOAT, voltage INT
) TAGS (location VARCHAR(64)) SECURE_DELETE 1;
ALTER STABLE meters SECURE_DELETE 1;

-- Statement level
DELETE FROM meters WHERE ts < '2021-10-01 10:40:00.100' SECURE_DELETE;
```

## 3. Related Reading

| Topic | Documentation |
|---|---|
| Database / table DDL | [Databases](../05-tdengine-sql/02-ddl/01-database.md), [Data Deletion](../05-tdengine-sql/03-data-write/02-delete.md) |
| SHOW / system tables | [SHOW](../05-tdengine-sql/09-system-info/03-show.md), [Metadata Tables](../05-tdengine-sql/09-system-info/01-meta.md) |
| taosd encryption parameters | [taosd](../12-operations-and-tooling/03-components/01-taosd.md): `encryptAlgorithm`, `encryptScope`, `encryptExtDir`, `encryptPassAlgorithm`, `secureEraseMode`, and related parameters |
| Data-at-rest reliability in the full trace | [Full-Trace Reliability · TDE](./03-full-trace-reliability.md) |
| Hardening checklist | [Security Hardening](./08-security-hardening.md) |
