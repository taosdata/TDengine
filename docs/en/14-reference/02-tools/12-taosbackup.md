---
title: taosBackup Reference
sidebar_label: taosBackup
toc_max_heading_level: 4
---

taosBackup is a high-performance backup and restore tool for TDengine TSDB. It stores backup data in an efficient columnar format and supports full backup, database-level backup, table-level backup, time-range backup, metadata-only backup, and resumable execution for a wide range of data protection and migration scenarios.

## Relationship with taosdump

taosBackup is the upgraded successor to taosdump. Because their storage formats are completely incompatible, the tool was renamed to avoid confusion. After the transition period, taosdump will no longer be included in installation packages.

Compared with taosdump, taosBackup provides major improvements in both performance and backup size.

## Performance Comparison

| Tool | Backup | Restore |
| --- | --- | --- |
| taosdump (baseline) | 1x | 1x |
| taosBackup | 5x | 3x |

## Backup Size

| Tool | Relative Size |
| --- | --- |
| taosdump (baseline) | 100% |
| taosBackup | 42% |

## Parameters and Features

taosBackup supports nearly all taosdump command-line parameters, except for a few special cases. It also adds features such as resumable execution, Parquet export, restoring only specified databases, and optimized backup and restore performance for low-frequency multi-table scenarios.

## Getting the Tool

taosBackup is included in both the server and client installation packages of TDengine TSDB v3.4.1.1 and later. For installation details, see [TDengine TSDB Installation](../../../get-started/).

## Running taosBackup

taosBackup runs on Windows, macOS, and Linux. It must be run from a command-line terminal, and you must specify either backup mode with `-o` or restore mode with `-i`.

:::tip
Before running taosBackup, make sure that the target TDengine TSDB cluster is up and running correctly.
:::

### Backup Example

```bash
taosBackup -h my-server -D test -o /root/backup/
```

This command backs up the `test` database from the TDengine service running on host `my-server` to the `/root/backup/` directory.

```bash
taosBackup -h my-server -o /root/backup/
```

If `-D` is not specified, taosBackup backs up all user databases by default, excluding the system databases `information_schema` and `performance_schema`.

### Restore Example

```bash
taosBackup -h my-server -i /root/backup/
```

This command restores the backup data under `/root/backup/` to the TDengine service running on host `my-server`.

```bash
taosBackup -h my-server -D test -i /root/backup/
```

This command restores the backup data under `/root/backup/` to the TDengine service running on host `my-server`, but restores only the `test` database.

## Command-Line Parameters

```bash
Usage: taosBackup [OPTION...] dbname [tbname ...] -o outpath
  or:  taosBackup [OPTION...] -o outpath
  or:  taosBackup [OPTION...] -i inpath
  or:  taosBackup [OPTION...] --databases db1,db2,...
```

| Command-Line Parameter | Description |
| --- | --- |
| `-h, --host=HOST` | FQDN or IP address of the TDengine server to connect to. Default: `localhost` |
| `-P, --port=PORT` | TDengine server port. Default: `6030` |
| `-c, --config-dir=CONFIG_DIR` | Directory containing the `taos.cfg` configuration file. If not specified, the default path is used |
| `-u, --user=USER` | Username for the connection. Default: `root` |
| `-p, --password=PASSWORD` | Password for the connection. Default: `taosdata` |
| `-o, --outpath=OUTPATH` | Output directory for backup files. Default: `./output` |
| `-i, --inpath=INPATH` | Input path containing backup files for restore operations |
| `-D, --databases=DATABASES` | Databases to back up or restore. Separate multiple databases with commas. If omitted, all user databases are processed |
| `-F, --format=FORMAT` | Backup storage format. Supported values: `binary` (default) or `parquet` |
| `-s, --schemaonly` | Flag. Back up only table schemas and tag data, without time-series data |
| `-S, --start-time=START_TIME` | Start time for backup data. Supports millisecond timestamps or ISO8601 format such as `2017-10-01T00:00:00.000+0800`. Effective only for backup |
| `-E, --end-time=END_TIME` | End time for backup data. Supports millisecond timestamps or ISO8601 format. Effective only for backup |
| `-T, --thread-num=THREAD_NUM` | Number of parallel threads for data backup or restore. Default: `8` |
| `-m, --tag-thread-num=THREAD_NUM` | Number of parallel threads for tag-data backup. Default: `2` |
| `-B, --data-batch=DATA_BATCH` | Number of rows written in each STMT batch during restore. For STMT2 (default), the valid range is `[1, 16384]` and the default is `10000`. For STMT1, the valid range is `[1, 100000]` and the default is `60000`. Effective only for restore |
| `-v, --stmt-version=VER` | STMT API version used during restore: `2` (default, TAOS_STMT2, faster, requires TDengine v3.3+) or `1` (compatible with the legacy TAOS_STMT API). Effective only for restore |
| `-W, --rename=RENAME-LIST` | Rename databases during restore. Format: `"db1=newdb1\|db2=newdb2"`. Effective only for restore |
| `-C, --checkpoint` | Flag. Enable resumable execution. taosBackup always writes checkpoint files. When this option is enabled, rerunning the command skips completed items |
| `-k, --retry-count=VALUE` | Number of retries after a connection or query failure. Default: `3` |
| `-z, --retry-sleep-ms=VALUE` | Wait time between retries in milliseconds. Default: `1000` |
| `-X, --dsn=DSN` | DSN for connecting to a cloud service, for example `https://host?token=<TOKEN>`. You can also set it through the `TDENGINE_CLOUD_DSN` environment variable. The command-line parameter takes precedence |
| `-Z, --driver=DRIVER` | Connection driver. Supported values: `Native` or `WebSocket`. Default: `Native`. When a DSN is set, the default automatically switches to `WebSocket` |
| `-g, --debug` | Flag. Enable debug output. Disabled by default |
| `-V, --version` | Show version information and exit |
| `--help` | Show help information and exit |

## Common Usage Scenarios

### Back Up Data

#### Back Up All Databases

```bash
taosBackup -h my-server -o /root/backup/
```

Back up all user databases, automatically excluding `information_schema` and `performance_schema`, to the `/root/backup/` directory.

#### Back Up Specific Databases

```bash
taosBackup -h my-server -D db1,db2 -o /root/backup/
```

Back up only the `db1` and `db2` databases.

#### Back Up Specific Super Tables or Normal Tables in a Database

```bash
taosBackup -h my-server -o /root/backup/ mydb meters d1 d2
```

Back up the super table `meters` and the normal tables `d1` and `d2` in the `mydb` database. The first positional argument is the database name. The remaining positional arguments are table names or super table names in that database, separated by spaces.

#### Back Up by Time Range

```bash
taosBackup -h my-server -D test -S "2024-01-01T00:00:00.000+0800" -E "2024-12-31T23:59:59.999+0800" -o /root/backup/
```

Back up only data from the `test` database for the full year of 2024.

#### Back Up Metadata Only

```bash
taosBackup -h my-server -D test -s -o /root/backup/
```

Back up only the schema and tag information of the `test` database, without time-series data. This is suitable for fast schema migration.

#### Back Up in Parquet Format

```bash
taosBackup -h my-server -D test -F parquet -o /root/backup/
```

Export the `test` database in Parquet format for integration with big-data ecosystems such as Spark, Hive, and DuckDB.

#### Resumable Backup

Resumable execution is disabled by default and must be explicitly enabled with `-C`. If a backup is interrupted, rerun the same command with `-C` and taosBackup will automatically skip databases, super tables, and child tables that were already completed, then continue with the unfinished portion.

```bash
# First backup attempt (interrupted for some reason)
taosBackup -h my-server -D test -o /root/backup/

# Run again with resumable execution enabled
taosBackup -h my-server -D test -o /root/backup/ -C
```

taosBackup always writes checkpoint files to the output directory. When rerun with `-C`, it reads the checkpoint files, skips completed items, and resumes from the interruption point.

:::tip

- If backup files already exist under the directory specified by `-o`, taosBackup overwrites files with the same names when resumable execution is not enabled. Use an empty directory for a fresh full backup.
- If the backup volume is large, consider splitting the backup with `-S` and `-E`, or enabling resumable execution with `-C`.

:::

### Restore Data

#### Restore to the Original Databases

```bash
taosBackup -h my-server -i /root/backup/
```

Restore the backup data under `/root/backup/` to `my-server`. During restore, taosBackup automatically creates the corresponding databases, super tables, and child tables. If a table already exists, table creation is skipped.

#### Rename Databases During Restore

```bash
taosBackup -h my-server -i /root/backup/ -W "db1=db1_restored|db2=db2_restored"
```

Restore `db1` in the backup as `db1_restored` and `db2` as `db2_restored`. This is useful for testing, validation, or parallel environments.

#### Resumable Restore

```bash
taosBackup -h my-server -i /root/backup/ -C
```

Restore also supports resumable execution. When rerun, it automatically skips data files that have already been restored successfully.

#### Restore When the Schema Has Changed

During restore, taosBackup automatically detects schema differences between the backup and the current target server. If the target super table has a different column set than the backup, such as added or removed columns, taosBackup calculates the common columns and performs partial-column writes automatically to ensure safe data restoration without manual intervention.

#### Adjust Batch Size to Avoid WAL Overflow

```bash
taosBackup -h my-server -i /root/backup/ -B 2000
```

If you encounter a `WAL size exceeds limit` error during restore, reduce the number of rows written in each batch with `-B`.

#### Connect to TDengine Cloud

```bash
taosBackup -i /root/backup/ -X "https://cloud-host?token=<TOKEN>"
```

Restore data through a DSN connection to TDengine Cloud. The driver automatically switches to `WebSocket`.

## Backup File Structure

Under the backup output directory, each database has its own subdirectory containing the following content:

```text
{outpath}/
  {dbname}/
    db.sql
    tags/
      {stbname}.sql
      {stbname}_data{N}.{ext}
    data/
      {stbname}/
        {stbname}_data{N}.{ext}
    _ntb/
      {tbname}.sql
      {tbname}/
        {tbname}_data{N}.{ext}
```

Where:

- `db.sql` is the SQL used to create the database.
- `{stbname}.sql` is the SQL used to create the super table.
- `{stbname}_data{N}.{ext}` under `tags/` stores tag data.
- `{stbname}_data{N}.{ext}` under `data/` stores time-series data for the super table in `binary` or `parquet` format.
- `{tbname}.sql` is the SQL used to create the normal table.
- `{tbname}_data{N}.{ext}` stores time-series data for the normal table.

The file extension is `.bin` for `binary` format and `.par` for `parquet` format.

## Output Metrics

### Startup Summary

At the start of backup or restore, taosBackup prints a summary of the current runtime parameters, for example:

```bash
===========================================================================
  taosBackup - BACKUP
===========================================================================
  Connect Mode : Native
  Server       : my-server:6030
  User         : root
  Output Path  : /root/backup/
  Databases    : test
  Data Threads : 8
  Tag Threads  : 2
  Format       : binary
  Schema Only  : no
  Time Range   : ALL
  Check Point  : no
===========================================================================
```

### Real-Time Progress

During execution, taosBackup continuously prints progress information, including the current database, super table, completed child tables, and estimated remaining time:

```bash
[DB 1/2: test] [STB 3/10: meters] [CTB 1500/5000 (30.0%)] elapsed: 12s, eta: 28s
```

### Final Summary

After backup or restore completes, taosBackup prints the final statistics summary:

```bash
===========================================================================
  Result       : SUCCESS
---------------------------------------------------------------------------
  Databases    : total=1, success=1, failed=0
  Super Tables : 10
  Child Tables : 5000 (data exported)
  Normal Tables: 2
  Total Rows   : 50000000
  Elapsed      : 45.23 s
===========================================================================
```

Field descriptions:

- **Databases**: Total number of processed databases, and the numbers of successful and failed databases.
- **Super Tables**: Number of processed super tables.
- **Child Tables**: Number of child tables whose data was exported or restored.
- **Normal Tables**: Number of processed normal tables.
- **Total Rows**: Total number of backed-up or restored rows.
- **Elapsed**: Total elapsed time in seconds.

:::tip
If the number of failed items is not zero, add `-g` to enable debug output for detailed errors, or check the TDengine server logs for troubleshooting.
:::
