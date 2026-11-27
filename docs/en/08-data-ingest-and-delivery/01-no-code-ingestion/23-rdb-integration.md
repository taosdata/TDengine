---
title: Relational Database Integration
sidebar_label: Relational Database Integration
toc_max_heading_level: 4
---

This page describes how TDengine integrates with mainstream relational databases: supported databases, synchronization modes, field mapping, data type conversion, transaction consistency, and data verification. To create a task for one database, see [Dameng](./24-dameng.md), [MySQL](./13-mysql.md), [Oracle](./15-oracle.md), [PostgreSQL](./14-postgres.md), or [SQL Server](./16-mssql.md).

## Overview

In industry, energy, and power systems, device data, business data, and point inventories are often stored in relational databases. TDengine Enterprise provides one relational ingestion path through taosX and taosExplorer:

- **One configuration model**: configure Dameng, MySQL, Oracle, PostgreSQL, and SQL Server in taosExplorer with the same page structure. Each database connects with its own native protocol, and no application code is required.
- **Multiple data shapes**: a source table can be a narrow table with one point value per row, or a wide table with several measurements per row. JSON or delimited text in a column can be split into fields during mapping.
- **Three synchronization modes**: full migration, incremental synchronization, and scheduled synchronization. See [Synchronization Modes](#synchronization-modes).
- **Visual mapping**: map source fields to supertable columns, tags, and subtable names on the page, and preview the result. See [Field Mapping](#field-mapping).
- **Automatic type conversion**: convert source types to TDengine types with one set of rules. See [Data Type Conversion](#data-type-conversion).
- **Consistency and verification**: read only committed data, save a checkpoint per window, write idempotently, and optionally check row counts and digests window by window. See [Transaction Consistency](#transaction-consistency) and [Data Verification](#data-verification).

Data moves through these steps:

1. taosX, or a taosX-Agent deployed in the source network, queries the source database with the SQL template and a time window.
2. The query result is extracted, filtered, and mapped into a TDengine supertable, subtables, and columns.
3. The data is written to TDengine. After the write succeeds, the window checkpoint is saved. When data verification is enabled, the checkpoint is saved only after verification passes.

## Supported Databases

| Database | Supported versions | Connection | Full migration | Incremental sync | Scheduled sync |
| --- | --- | --- | --- | --- | --- |
| Dameng | DM8 | Dameng DPI | Supported | Supported | Supported |
| MySQL | 5.6, 5.7, 8.0 and later | MySQL protocol | Supported | Supported | Supported |
| Oracle | 11g, 12c, 19c | Oracle OCI | Supported | Supported | Supported |
| PostgreSQL | 15.0 and later | PostgreSQL protocol | Supported | Supported | — |
| SQL Server | 2012, 2022 | TDS protocol | Supported | Supported | — |

A database that speaks the MySQL protocol, such as MariaDB or Apache Doris, can use the MySQL data source. See [MySQL](./13-mysql.md) for the caveats.

## Synchronization Modes {#synchronization-modes}

The three modes use the same parameters: **Start Time**, **End Time**, **Query Interval**, **Delay Duration**, and **Schedule**. The combination selects the mode:

| Mode | Parameter combination | How it runs | When it ends | Typical use |
| --- | --- | --- | --- | --- |
| Full migration | Set start time and end time, and choose **Continuous Synchronization** | Read window by window from the start time until the end time | Stops automatically after data before the end time is read | One-time historical migration |
| Incremental synchronization | Set start time, leave end time blank, and choose **Continuous Synchronization** | After historical data is read, keep reading new data, split into windows by the query interval | Does not stop automatically | The source keeps receiving writes and near-real-time sync is required |
| Scheduled synchronization | Set start time, choose **Scheduled Window Synchronization**, and set an interval or a Cron expression | On each trigger, read from the checkpoint to the safe watermark, then wait for the next trigger | If an end time is set, the task completes when the checkpoint reaches it | The source loads data in batches, or queries should avoid peak hours |

### Full Migration

Full migration copies a time range of historical data into TDengine.

1. Set **Start Time** to the earliest source time and **End Time** to the migration cutoff.
2. **Query Interval** is the span of one query. Reduce it when data is dense, so one query does not return too much. Increase it when data is sparse, so there are fewer queries.
3. Add `ORDER BY` on the time column in the **SQL Template** so rows are written in time order.
4. If the task stops and is started again, it continues after the last successful window. It does not start over.

When the migration finishes, the task status becomes **Completed**. To keep synchronizing new data, copy the task, set the start time to the previous end time, leave the end time blank, and run it as incremental synchronization.

### Incremental Synchronization

Incremental synchronization keeps reading new source data after the historical range has been read.

- **Cursor column**: the time predicate in the SQL template should use a column that reflects write order. Prefer an ingestion time or update time, not the device sample time. Sample time can arrive out of order, and using it as the cursor can skip late rows.
- **Delay duration**: a window is queried only after `window end + delay duration`, so transactions that are not yet committed have time to commit. Set the delay longer than the longest write transaction in the source database.
- **What is captured**: incremental synchronization reads inserted or updated rows by the cursor column. An updated source row is read again and overwrites the TDengine row with the same primary key, as long as the update refreshes the cursor column. Deleted source rows are not deleted in TDengine.

### Scheduled Synchronization

Scheduled synchronization runs on a schedule. Each trigger processes windows from the checkpoint to the safe watermark, which is the current time minus the delay duration.

- **Schedule**: set the first scheduled time and a fixed interval in minutes, or set a five-field minute-level Cron expression. Scheduling uses the current time zone of the taosExplorer page.
- **Maximum windows per run**: limits how many windows one trigger reads, which limits the query load on the source. Remaining windows continue on later triggers.
- **Retry**: if a run fails, the task is shown as failed and the schedule is kept. The next trigger retries from the unconfirmed window.

See [Oracle · Scheduled Window Example](./15-oracle.md#scheduled-window-example). Dameng and MySQL use the same settings.

### Resume from a Checkpoint

All three modes save a checkpoint per window. The checkpoint is stored in taosX. After the task stops, taosX restarts, or a taosX-Agent reconnects, reading continues after the last successful window.

## Field Mapping {#field-mapping}

Configure field mapping in **Data Mapping**. It decides where each source column is written in TDengine.

### Mapping Targets

| Source data | Target | Description |
| --- | --- | --- |
| Time column | Supertable primary timestamp | Required. Every row must have a non-null time. |
| Point or device identifier | Subtable name and tag | Build the subtable name with the **Subtable Name** template, for example `meter_{METER_ID}`. |
| Point attributes, such as location and model | Tags | Point attributes are written as tags. Query them with `SELECT`, and change them with `UPDATE`. |
| Measurement columns | Ordinary data columns | The column name can differ from the source name. |
| Column that distinguishes several rows at one time | Composite primary key column | Required when one point has several rows at the same time. See [Transaction Consistency](#transaction-consistency). |

### How to Map

1. Click **Retrieve from Server** to load sample data with the SQL template.
2. Select the target supertable. If it does not exist, generate it from the sample. Column types follow [Data Type Conversion](#data-type-conversion), and you can adjust them afterward.
3. Source fields and target columns are matched by name, ignoring case. For a different name, select the source field in the list, or enter an expression such as `VOLTAGE / 1000` or `CONCAT(AREA, '-', LINE)`.
4. Leave source fields that should not be written unmapped.
5. Click **Preview** and confirm the subtable name, tags, and column values.

### Wide Tables and Narrow Tables

- **Wide table**: each row contains several measurements for one device, for example `(TS, DEVICE_ID, VOLTAGE, CURRENT, PHASE)`. Map each measurement column to one ordinary column of the supertable.
- **Narrow table**: each row contains one value of one point, for example `(TS, TAG_NAME, VALUE, QUALITY)`. Usually `TAG_NAME` builds the subtable name, `VALUE` and `QUALITY` map to ordinary columns, and point attributes map to tags.
- **Composite text**: use **Extract or Split from Column** to take several fields from one text column by delimiter, regular expression, or JSON path, and then map those fields.

### Mapping Example

The source meter-reading table is:

```sql
CREATE TABLE METER_READINGS (
  ID         BIGINT PRIMARY KEY,
  METER_ID   VARCHAR(32),
  LOCATION   VARCHAR(64),
  READING_TS TIMESTAMP,
  INGEST_TS  TIMESTAMP,
  VOLTAGE    DECIMAL(10, 3),
  CURRENT_A  DOUBLE
);
```

Map it to the TDengine supertable `meters(ts TIMESTAMP, voltage DECIMAL(10, 3), current DOUBLE) TAGS (meter_id VARCHAR(32), location NCHAR(64))`:

| Source field | Target | Description |
| --- | --- | --- |
| `READING_TS` | `ts` | Use the sample time as the primary key |
| `METER_ID` | Subtable name `meter_{METER_ID}` and tag `meter_id` | One subtable per meter |
| `LOCATION` | Tag `location` | Use `NCHAR` for non-ASCII text |
| `VOLTAGE` | `voltage` | Precision is unchanged |
| `CURRENT_A` | `current` | The names differ, so map it manually |
| `INGEST_TS` | Not written | Used only as the incremental cursor in the SQL template |
| `ID` | Not written | — |

The SQL template is:

```sql
SELECT METER_ID, LOCATION, READING_TS, INGEST_TS, VOLTAGE, CURRENT_A
FROM METER_READINGS
WHERE INGEST_TS >= ${start} AND INGEST_TS < ${end}
ORDER BY INGEST_TS, ID
```

## Data Type Conversion {#data-type-conversion}

After taosX reads the source data, it converts source types to TDengine types as shown below. A generated supertable uses those TDengine types. When writing to an existing supertable, source values are converted to the target column type. A value that cannot be converted follows the [exception handling strategy](./13-mysql.md#configure-exception-handling).

### Type Mapping

| Category | Dameng | MySQL | Oracle | TDengine | Notes |
| --- | --- | --- | --- | --- | --- |
| Boolean | `BIT`, `BOOLEAN` | `BOOLEAN`, `BIT(1)` | `BOOLEAN` (23ai) | `BOOL` | |
| 1-byte integer | `TINYINT`, `BYTE` | `TINYINT` | — | `TINYINT` | MySQL unsigned types become `TINYINT UNSIGNED` |
| 2-byte integer | `SMALLINT` | `SMALLINT` | — | `SMALLINT` | MySQL unsigned types become `SMALLINT UNSIGNED` |
| 4-byte integer | `INT`, `INTEGER` | `INT`, `MEDIUMINT` | — | `INT` | MySQL unsigned types become `INT UNSIGNED` |
| 8-byte integer | `BIGINT` | `BIGINT` | — | `BIGINT` | MySQL unsigned types become `BIGINT UNSIGNED` |
| Exact numeric | `NUMBER(p,s)`, `DECIMAL(p,s)`, `NUMERIC(p,s)` | `DECIMAL(p,s)` | `NUMBER(p,s)` | `DECIMAL(p,s)` | Precision is kept when `p` is at most 38. When `p` is greater than 38, the value becomes `DOUBLE`. |
| Unspecified numeric | `NUMBER` | — | `NUMBER` | `DOUBLE` | `NUMBER` without a precision |
| Single-precision float | `REAL`, `FLOAT(p ≤ 24)` | `FLOAT` | `BINARY_FLOAT` | `FLOAT` | |
| Double-precision float | `DOUBLE`, `DOUBLE PRECISION`, `FLOAT` | `DOUBLE` | `BINARY_DOUBLE`, `FLOAT` | `DOUBLE` | |
| String | `CHAR`, `VARCHAR`, `VARCHAR2` | `CHAR`, `VARCHAR` | `CHAR`, `VARCHAR2`, `NCHAR`, `NVARCHAR2` | `NCHAR(n)` | The default is `NCHAR` so non-ASCII text is kept. Use `VARCHAR` when the value is ASCII only. |
| Large text | `TEXT`, `CLOB`, `LONGVARCHAR` | `TEXT`, `MEDIUMTEXT`, `LONGTEXT` | `CLOB`, `NCLOB`, `LONG` | `NCHAR` / `VARCHAR` | An ordinary column holds at most 65,517 bytes. Longer values follow the **column length overflow** policy. |
| Binary | `BINARY`, `VARBINARY` | `BINARY`, `VARBINARY` | `RAW` | `VARBINARY` | |
| Large binary | `BLOB`, `IMAGE`, `LONGVARBINARY` | `BLOB`, `MEDIUMBLOB`, `LONGBLOB` | `BLOB`, `LONG RAW` | `BLOB` | One value is at most 16 MB |
| Date and time | `DATE`, `TIMESTAMP`, `DATETIME` | `DATE`, `DATETIME`, `TIMESTAMP` | `DATE`, `TIMESTAMP` | `TIMESTAMP` | Time zone and precision rules are below |
| Time with time zone | `TIMESTAMP WITH TIME ZONE`, `DATETIME WITH TIME ZONE` | — | `TIMESTAMP WITH TIME ZONE`, `TIMESTAMP WITH LOCAL TIME ZONE` | `TIMESTAMP` | Converted to an absolute time by the time zone in the value |
| Time of day | `TIME` | `TIME` | — | `VARCHAR(18)` | Written as `HH:MI:SS[.ffffff]` |
| JSON | `JSON` | `JSON` | `JSON` (21c) | `NCHAR` | Written as JSON text. Use **Extract or Split from Column** to parse it into columns first. |

For a type that is not listed, such as a spatial type, `INTERVAL`, `ROWID`, an array, or a user-defined type, convert it to a string or a number with `CAST` or a database function in the SQL template.

### Time and Time Zone {#time-and-time-zone}

- **Task time zone**: for MySQL, PostgreSQL, SQL Server, and Oracle tasks, the time zone is set by the task parameter `time_zone`. If it is not set, the UTC offset of Start Time is used, which is the taosExplorer page time zone in effect when Start Time was set. `time_zone` accepts only a fixed UTC offset such as `+08:00` or `+0800`, or `UTC` or `Z`. IANA names such as `Asia/Shanghai` are not supported. The task time zone determines the values of the time placeholders in the SQL template and how query windows are cut (a window never crosses midnight in the task time zone). If the source stores UTC, use a UTC task time zone: in taosExplorer, switch the page time zone to UTC before setting Start Time, or set `time_zone=UTC` through the API or CLI.
- The task offset sets the session time zone for MySQL and Oracle. MySQL 8.0.19 and later accept `-13:59` through `+14:00`, and earlier versions accept `-12:59` through `+13:00`. Oracle's documented range is `-12:00` through `+14:00`. For PostgreSQL and SQL Server, taosX applies the database-specific range when the SQL template includes a complete `${start}` and `${end}` pair or `${start_time}` and `${end_time}` pair, including templates that also use placeholders without an offset. PostgreSQL accepts `-15:59` through `+15:59`, and SQL Server accepts `-14:00` through `+14:00`. Without either pair, PostgreSQL and SQL Server tasks accept fixed offsets from `-23:59` through `+23:59` for local time conversion. All ranges include their endpoints.
- Types converted with the session time zone (MySQL `TIMESTAMP` and Oracle `TIMESTAMP WITH LOCAL TIME ZONE`) are converted using the task time zone, which is the session time zone.
- A time type without a time zone (MySQL `DATETIME`, Oracle `DATE` and `TIMESTAMP`, PostgreSQL `timestamp`) is read as its wall-clock value without conversion by the task time zone. SQL Server `datetime`, `datetime2`, and `smalldatetime` are read as UTC.
- A time type that includes a time zone (such as Oracle `TIMESTAMP WITH TIME ZONE`, SQL Server `datetimeoffset`, and PostgreSQL `timestamptz`) is converted by the time zone in the value. The task time zone does not apply.
- **Time precision**: the target database precision must be at least the precision of the source data. If the source is precise to microseconds, use microsecond or nanosecond precision. A coarser target precision truncates the extra part. Distinct times can become the same timestamp and overwrite each other, so the row count drops. [Data Verification](#data-verification) can find this.

### Numbers and Strings

- When a number is written to a narrower column, for example `BIGINT` into `INT`, an out-of-range value follows the **data exception** policy. It is not truncated to a wrong value.
- An exact numeric written to a `DECIMAL` column keeps its precision. Writing it to `FLOAT` or `DOUBLE` can lose precision. Use `DECIMAL` for data that is sensitive to precision, such as metering and settlement values.
- Strings are read in the source character set and written as UTF-8. Set the connection character set to match the source. For example, when Dameng uses GB18030, set the connection character set to GB18030.
- `NULL` is written as `NULL`.

### Exception Handling

Failed type conversion, a value that is too long, and a null timestamp follow the task [exception handling strategy](./13-mysql.md#configure-exception-handling). The choices include archive, discard, and error. To keep the synchronized data complete, set **Data Exception** and **Column Length Overflow** to **Error**, so data is not discarded or truncated silently.

## Transaction Consistency {#transaction-consistency}

A relational database uses transactions to keep data consistent. TDengine organizes time-series data by a timestamp primary key. The integration keeps the synchronized result consistent with committed source data at read time, write time, and checkpoint time.

### Read Only Committed Data

- Each window is read by one read-only query. Dameng and Oracle provide statement-level consistent reads. MySQL InnoDB uses a consistent snapshot. The result is the data committed at one point in time, and it does not include uncommitted changes.
- taosX runs only `SELECT`. It does not lock source tables, and it does not block business writes.
- A transaction can commit later than the time stored in its rows. To avoid missing rows that are not yet committed when the window is queried, set **Delay Duration** longer than the longest write transaction, and use an ingestion-time column as the cursor.

### Idempotent Writes {#idempotent-writes}

- In one TDengine subtable, only one row is kept for each primary key. Writing the same row again does not change the result.
- If a window write fails, the whole window is read and written again. Rows already written are overwritten by the same data, so duplicates are not created.
- When one point has several rows at the same time, for example several inspection records at one timestamp, define a composite primary key on the supertable and map the source field that distinguishes those rows to the composite key column. Otherwise the rows overwrite each other by timestamp and only the last row remains.

### Atomic Checkpoint Advance

- The checkpoint for a window is saved only after every row in the window is written. When data verification is enabled, it is saved only after verification passes.
- If the task stops while a window is in progress, the checkpoint stays on the previous window. After recovery, the current window is processed again.
- Each source row is therefore kept exactly once in TDengine: it is not lost, and it is not duplicated.

### Consistency Scope

- Subtables in one window can reside on different vnodes. While the write is in progress, a query can see some subtables written and others not yet written. After the window write finishes, the data is consistent.
- A row deleted in the source database is not deleted in TDengine. To synchronize deletions, use a logical-delete flag column in the source, map it to TDengine, and filter on it in queries.

## Data Verification {#data-verification}

Dameng, MySQL, and Oracle data sources can verify data window by window, comparing the source with the data written to TDengine, so that the synchronized data matches the source.

### Verification Modes

Set **Data Verification** in **Advanced Options**:

| Mode | What is checked | Cost |
| --- | --- | --- |
| Off | No check. This is the default. | None |
| Row count | Compare the source row count of each window with the number of rows written to TDengine | One extra count query on the source and one on TDengine per window |
| Row count and digest | In addition to the row count, sort each window by primary key, calculate an SM3 digest, and compare the source with the data in TDengine value by value | The written data is read back, so the query volume is about doubled |

### Verification Rules

After a window is written, taosX checks the following:

1. **Row balance**: `rows read from the source = rows written successfully + rows filtered + rows discarded or archived by the exception policy`.
2. **Rows stored**: query TDengine by the window time range and the mapped subtables. The row count must equal the number of rows written successfully.
3. **Value check** (row count and digest only): calculate a digest of the successfully written values after mapping, and compare it with the digest of the same rows read back from TDengine. Floating-point values are compared at the precision of the target column.

The checkpoint is saved only when every check passes. If any check fails, the **Verification Failure Policy** applies:

- **Redo window** (default): read and write the window again, up to **Verification Retries** times (default 3). If it still does not match, the task reports an error.
- **Stop on error**: stop the task immediately, keep the checkpoint, and wait for an operator.

### View Verification Results

On the task details page, the **Data Verification** tab lists each window: time range, rows read from the source, rows written, rows filtered, exception rows, verification result, and retry count. Filter by result and export CSV as a reconciliation report.

After synchronization finishes, you can also run summary queries in the source database and in TDengine:

```sql
-- Source (Dameng in this example)
SELECT
  COUNT(*),
  SUM(VOLTAGE)
FROM METER_READINGS
WHERE READING_TS >= '2024-01-01 00:00:00'
  AND READING_TS < '2024-02-01 00:00:00';

-- TDengine
SELECT
  COUNT(*),
  SUM(voltage)
FROM meters
WHERE ts >= '2024-01-01 00:00:00'
  AND ts < '2024-02-01 00:00:00';
```

### Conditions for a Complete Match

When the following conditions hold and **Row Count and Digest** is enabled, every window passing verification means the data in TDengine matches the committed source data:

- The time column and the composite primary key columns identify every source row. See [Idempotent Writes](#idempotent-writes).
- The target database time precision is at least the source time precision.
- Exception policies such as **Data Exception** and **Column Length Overflow** are set to **Error**, or the discarded and archived rows are expected.
- The delay duration covers the longest write transaction in the source database.

## Configuration Steps

Create a relational synchronization task in taosExplorer as follows. The fields of each database are on its own page:

1. Open **Data Ingestion**, click **+ Add Data Source**, select the database type, and enter the task name and target database.
2. Enter the connection and authentication information, and click **Check Connectivity**.
3. Write the SQL template, and set the start time, end time, query interval, delay duration, and schedule to select the synchronization mode.
4. In **Data Mapping**, configure the subtable name, tags, and columns, and click **Preview**.
5. In **Advanced Options**, set read and write concurrency, batch size, and data verification.
6. Set the exception handling strategy.
7. Click **Submit**, and view the run status, written row count, and verification result in the task list.
