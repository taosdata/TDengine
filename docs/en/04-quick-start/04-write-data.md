---
sidebar_label: Data Ingestion
title: Data Ingestion
description: Use SQL to write, update, and delete time-series data
toc_max_heading_level: 4
---

This chapter continues with the smart meter model from the previous chapter. You will quickly try writing, updating, and deleting time-series data in the shell: one or many rows at a time, multi-table inserts, automatic table creation, updates, and deletes.

## Prerequisites

Confirm that you have completed the earlier chapter:

1. The TDengine service is running and you can connect with the shell.
2. You understand the basic model of the `power` database, the `meters` supertable, and subtables such as `d1001` and `d1002`.

If you have not created these objects yet, run the following SQL in the shell.

```sql
CREATE DATABASE IF NOT EXISTS power PRECISION 'ms' KEEP 3650 DURATION 10 BUFFER 16;

USE power;

CREATE STABLE IF NOT EXISTS meters (
    ts timestamp,
    current float,
    voltage int,
    phase float
) TAGS (
    location varchar(64),
    group_id int
);

CREATE TABLE IF NOT EXISTS d1001
USING meters TAGS ("California.SanFrancisco", 2);

CREATE TABLE IF NOT EXISTS d1002
USING meters TAGS ("California.SanFrancisco", 3);

CREATE TABLE IF NOT EXISTS d1003
USING meters TAGS ("California.LosAngeles", 2);

CREATE TABLE IF NOT EXISTS d1004
USING meters TAGS ("California.LosAngeles", 3);
```

## Writing

In the shell, you can write time-series data with the `INSERT` statement.

### Writing One Record at a Time

Run the following SQL to write one row into subtable `d1001`: current 10.3A, voltage 219V, phase 0.31.

```sql
INSERT INTO d1001 (ts, current, voltage, phase) VALUES ("2018-10-03 14:38:05", 10.3, 219, 0.31);
```

If `VALUES` includes all columns of the table, you can omit the column list. The effect is the same.

```sql
INSERT INTO d1001 VALUES ("2018-10-03 14:38:05", 10.3, 219, 0.31);
```

The timestamp column can also use a numeric timestamp in the database precision.

```sql
INSERT INTO d1001 VALUES (1538548685000, 10.3, 219, 0.31);
```

These three forms have the same effect.

### Writing Multiple Records at Once

Assume `d1001` collects data every 10 seconds and reports every 30 seconds. You can write 3 rows in one `INSERT` statement.

```sql
INSERT INTO d1001 VALUES
 ("2018-10-03 14:38:05", 10.2, 220, 0.23),
 ("2018-10-03 14:38:15", 12.6, 218, 0.33),
 ("2018-10-03 14:38:25", 12.3, 221, 0.31);
```

### Writing to Multiple Tables at Once

You can also write to `d1001`, `d1002`, and `d1003` in one statement. The following SQL writes 9 rows in total.

```sql
INSERT INTO d1001 VALUES
    ("2018-10-03 14:38:05", 10.2, 220, 0.23),
    ("2018-10-03 14:38:15", 12.6, 218, 0.33),
    ("2018-10-03 14:38:25", 12.3, 221, 0.31)
d1002 VALUES
    ("2018-10-03 14:38:04", 10.2, 220, 0.23),
    ("2018-10-03 14:38:14", 10.3, 218, 0.25),
    ("2018-10-03 14:38:24", 10.1, 220, 0.22)
d1003 VALUES
    ("2018-10-03 14:38:06", 11.5, 221, 0.35),
    ("2018-10-03 14:38:16", 10.4, 220, 0.36),
    ("2018-10-03 14:38:26", 10.3, 220, 0.33);
```

### Specifying Columns for Writing

When you write only some columns, columns that do not appear are filled with `NULL`. The timestamp column must be present and cannot be NULL. The following SQL writes voltage and phase into `d1004`, with current as `NULL`.

```sql
INSERT INTO d1004 (ts, voltage, phase) VALUES ("2018-10-04 14:38:06", 223, 0.29);
```

### Automatic Table Creation on Insert

With an `INSERT` that uses the `USING` keyword, if the subtable does not exist, TDengine creates it automatically before writing; if it already exists, it writes directly. You can also specify only some tag columns; unspecified tags are `NULL`.

```sql
INSERT INTO d1005
USING meters (location)
TAGS ("California.SanFrancisco")
VALUES ("2018-10-04 14:38:07", 10.15, 217, 0.33);
```

Automatic table creation also supports writing to multiple tables at once. The following SQL writes 9 rows in total.

```sql
INSERT INTO d1001 USING meters TAGS ("California.SanFrancisco", 2) VALUES
    ("2018-10-03 14:38:05", 10.2, 220, 0.23),
    ("2018-10-03 14:38:15", 12.6, 218, 0.33),
    ("2018-10-03 14:38:25", 12.3, 221, 0.31)
d1002 USING meters TAGS ("California.SanFrancisco", 3) VALUES
    ("2018-10-03 14:38:04", 10.2, 220, 0.23),
    ("2018-10-03 14:38:14", 10.3, 218, 0.25),
    ("2018-10-03 14:38:24", 10.1, 220, 0.22)
d1003 USING meters TAGS ("California.LosAngeles", 2) VALUES
    ("2018-10-03 14:38:06", 11.5, 221, 0.35),
    ("2018-10-03 14:38:16", 10.4, 220, 0.36),
    ("2018-10-03 14:38:26", 10.3, 220, 0.33);
```

### Inserting Through Supertables

You can also write directly to a supertable. The supertable itself does not store data; writes go to the corresponding subtable. The following SQL writes into `d1001` by specifying `tbname`.

```sql
INSERT INTO meters (tbname, ts, current, voltage, phase, location, group_id)
VALUES ("d1001", "2018-10-03 14:38:05", 10.2, 220, 0.23, "California.SanFrancisco", 2);
```

### Writing Through Virtual Tables

Note: Virtual tables and virtual supertables are generated dynamically and do not store data. They do not support writes.

### Zero-Code Insertion

Besides writing SQL in the shell, you can import data through third-party tools such as Telegraf, Prometheus, EMQX, StatsD, collectd, and HiveMQ. TDengine TSDB Enterprise also provides connectors for MQTT, OPC, AVEVA PI System, Wonderware, Kafka, MySQL, Oracle, and more. After configuration, you can write data without application code. For a quick walkthrough, see [No-Code Data Ingestion](./10-no-code-ingestion.md).

## Update

Writing data with the same timestamp replaces the old values with the new ones. The following SQL updates the current of `d1001` at `2018-10-03 14:38:05` to `22`.

```sql
INSERT INTO d1001 (ts, current) VALUES ("2018-10-03 14:38:05", 22);
```

## Delete

You can delete abnormal data by timestamp. The following SQL deletes data in the supertable `meters` earlier than `2021-10-01 10:40:00.100`.

```sql
DELETE FROM meters WHERE ts < '2021-10-01 10:40:00.100';
```

Deletion is irreversible. It is recommended to run a `SELECT` with the same `WHERE` condition first to confirm the rows to delete, then run `DELETE`.

## View Compression Ratio

After writing data, you can check the database compression ratio and disk usage.

```sql
SELECT * FROM INFORMATION_SCHEMA.INS_DISK_USAGE WHERE db_name = 'power';
```

You can also check the compression distribution of a single table.

```sql
SHOW TABLE DISTRIBUTED d1001;
```

For more on disk usage and distribution, see [View DB Disk Usage](../05-tdengine-sql/02-ddl/01-database.md#view-db-disk-usage) and [SHOW TABLE DISTRIBUTED](../05-tdengine-sql/09-system-info/03-show.md#show-table-distributed).

For more write syntax, delete rules, and compression settings, continue with [Data Writing](../05-tdengine-sql/03-data-write/01-insert.md).
