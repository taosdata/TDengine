---
sidebar_label: Data Ingestion
title: Ingest, Update, and Delete Data
slug: /basic-features/data-ingestion
---

This document describes how to insert, update, and delete data using SQL. The databases and tables used as examples in this document are defined in [Sample Data](../data-model/#sample-data).

TDengine can also ingest data from various data collection tools. For more information, see [Integrate with Data Collection Tools](../../third-party-tools/data-collection/).

## Ingest Data

You use the `INSERT` statement to ingest data into TDengine. You can ingest one or more records into one or more tables.

### Insert a Record

The following SQL statement inserts one record into the `d1001` subtable:

```sql
INSERT INTO d1001 (ts, current, voltage, phase) VALUES ("2018-10-03 14:38:05", 10.3, 219, 0.31);
```

In this example, a smart meter with device ID `d1001` collected data on October 3, 2018 at 14:38:05. The data collected indicated a current of 10.3 A, voltage of 219 V, and phase of 0.31. The SQL statement provided inserts data into the `ts`, `current`, `voltage`, and `phase` columns of subtable `d1001` with the values `2018-10-03 14:38:05`, `10.3`, `219`, and `0.31`, respectively.

Note that when inserting data into every column of a subtable at once, you can omit the column list. The following SQL statement therefore achieves the same result:

```sql
INSERT INTO d1001 VALUES ("2018-10-03 14:38:05", 10.3, 219, 0.31);
```

Also note that timestamps can be inserted in Unix time if desired:

```sql
INSERT INTO d1001 VALUES (1538548685000, 10.3, 219, 0.31);
```

### Insert Multiple Records

The following SQL statement inserts multiple records into the `d1001` subtable:

```sql
INSERT INTO d1001 VALUES
("2018-10-03 14:38:05", 10.2, 220, 0.23),
("2018-10-03 14:38:15", 12.6, 218, 0.33),
("2018-10-03 14:38:25", 12.3, 221, 0.31);
```

This method can be useful in scenarios where a data collection point (DCP) collects data faster than it reports data. In this example, the smart meter with device ID `d1001` collects data every 10 seconds but reports data every 30 seconds, meaning that three records need to be inserted every 30 seconds.

### Insert into Multiple Tables

The following SQL statement inserts three records each into the `d1001`, `d1002`, and `d1003` subtables:

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

### Insert into Specific Columns

The following SQL statement inserts a record containing only the `ts`, `voltage`, and `phase` columns into the `d1004`subtable:

```sql
INSERT INTO d1004 (ts, voltage, phase) VALUES ("2018-10-04 14:38:06", 223, 0.29);
```

A `NULL` value is written to any columns not included in the `INSERT` statement. Note that the timestamp column cannot be omitted and cannot be null.

### Create Subtable on Insert

It is not necessary to create subtables in advance. You can use the `INSERT` statement with the `USING` keyword to create subtables automatically:

```sql
INSERT INTO d1002 USING meters TAGS ("California.SanFrancisco", 2) VALUES (now, 10.2, 219, 0.32);
```

If the subtable `d1002` already exists, the specified metrics are inserted into the subtable. If the subtable does not exist, it is created using the `meters` subtable with the specified tag values, and the specified metrics are then inserted into it. This can be useful when creating subtables programatically for new DCPs.

### Insert via Supertable

The following statement inserts a record into the `d1001` subtable via the `meters` supertable.

```sql
INSERT INTO meters (tbname, ts, current, voltage, phase, location, group_id) VALUES ("d1001", "2018-10-03 14:38:05", 10.2, 220, 0.23, "California.SanFrancisco", 2);
```

Note that the data is not stored in the supertable itself, but in the subtable specified as the value of the `tbname` column.

## Update Data

You can update existing metric data by writing a new record with the same timestamp as the record that you want to replace:

```sql
INSERT INTO d1001 (ts, current) VALUES ("2018-10-03 14:38:05", 22);
```

This SQL statement updates the value of the `current` column at the specified time to `22`.

## Delete Data

TDengine automatically deletes expired data based on the retention period configured for your database. However, if necessary, you can manually delete data from a table.

:::warning

Deleted data cannot be recovered. Exercise caution when deleting data.

Before deleting data, run a `SELECT` statement with the same `WHERE` condition to query the data that you want to delete. Confirm that you want to delete all data returned by the `SELECT` statement, and only then run the `DELETE` statement.

:::

The following SQL statement deletes all data from supertable `meters` whose timestamp is earlier than 2021-10-01 10:40:00.100.

```sql
DELETE FROM meters WHERE ts < '2021-10-01 10:40:00.100';
```

Note that when deleting data, you can filter only on the timestamp column. Other filtering conditions are not supported.
