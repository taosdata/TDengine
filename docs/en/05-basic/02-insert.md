---
sidebar_label: Data Ingestion
title: Data Ingestion
slug: /basic-features/data-ingestion
---

This chapter uses the data model of smart meters as an example to introduce how to write, update, and delete time-series data in TDengine using SQL.

## Writing

In TDengine, you can write time-series data using the SQL insert statement.

### Writing One Record at a Time

Assume that the smart meter with device ID d1001 collected data on October 3, 2018, at 14:38:05: current 10.3A, voltage 219V, phase 0.31. We have already created a subtable d1001 belonging to the supertable meters in the TDengine's power database. Next, you can write time-series data into the subtable d1001 using the following insert statement.

1. You can write time-series data into the subtable d1001 using the following INSERT statement.

```sql
insert into d1001 (ts, current, voltage, phase) values ( "2018-10-03 14:38:05", 10.3, 219, 0.31)
```

The above SQL writes `2018-10-03 14:38:05`, `10.3`, `219`, `0.31` into the columns `ts`, `current`, `voltage`, `phase` of the subtable `d1001`.

1. When the `VALUES` part of the `INSERT` statement includes all columns of the table, the list of fields before `VALUES` can be omitted, as shown in the following SQL statement, which has the same effect as the previous INSERT statement specifying columns.

```sql
insert into d1001 values("2018-10-03 14:38:05", 10.3, 219, 0.31)
```

1. For the table's timestamp column (the first column), you can also directly use the timestamp of the database precision.

```sql
INSERT INTO d1001 VALUES (1538548685000, 10.3, 219, 0.31);
```

The effects of the above three SQL statements are exactly the same.

### Writing Multiple Records at Once

Assume that the smart meter with device ID d1001 collects data every 10s and reports data every 30s, i.e., it needs to write 3 records every 30s. Users can write multiple records in one insert statement. The following SQL writes a total of 3 records.

```sql
insert into d1001 values
 ( "2018-10-03 14:38:05", 10.2, 220, 0.23),
 ( "2018-10-03 14:38:15", 12.6, 218, 0.33),
 ( "2018-10-03 14:38:25", 12.3, 221, 0.31)
```

The above SQL writes a total of three records.

### Writing to Multiple Tables at Once

Assume that the smart meters with device IDs d1001, d1002, and d1003, all need to write 3 records every 30 seconds. For such cases, TDengine supports writing multiple records to multiple tables at once.

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
    ("2018-10-03 14:38:26", 10.3, 220, 0.33)
;
```

The above SQL writes a total of nine records.

### Specifying Columns for Writing

You can write data to specific columns of a table by specifying columns. Columns not appearing in the SQL will be automatically filled with NULL values. Note that the timestamp column must be present, and its value cannot be NULL. The following SQL writes one record to the subtable d1004. This record only includes voltage and phase, with the current value being NULL.

```sql
insert into d1004 (ts, voltage, phase) values("2018-10-04 14:38:06", 223, 0.29)
```

### Automatic Table Creation on Insert

Users can perform inserts using the `using` keyword for automatic table creation. If the subtable does not exist, it triggers automatic table creation before data insertion; if the subtable already exists, it directly inserts the data. An insert statement with automatic table creation can also specify only some tag columns for insertion, leaving the unspecified tag columns as NULL values. The following SQL inserts a record. If the subtable d1005 does not exist, it first creates the table automatically with the tag `group_id` value as NULL, then inserts the data.

```sql
insert into d1005
using meters (location)
tags ( "beijing.chaoyang")
values ( "2018-10-04 14:38:07", 10.15, 217, 0.33)
```

The insert statement with automatic table creation also supports inserting data into multiple tables in one statement. The following SQL uses an automatic table creation insert statement to insert 9 records.

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
    ("2018-10-03 14:38:26", 10.3, 220, 0.33)
;
```

### Inserting Through Supertables

TDengine also supports direct data insertion into supertables. It is important to note that a supertable is a template and does not store data itself; the data is stored in the corresponding subtables. The following SQL inserts a record into the subtable d1001 by specifying the tbname column.

```sql
insert into meters (tbname, ts, current, voltage, phase, location, group_id)
values( "d1001, "2018-10-03 14:38:05", 10.2, 220, 0.23, "California.SanFrancisco", 2)
```

### Zero-Code Insertion

To facilitate easy data insertion for users, TDengine has seamlessly integrated with many well-known third-party tools, including Telegraf, Prometheus, EMQX, StatsD, collectd, and HiveMQ. Users only need to perform simple configurations on these tools to easily import data into TDengine. Additionally, TDengine Enterprise offers a variety of connectors, such as MQTT, OPC, AVEVA PI System, Wonderware, Kafka, MySQL, Oracle, etc. By configuring the corresponding connection information on the TDengine side, users can efficiently write data from different data sources into TDengine without writing any code.

## Update

Data in time-series can be updated by inserting a record with a duplicate timestamp; the newly inserted data will replace the old values. The following SQL, by specifying columns, inserts 1 row of data into the subtable `d1001`; when there is already data with the datetime `2018-10-03 14:38:05` in subtable `d1001`, the new `current` (current) value 22 will replace the old value.

```sql
INSERT INTO d1001 (ts, current) VALUES ("2018-10-03 14:38:05", 22);
```

## Delete

To facilitate the cleanup of abnormal data caused by equipment failures and other reasons, TDengine supports deleting time-series data based on timestamps. The following SQL deletes all data in the supertable `meters` with timestamps earlier than `2021-10-01 10:40:00.100`. Data deletion is irreversible, so use it with caution. To ensure that the data being deleted is indeed what you want to delete, it is recommended to first use a select statement with the deletion condition in the where clause to view the data to be deleted, and confirm it is correct before executing delete.

```sql
delete from meters where ts < '2021-10-01 10:40:00.100' ;
```
