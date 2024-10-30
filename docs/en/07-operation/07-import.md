---
title: Data Import
description: This document describes how to import data into TDengine.
---

There are multiple ways of importing data provided by TDengine: import with script, import from data file, import using `taosdump`.

## Import Using Script

TDengine CLI `taos` supports `source <filename>` command for executing the SQL statements in the file in batch. The SQL statements for creating databases, creating tables, and inserting rows can be written in a single file with one statement on each line, then the file can be executed using the `source` command in TDengine CLI `taos` to execute the SQL statements in order and in batch. In the script file, any line beginning with "#" is treated as comments and ignored silently.

## Import from Data File

In TDengine CLI, data can be imported from a CSV file into an existing table. The data in a single CSV must belong to the same table and must be consistent with the schema of that table. The SQL statement is as below:

```sql
insert into tb1 file 'path/data.csv';
```

:::note
If there is a description in the first line of the CSV file, please remove it before importing. If there is no value for a column, please use `NULL` without quotes.

:::

For example, there is a subtable d1001 whose schema is as below:

```sql
taos> DESCRIBE d1001
             Field              |        Type        |   Length    |    Note    |
=================================================================================
 ts                             | TIMESTAMP          |           8 |            |
 current                        | FLOAT              |           4 |            |
 voltage                        | INT                |           4 |            |
 phase                          | FLOAT              |           4 |            |
 location                       | BINARY             |          64 | TAG        |
 groupid                        | INT                |           4 | TAG        |
```

The format of the CSV file to be imported, data.csv, is as below:

```csv
'2018-10-04 06:38:05.000',10.30000,219,0.31000
'2018-10-05 06:38:15.000',12.60000,218,0.33000
'2018-10-06 06:38:16.800',13.30000,221,0.32000
'2018-10-07 06:38:05.000',13.30000,219,0.33000
'2018-10-08 06:38:05.000',14.30000,219,0.34000
'2018-10-09 06:38:05.000',15.30000,219,0.35000
'2018-10-10 06:38:05.000',16.30000,219,0.31000
'2018-10-11 06:38:05.000',17.30000,219,0.32000
'2018-10-12 06:38:05.000',18.30000,219,0.31000
```

Then, the below SQL statement can be used to import data from file "data.csv", assuming the file is located under the home directory of the current Linux user.

```sql
taos> insert into d1001 file '~/data.csv';
Query OK, 9 row(s) affected (0.004763s)
```

## Import using taosdump

A convenient tool for importing and exporting data is provided by TDengine, `taosdump`, which can be used to export data from one TDengine cluster and import into another one. For the details of using `taosdump` please refer to the taosdump documentation.
