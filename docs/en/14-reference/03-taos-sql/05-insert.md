---
title: Insert Data
description: Detailed syntax for writing data
slug: /tdengine-reference/sql-manual/insert-data
---

## Insertion Syntax

Data insertion supports two syntax types: normal syntax and supertable syntax. In normal syntax, the table name immediately following `INSERT INTO` is either a subtable name or a basic table name. In supertable syntax, the table name immediately following `INSERT INTO` is the supertable name.

### Normal Syntax

```sql
INSERT INTO
    tb_name
        [USING stb_name [(tag1_name, ...)] TAGS (tag1_value, ...)]
        [(field1_name, ...)]
        VALUES (field1_value, ...) [(field1_value2, ...) ...] | FILE csv_file_path
    [tb2_name
        [USING stb_name [(tag1_name, ...)] TAGS (tag1_value, ...)]
        [(field1_name, ...)]
        VALUES (field1_value, ...) [(field1_value2, ...) ...] | FILE csv_file_path
    ...];

INSERT INTO tb_name [(field1_name, ...)] subquery
```

### Supertable Syntax

```sql
INSERT INTO
    stb1_name [(field1_name, ...)]       
        VALUES (field1_value, ...) [(field1_value2, ...) ...] | FILE csv_file_path
    [stb2_name [(field1_name, ...)]  
        VALUES (field1_value, ...) [(field1_value2, ...) ...] | FILE csv_file_path
    ...];
```

**About Timestamps**

1. TDengine requires that the inserted data must have timestamps. Please note the following points regarding the timestamps used for data insertion:

2. Different formats of timestamps will have different precision impacts. The string format of timestamps is not affected by the time precision settings of the DATABASE; however, the long integer format of timestamps is influenced by the time precision settings of the DATABASE. For example, the UNIX seconds for the timestamp "2021-07-13 16:16:48" is 1626164208. Therefore, under millisecond precision, it should be written as 1626164208000, under microsecond precision it should be written as 1626164208000000, and under nanosecond precision, it should be written as 1626164208000000000.

3. When inserting multiple rows of data, do not set the values of the timestamp column to NOW. Otherwise, multiple records in the statement will have the same timestamp, which may result in overlaps that prevent all data rows from being saved correctly. This is because the NOW function will be parsed as the client execution time of the SQL statement, and multiple occurrences of NOW in the same statement will be replaced with the exact same timestamp value.
   The earliest allowed timestamp for inserted records is relative to the current server time minus the configured KEEP value (the number of days data is retained, which can be specified when creating the database; the default value is 3650 days). The latest allowed timestamp for inserted records depends on the DATABASE's PRECISION value (timestamp precision, which can be specified when creating the database; ms indicates milliseconds, us indicates microseconds, ns indicates nanoseconds, with the default being milliseconds): if it is milliseconds or microseconds, the value is UTC 00:00:00.000 on January 1, 2970, plus 1000 years; if it is nanoseconds, the value is UTC 00:00:00.000000000 on January 1, 2262, plus 292 years.

**Syntax Explanation**

1. You can specify the columns for which values are being inserted, and the database will automatically fill the unspecified columns with NULL.

2. The VALUES syntax represents one or more rows of data to be inserted.

3. The FILE syntax indicates that data comes from a CSV file (comma-separated, with values enclosed in single quotes), and the CSV file does not require a header.

4. Both `INSERT ... VALUES` statements and `INSERT ... FILE` statements can insert data into multiple tables within a single INSERT statement.

5. The INSERT statement is completely parsed before execution, so for the following statement, there will be no data errors, but the table will be created successfully:

   ```sql
   INSERT INTO d1001 USING meters TAGS('Beijing.Chaoyang', 2) VALUES('a');
   ```

6. In cases where data is being inserted into multiple subtables, there can still be instances where some data writes fail while others succeed. This is because multiple subtables may be distributed across different VNODEs. The client will fully parse the INSERT statement and send the data to the involved VNODEs, each of which will independently perform the write operation. If a particular VNODE fails to write due to issues (such as network problems or disk failures), it will not affect the writes to other VNODE nodes.

7. The primary key column value must be specified and cannot be NULL.

**Normal Syntax Explanation**

1. The USING clause is the auto-create table syntax. If the user is unsure whether a table exists when writing data, they can use the auto-create table syntax to create the non-existent table; if the table already exists, a new table will not be created. When auto-creating a table, it is required to use a supertable as a template and specify the tag values for the data table. It is also possible to specify only a portion of the tag columns, with unspecified tag columns set to NULL.

2. You can use the `INSERT ... subquery` statement to insert data from TDengine into the specified table. The subquery can be any query statement. This syntax can only be used for subtables and basic tables and does not support auto-creating tables.

**Supertable Syntax Explanation**

1. The tbname column must be specified in the field_name list; otherwise, an error will occur. The tbname column is the subtable name, and its type is string. Characters do not need to be escaped and cannot include a dot '.'.

2. The field_name list supports tag columns. When the subtable already exists, specifying tag values will not trigger a change in tag values; when the subtable does not exist, it will create the subtable using the specified tag values. If no tag columns are specified, all tag values will be set to NULL.

3. Parameter binding for insertion is not supported.

## Insert a Record

To write data to the database, specify the name of the already created subtable and provide one or more rows of data through the VALUES keyword. For example, executing the following statement can write one record:

```sql
INSERT INTO d1001 VALUES (NOW, 10.2, 219, 0.32);
```

## Insert Multiple Records

Alternatively, you can write two records with the following statement:

```sql
INSERT INTO d1001 VALUES ('2021-07-13 14:06:32.272', 10.2, 219, 0.32) (1626164208000, 10.15, 217, 0.33);
```

## Specify Columns for Insertion

When inserting records into the data subtable, you can map the data to specified columns, regardless of whether you are inserting a single row or multiple rows. For the columns not mentioned in the SQL statement, the database will automatically fill them with NULL. The primary key (timestamp) cannot be NULL. For example:

```sql
INSERT INTO d1001 (ts, current, phase) VALUES ('2021-07-13 14:06:33.196', 10.27, 0.31);
```

## Insert Records into Multiple Tables

You can insert one or multiple records into multiple tables in a single statement, and you can also specify columns during the insertion process. For example:

```sql
INSERT INTO d1001 VALUES ('2021-07-13 14:06:34.630', 10.2, 219, 0.32) ('2021-07-13 14:06:35.779', 10.15, 217, 0.33)
            d1002 (ts, current, phase) VALUES ('2021-07-13 14:06:34.255', 10.27, 0.31);
```

## Automatically Create Tables When Inserting Records

If the user is unsure whether a certain table exists while writing data, they can use the auto-create table syntax to create non-existent tables; if the table already exists, a new table will not be created. When auto-creating a table, it is required to use a supertable as a template and specify the tag values for the data table. For example:

```sql
INSERT INTO d21001 USING meters TAGS ('California.SanFrancisco', 2) VALUES ('2021-07-13 14:06:32.272', 10.2, 219, 0.32);
```

You can also specify only some tag column values during auto-creation, and unspecified tag columns will be set to NULL. For example:

```sql
INSERT INTO d21001 USING meters (groupId) TAGS (2) VALUES ('2021-07-13 14:06:33.196', 10.15, 217, 0.33);
```

The auto-create table syntax also supports inserting records into multiple tables in a single statement. For example:

```sql
INSERT INTO d21001 USING meters TAGS ('California.SanFrancisco', 2) VALUES ('2021-07-13 14:06:34.630', 10.2, 219, 0.32) ('2021-07-13 14:06:35.779', 10.15, 217, 0.33)
            d21002 USING meters (groupId) TAGS (2) VALUES ('2021-07-13 14:06:34.255', 10.15, 217, 0.33)
            d21003 USING meters (groupId) TAGS (2) (ts, current, phase) VALUES ('2021-07-13 14:06:34.255', 10.27, 0.31);
```

## Insert Records from a File

In addition to using the VALUES keyword to insert one or multiple rows of data, you can also place the data to be written in a CSV file (comma-separated, with timestamp and string-type values enclosed in single quotes) for the SQL statement to read. The CSV file does not require a header. For example, if the content of the `/tmp/csvfile.csv` file is:

```csv
'2021-07-13 14:07:34.630', 10.2, 219, 0.32
'2021-07-13 14:07:35.779', 10.15, 217, 0.33
```

Then you can write the data from this file into the subtable with the following statement:

```sql
INSERT INTO d1001 FILE '/tmp/csvfile.csv';
```

## Insert Records from a File and Automatically Create Tables

```sql
INSERT INTO d21001 USING meters TAGS ('California.SanFrancisco', 2) FILE '/tmp/csvfile.csv';
```

You can also insert records into multiple tables using the auto-create table method in a single statement. For example:

```sql
INSERT INTO d21001 USING meters TAGS ('California.SanFrancisco', 2) FILE '/tmp/csvfile_21001.csv'
            d21002 USING meters (groupId) TAGS (2) FILE '/tmp/csvfile_21002.csv';
```

## Supertable Syntax

When automatically creating tables, the table names are specified through the tbname column.

```sql
INSERT INTO meters(tbname, location, groupId, ts, current, voltage, phase) 
                values('d31001', 'California.SanFrancisco', 2, '2021-07-13 14:06:34.630', 10.2, 219, 0.32) 
                ('d31001', 'California.SanFrancisco', 2, '2021-07-13 14:06:35.779', 10.15, 217, 0.33)
                ('d31002', NULL, 2, '2021-07-13 14:06:34.255', 10.15, 217, 0.33)        
```
