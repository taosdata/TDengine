---
title: Insert
sidebar_label: Insert
description: This document describes how to insert data into TDengine.
---

## Syntax
The writing of records supports two syntaxes, normal syntax and super table syntax. In the normal syntax, the table name immediately following `INSERT INTO` represents subtable names or regular table names. In the super table syntax, the table name immediately following `INSERT INTO` represents the super table name.
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
### Super Table Syntax
```sql
INSERT INTO
    stb1_name [(field1_name, ...)]       
        VALUES (field1_value, ...) [(field1_value2, ...) ...] | FILE csv_file_path
    [stb2_name [(field1_name, ...)]  
        VALUES (field1_value, ...) [(field1_value2, ...) ...] | FILE csv_file_path
    ...];
```

**Timestamps**

1. All data writes must include a timestamp. With regard to timestamps, note the following:

2. The precision of a timestamp depends on its format. The precision configured for the database affects only timestamps that are inserted as long integers (UNIX time). Timestamps inserted as date and time strings are not affected. As an example, the timestamp 2021-07-13 16:16:48 is equivalent to 1626164208 in UNIX time. This UNIX time is modified to 1626164208000 for databases with millisecond precision, 1626164208000000 for databases with microsecond precision, and 1626164208000000000 for databases with nanosecond precision.

3. If you want to insert multiple rows simultaneously, do not use the NOW function in the timestamp. Using the NOW function in this situation will cause multiple rows to have the same timestamp and prevent them from being stored correctly. This is because the NOW function obtains the current time on the client, and multiple instances of NOW in a single statement will return the same time.
   The earliest timestamp that you can use when inserting data is equal to the current time on the server minus the value of the KEEP parameter (You can configure the KEEP parameter when you create a database and the default value is 3650 days). The latest timestamp you can use when inserting data depends on the PRECISION parameter (You can configure the PRECISION parameter when you create a database, ms means milliseconds, us means microseconds, ns means nanoseconds, and the default value is milliseconds). If the timestamp precision is milliseconds or microseconds, the latest timestamp is the Unix epoch (January 1st, 1970 at 00:00:00.000 UTC) plus 1000 years, that is, January 1st, 2970 at 00:00:00.000 UTC; If the timestamp precision is nanoseconds, the latest timestamp is the Unix epoch plus 292 years, that is, January 1st, 2262 at 00:00:00.000000000 UTC.

**Syntax**

1. You can insert data into specified columns. Any columns in which you do not insert data will be assigned a null value.

2. The VALUES clause inserts one or more rows of data into a table.

3. The FILE clause inserts tags or data from a comma-separates values (CSV) file. Do not include headers in your CSV files.

4. A single `INSERT ... VALUES` statement and `INSERT ... FILE` statement can write data to multiple tables.

5. The INSERT statement is fully parsed before being executed, so that if any element of the statement fails, the entire statement will fail. For example, the following statement will not create a table because the latter part of the statement is invalid:

   ```sql
   INSERT INTO d1001 USING meters TAGS('Beijing.Chaoyang', 2) VALUES('a');
   ```

6. However, an INSERT statement that writes data to multiple subtables can succeed for some tables and fail for others. This situation is caused because vnodes perform write operations independently of each other. One vnode failing to write data does not affect the ability of other vnodes to write successfully.

**Normal Syntax**
1. The USING clause automatically creates the specified subtable if it does not exist. If it's unknown whether the table already exists, the table can be created automatically while inserting using the SQL statement below. To use this functionality, a STable must be used as template and tag values must be provided. Any tags that you do not specify will be assigned a null value.

2. Data from TDengine can be inserted into a specified table using the `INSERT ... subquery` statement. Arbitrary query statements are supported. This syntax can only be used for subtables and normal tables, and does not support automatic table creation.

**Super Table Syntax**

1. The tbname column must be included in the field_name list and represents the name of the child table. This column is of string type, and the use of the . character is not permitted in the tbname column.

2. Tag columns are eligible for inclusion in the field_name list. If the specified child table doesn't exist, a new child table will be generated with the provided tag values. In the absence of specified tag values, the newly created table will have all NULL tag values. Existing child table tag values remain unchanged.

3. Param binding is not supported.
## Insert a Record

Single row or multiple rows specified with VALUES can be inserted into a specific table. A single row is inserted using the below statement.

```sql
INSERT INTO d1001 VALUES (NOW, 10.2, 219, 0.32);
```

## Insert Multiple Records

Double rows are inserted using the below statement.

```sql
INSERT INTO d1001 VALUES ('2021-07-13 14:06:32.272', 10.2, 219, 0.32) (1626164208000, 10.15, 217, 0.33);
```

## Write to a Specified Column

Data can be inserted into specific columns, either single row or multiple row, while other columns will be inserted as NULL value. The key (timestamp) cannot be null. For example:

```sql
INSERT INTO d1001 (ts, current, phase) VALUES ('2021-07-13 14:06:33.196', 10.27, 0.31);
```

## Insert Into Multiple Tables

One or multiple rows can be inserted into multiple tables in a single SQL statement, with or without specifying specific columns. For example:

```sql
INSERT INTO d1001 VALUES ('2021-07-13 14:06:34.630', 10.2, 219, 0.32) ('2021-07-13 14:06:35.779', 10.15, 217, 0.33)
            d1002 (ts, current, phase) VALUES ('2021-07-13 14:06:34.255', 10.27, 0.31);
```

## Automatically Create Table When Inserting

If it's unknown whether the table already exists, the table can be created automatically while inserting using the SQL statement below. To use this functionality, a STable must be used as template and tag values must be provided. For example:

```sql
INSERT INTO d21001 USING meters TAGS ('California.SanFrancisco', 2) VALUES ('2021-07-13 14:06:32.272', 10.2, 219, 0.32);
```

It's not necessary to provide values for all tags when creating tables automatically, the tags without values provided will be set to NULL. For example:

```sql
INSERT INTO d21001 USING meters (groupId) TAGS (2) VALUES ('2021-07-13 14:06:33.196', 10.15, 217, 0.33);
```

Multiple rows can also be inserted into the same table in a single SQL statement. For example:

```sql
INSERT INTO d21001 USING meters TAGS ('California.SanFrancisco', 2) VALUES ('2021-07-13 14:06:34.630', 10.2, 219, 0.32) ('2021-07-13 14:06:35.779', 10.15, 217, 0.33)
            d21002 USING meters (groupId) TAGS (2) VALUES ('2021-07-13 14:06:34.255', 10.15, 217, 0.33)
            d21003 USING meters (groupId) TAGS (2) (ts, current, phase) VALUES ('2021-07-13 14:06:34.255', 10.27, 0.31);
```

## Insert Rows From A File

Besides using `VALUES` to insert one or multiple rows, the data to be inserted can also be prepared in a CSV file with comma as separator and timestamp and string field value quoted by single quotes. Table definition is not required in the CSV file. For example, if file "/tmp/csvfile.csv" contains the below data:

```
'2021-07-13 14:07:34.630', 10.2, 219, 0.32
'2021-07-13 14:07:35.779', 10.15, 217, 0.33
```

Then data in this file can be inserted by the SQL statement below:

```sql
INSERT INTO d1001 FILE '/tmp/csvfile.csv';
```

## Create Tables Automatically and Insert Rows From File

```sql
INSERT INTO d21001 USING meters TAGS ('California.SanFrancisco', 2) FILE '/tmp/csvfile.csv';
```

When writing data from a file, you can automatically create the specified subtable if it does not exist. For example:

```sql
INSERT INTO d21001 USING meters TAGS ('California.SanFrancisco', 2) FILE '/tmp/csvfile_21001.csv'
            d21002 USING meters (groupId) TAGS (2) FILE '/tmp/csvfile_21002.csv';
```
## Super Table Syntax

Automatically creating table and the table name is specified through the `tbname` column

```sql
INSERT INTO meters(tbname, location, groupId, ts, current, voltage, phase) 
                values('d31001', 'California.SanFrancisco', 2, '2021-07-13 14:06:34.630', 10.2, 219, 0.32) 
                ('d31001', 'California.SanFrancisco', 2, '2021-07-13 14:06:35.779', 10.15, 217, 0.33)
                ('d31002', NULL, 2, '2021-07-13 14:06:34.255', 10.15, 217, 0.33)        
```

