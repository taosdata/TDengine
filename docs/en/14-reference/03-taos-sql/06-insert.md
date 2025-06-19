---
title: Data Ingestion
slug: /tdengine-reference/sql-manual/insert-data
---

## Writing Syntax

There are two syntaxes supported for writing records: normal syntax and supertable syntax. Under normal syntax, the table name immediately following INSERT INTO is either a subtable name or a regular table name. Under supertable syntax, the table name immediately following INSERT INTO is a supertable name.

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

About Timestamps:

1. TDengine requires that inserted data must have timestamps. Pay attention to the following points regarding the timestamps:

1. Different timestamp formats can affect precision differently. String format timestamps are not affected by the precision setting of the DATABASE they belong to; however, long integer format timestamps are affected by the DATABASE's precision setting. For example, the UNIX seconds for the timestamp "2021-07-13 16:16:48" is 1626164208. Therefore, it needs to be written as 1626164208000 in millisecond precision, 1626164208000000 in microsecond precision, and 1626164208000000000 in nanosecond precision.

1. When inserting multiple rows of data at once, do not set the value of the first column's timestamp to NOW for all rows. This will cause multiple records in the statement to use the same timestamp, potentially leading to data overwriting and not all rows being correctly saved. This happens because the NOW function is resolved to the client execution time of the SQL statement, and multiple NOW markers in the same statement will be replaced with the exact same timestamp value.
   The oldest record timestamp allowed for insertion is relative to the current server time, minus the configured KEEP value (the number of days data is retained, which can be specified when creating the database, default is 3650 days). The newest record timestamp allowed for insertion depends on the database's PRECISION value (timestamp precision, which can be specified when creating the database, ms for milliseconds, us for microseconds, ns for nanoseconds, default is milliseconds): if it is milliseconds or microseconds, the value is January 1, 1970, 00:00:00.000 UTC plus 1000 years, i.e., January 1, 2970, 00:00:00.000 UTC; if it is nanoseconds, the value is January 1, 1970, 00:00:00.000000000 UTC plus 292 years, i.e., January 1, 2262, 00:00:00.000000000 UTC.

Syntax Notes:

1. You can specify the columns for which values are to be inserted; for columns not specified, the database will automatically fill them with NULL.

1. The VALUES syntax indicates the row or rows of data to be inserted.

1. The FILE syntax indicates that the data comes from a CSV file (comma-separated, with each value enclosed in single quotes), which does not require a header. For creating subtables only, refer to the 'Table' section.

1. Both `INSERT ... VALUES` and `INSERT ... FILE` statements can insert data into multiple tables in a single INSERT statement.

1. INSERT statements are fully parsed before execution, preventing situations where data errors occur but table creation succeeds.

```sql
INSERT INTO d1001 USING meters TAGS('Beijing.Chaoyang', 2) VALUES('a');
```

1. When inserting data into multiple subtables, there may still be cases where some data fails to write while other data writes successfully. This is because multiple subtables may be distributed across different VNODEs. After the client fully parses the INSERT statement, it sends the data to each involved VNODE, where each VNODE independently performs the write operation. If a VNODE fails to write due to some reason (such as network issues or disk failure), it will not affect the write operations of other VNODE nodes.
1. The primary key column value must be specified and cannot be NULL.

Standard Syntax Explanation:

1. The USING clause is for automatic table creation syntax. If a user is unsure whether a table exists when writing data, they can use the automatic table creation syntax to create a non-existent table during data writing; if the table already exists, a new table will not be created. Automatic table creation requires using a supertable as a template and specifying the TAGS values for the data table. It is possible to specify only some TAGS column values, with unspecified TAGS columns set to NULL.

1. You can use the `INSERT ... subquery` statement to insert data from TDengine into a specified table. The subquery can be any query statement. This syntax can only be used for subtables and regular tables, and does not support automatic table creation.

Supertable Syntax Explanation:

1. The tbname column must be specified in the field_name list, otherwise, it will result in an error. The tbname column is the subtable name, which is a string type. Characters do not need to be escaped and cannot include the dot '.'.

1. The field_name list supports tag columns. When a subtable already exists, specifying tag values will not trigger a modification of the tag values; when a subtable does not exist, the specified tag values will be used to establish the subtable. If no tag columns are specified, all tag column values are set to NULL.

1. Parameter binding for writing is not supported.

## Inserting a Record

Specify the table name of an already created data subtable, and provide one or more rows of data using the VALUES keyword to write these data into the database. For example, execute the following statement to write a single record:

```sql
INSERT INTO d1001 VALUES (NOW, 10.2, 219, 0.32);
```

## Inserting Multiple Records

Alternatively, you can write two records with the following statement:

```sql
INSERT INTO d1001 VALUES ('2021-07-13 14:06:32.272', 10.2, 219, 0.32) (1626164208000, 10.15, 217, 0.33);
```

## Specifying Columns for Insertion

When inserting records into a data subtable, whether inserting one row or multiple rows, you can map the data to specific columns. For columns not mentioned in the SQL statement, the database will automatically fill them with NULL. The primary key (timestamp) cannot be NULL. For example:

```sql
INSERT INTO d1001 (ts, current, phase) VALUES ('2021-07-13 14:06:33.196', 10.27, 0.31);
```

## Inserting Records into Multiple Tables

You can insert one or more records into multiple tables in a single statement, and also specify columns during the insertion process. For example:

```sql
INSERT INTO d1001 VALUES ('2021-07-13 14:06:34.630', 10.2, 219, 0.32) ('2021-07-13 14:06:35.779', 10.15, 217, 0.33)
            d1002 (ts, current, phase) VALUES ('2021-07-13 14:06:34.255', 10.27, 0.31);
```

## Automatic Table Creation During Record Insertion

If a user is unsure whether a table exists when writing data, they can use the automatic table creation syntax to create a non-existent table during data writing; if the table already exists, a new table will not be created. Automatic table creation requires using a supertable as a template and specifying the TAGS values for the data table. For example:

```sql
INSERT INTO d21001 USING meters TAGS ('California.SanFrancisco', 2) VALUES ('2021-07-13 14:06:32.272', 10.2, 219, 0.32);
```

You can also specify only some TAGS column values during automatic table creation, with unspecified TAGS columns set to NULL. For example:

```sql
INSERT INTO d21001 USING meters (groupId) TAGS (2) VALUES ('2021-07-13 14:06:33.196', 10.15, 217, 0.33);
```

The automatic table creation syntax also supports inserting records into multiple tables in a single statement. For example:

```sql
INSERT INTO d21001 USING meters TAGS ('California.SanFrancisco', 2) VALUES ('2021-07-13 14:06:34.630', 10.2, 219, 0.32) ('2021-07-13 14:06:35.779', 10.15, 217, 0.33)
            d21002 USING meters (groupId) TAGS (2) VALUES ('2021-07-13 14:06:34.255', 10.15, 217, 0.33)
            d21003 USING meters (groupId) TAGS (2) (ts, current, phase) VALUES ('2021-07-13 14:06:34.255', 10.27, 0.31);
```

## Inserting Data Records from a File

In addition to using the VALUES keyword to insert one or more rows of data, you can also place the data to be written in a CSV file (separated by commas, with timestamps and string type values enclosed in single quotes) for SQL commands to read. The CSV file does not need a header. For example, if the content of the /tmp/csvfile.csv file is:

```csv
'2021-07-13 14:07:34.630', 10.2, 219, 0.32
'2021-07-13 14:07:35.779', 10.15, 217, 0.33
```

Then the following command can be used to write the data in this file to the subtable:

```sql
INSERT INTO d1001 FILE '/tmp/csvfile.csv';
```

## Inserting Data Records from a File and Automatically Creating Tables

```sql
INSERT INTO d21001 USING meters TAGS ('California.SanFrancisco', 2) FILE '/tmp/csvfile.csv';
```

You can also insert records into multiple tables in one statement with automatic table creation. For example:

```sql
INSERT INTO d21001 USING meters TAGS ('California.SanFrancisco', 2) FILE '/tmp/csvfile_21001.csv'
            d21002 USING meters (groupId) TAGS (2) FILE '/tmp/csvfile_21002.csv';
```

## Inserting Data into a Supertable and Automatically Creating Subtables

Automatically create tables, with table names specified by the tbname column

```sql
INSERT INTO meters(tbname, location, groupId, ts, current, voltage, phase)
                VALUES ('d31001', 'California.SanFrancisco', 2, '2021-07-13 14:06:34.630', 10.2, 219, 0.32)
                ('d31001', 'California.SanFrancisco', 2, '2021-07-13 14:06:35.779', 10.15, 217, 0.33)
                ('d31002', NULL, 2, '2021-07-13 14:06:34.255', 10.15, 217, 0.33)
```

## Inserting Data into a Supertable from a CSV File and Automatically Creating Subtables

Create subtables for the supertable based on the contents of the CSV file, and populate the respective columns and tags

```sql
INSERT INTO meters(tbname, location, groupId, ts, current, voltage, phase)
                FILE '/tmp/csvfile_21002.csv'
```
