---
sidebar_label: Data Ingestion
title: Data Ingestion
description: Detailed syntax for writing data
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

INSERT INTO stb_name (tbname, field1_name, ...) subquery
```

#### About Primary Key Timestamps

TDengine requires that inserted data must have timestamps. Pay attention to the following points:

1. Different timestamp formats are affected differently by database time precision. String-format timestamps are not affected by the precision of the DATABASE they belong to; long-integer timestamps are. For example, the UNIX seconds for `2021-07-13 16:16:48` is `1626164208`. Write it as `1626164208000` for millisecond precision, `1626164208000000` for microsecond precision, and `1626164208000000000` for nanosecond precision.

2. When inserting multiple rows at once, do not set every first-column timestamp to `NOW`. Otherwise multiple rows in the same statement share one timestamp and may overwrite each other. `NOW` resolves to the client execution time of the SQL statement, so multiple `NOW` markers in one statement become the exact same value.

3. The newest timestamp allowed for insertion is the current time plus 100 years. For example, if the current time is `2024-11-11 12:00:00`, the newest allowed timestamp is `2124-11-11 12:00:00`. The oldest timestamp allowed depends on the database `KEEP` setting. The enterprise edition supports multi-tier storage and can set multiple `KEEP` values. As shown below, if `KEEP` is `100h,100d,3650d`, the oldest allowed timestamp is the current time minus 3650 days. Data with timestamps in `[Now - 100h, Now + 100y)` is kept on tier-1 storage, `[Now - 100d, Now - 100h)` on tier-2, and `[Now - 3650d, Now - 100d)` on tier-3. The community edition does not support multi-tier storage and can only use one `KEEP` value; if multiple values are configured, the maximum is used. If a timestamp is outside the valid range, TDengine returns the error `Timestamp out of range`.

![Keep time-range diagram](../../assets/insert-01.jpg)

#### Syntax Notes

1. You can specify the columns for which values are to be inserted; for columns not specified, the database will automatically fill them with NULL.

2. The VALUES syntax indicates the row or rows of data to be inserted. An English comma between multiple rows is allowed (standard SQL style), for example `VALUES (...), (...)`. Omitting the comma and writing `VALUES (...) (...)` is also valid. When inserting into multiple tables, commas between table clauses are likewise optional.

3. The FILE syntax indicates that the data comes from a CSV file (comma-separated, with each value enclosed in single quotes), which does not require a header. For creating subtables only, see [Tables · Batch creation of subtables](../02-ddl/02-table.md#batch-creation-of-subtables).

4. Both `INSERT ... VALUES` and `INSERT ... FILE` statements can insert data into multiple tables in a single INSERT statement.

5. INSERT statements are fully parsed before execution, preventing situations where data errors occur but table creation succeeds.

```sql
INSERT INTO d1001 USING meters TAGS('Beijing.Chaoyang', 2) VALUES('a');
```

6. When inserting data into multiple subtables, there may still be cases where some data fails to write while other data writes successfully. This is because multiple subtables may be distributed across different VNODEs. After the client fully parses the INSERT statement, it sends the data to each involved VNODE, where each VNODE independently performs the write operation. If a VNODE fails to write due to some reason (such as network issues or disk failure), it will not affect the write operations of other VNODE nodes.

7. The primary key column value must be specified and cannot be NULL.

#### Standard Syntax Explanation

1. The USING clause is for automatic table creation. If a user is unsure whether a table exists when writing data, they can use the automatic table creation syntax to create a non-existent table during data writing; if the table already exists, a new table will not be created and TAGS values will not be modified. Automatic table creation requires using a supertable as a template and specifying the TAGS values for the data table. It is possible to specify only some TAGS column values, with unspecified TAGS columns set to NULL.

2. You can use the `INSERT ... subquery` statement to insert data from TDengine into a specified table. The subquery can be any query statement.

#### Supertable Syntax Explanation

1. The tbname column must be specified in the field_name list, otherwise, it will result in an error. The tbname column is the subtable name, which is a string type. Characters do not need to be escaped and cannot include the dot '.'.

2. The field_name list supports tag columns. When a subtable already exists, specifying tag values will not trigger a modification of the tag values; when a subtable does not exist, the specified tag values will be used to establish the subtable. If no tag columns are specified, all tag column values are set to NULL.

3. Parameter binding for writing is not supported.

4. You can use the `INSERT ... subquery` statement to insert the data from TDengine into a specified super table. The field_name must be specified, and the first field_name must be tbname, otherwise, it will result in an error. Automatic table creation is supported.

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

If a user is unsure whether a table exists when writing data, they can use the automatic table creation syntax to create a non-existent table during data writing; if the table already exists, a new table will not be created and TAGS values will not be modified. Automatic table creation requires using a supertable as a template and specifying the TAGS values for the data table. For example:

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

You can also insert records into multiple tables in one statement with automatic table creation. If a table already exists, TAGS values are not modified. For example:

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
