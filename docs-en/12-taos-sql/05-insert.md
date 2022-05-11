---
sidebar_label: Insert
title: Insert
---

## Syntax

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
```

## Insert Single or Multiple Rows

Single row or multiple rows specified with VALUES can be inserted into a specific table. For example

Single row is inserted using below statement.

```sq;
INSERT INTO d1001 VALUES (NOW, 10.2, 219, 0.32);
```

Double rows can be inserted using below statement.

```sql
INSERT INTO d1001 VALUES ('2021-07-13 14:06:32.272', 10.2, 219, 0.32) (1626164208000, 10.15, 217, 0.33);
```

:::note

1. In the second example above, different formats are used in the two rows to be inserted. In the first row, the timestamp format is a date and time string, which is interpreted from the string value only. In the second row, the timestamp format is a long integer, which will be interpreted based on the database time precision.
2. When trying to insert multiple rows in single statement, only the timestamp of one row can be set as NOW, otherwise there will be duplicate timestamps among the rows and the result may be out of expectation because NOW will be interpreted as the time when the statement is executed.
3. The oldest timestamp that is allowed is subtracting the KEEP parameter from current time.
4. The newest timestamp that is allowed is adding the DAYS parameter to current time.

:::

## Insert Into Specific Columns

Data can be inserted into specific columns, either single row or multiple row, while other columns will be inserted as NULL value.

```
INSERT INTO d1001 (ts, current, phase) VALUES ('2021-07-13 14:06:33.196', 10.27, 0.31);
```

:::info
If no columns are explicitly specified, all the columns must be provided with values, this is called "all column mode". The insert performance of all column mode is much better than specifying a part of columns, so it's encouraged to use "all column mode" while providing NULL value explicitly for the columns for which no actual value can be provided.

:::

## Insert Into Multiple Tables

One or multiple rows can be inserted into multiple tables in single SQL statement, with or without specifying specific columns.

```sql
INSERT INTO d1001 VALUES ('2021-07-13 14:06:34.630', 10.2, 219, 0.32) ('2021-07-13 14:06:35.779', 10.15, 217, 0.33)
            d1002 (ts, current, phase) VALUES ('2021-07-13 14:06:34.255', 10.27, 0.31）;
```

## Automatically Create Table When Inserting

If it's not sure whether the table already exists, the table can be created automatically while inserting using below SQL statement. To use this functionality, a STable must be used as template and tag values must be provided.

```sql
INSERT INTO d21001 USING meters TAGS ('Beijing.Chaoyang', 2) VALUES ('2021-07-13 14:06:32.272', 10.2, 219, 0.32);
```

It's not necessary to provide values for all tag when creating tables automatically, the tags without values provided will be set to NULL.

```sql
INSERT INTO d21001 USING meters (groupId) TAGS (2) VALUES ('2021-07-13 14:06:33.196', 10.15, 217, 0.33);
```

Multiple rows can also be inserted into same table in single SQL statement using this way.自

```sql
INSERT INTO d21001 USING meters TAGS ('Beijing.Chaoyang', 2) VALUES ('2021-07-13 14:06:34.630', 10.2, 219, 0.32) ('2021-07-13 14:06:35.779', 10.15, 217, 0.33)
            d21002 USING meters (groupId) TAGS (2) VALUES ('2021-07-13 14:06:34.255', 10.15, 217, 0.33)
            d21003 USING meters (groupId) TAGS (2) (ts, current, phase) VALUES ('2021-07-13 14:06:34.255', 10.27, 0.31);
```

:::info
Prior to version 2.0.20.5, when using `INSERT` to create table automatically and specify the columns, the column names must follow the table name immediately. From version 2.0.20.5, the column names can follow the table name immediately, also can be put between `TAGS` and `VALUES`. In same SQL statement, however, these two ways of specifying column names can't be mixed.
:::

## Insert Rows From A File

Besides using `VALUES` to insert one or multiple rows, the data to be inserted can also be prepared in a CSV file with comma as separator and each field value quoted by single quotes. Table definition is not required in the CSV file. For example, if file "/tmp/csvfile.csv" contains below data:

```
'2021-07-13 14:07:34.630', '10.2', '219', '0.32'
'2021-07-13 14:07:35.779', '10.15', '217', '0.33'
```

Then data in this file can be inserted by below SQL statement:

```sql
INSERT INTO d1001 FILE '/tmp/csvfile.csv';
```

## CreateTables Automatically and Insert Rows From File

From version 2.1.5.0, tables can be automatically created using a super table as template when inserting data from a CSV file, Like below:

```sql
INSERT INTO d21001 USING meters TAGS ('Beijing.Chaoyang', 2) FILE '/tmp/csvfile.csv';
```

Multiple tables can be automatically created and inserted in single SQL statement, like below:也

```sql
INSERT INTO d21001 USING meters TAGS ('Beijing.Chaoyang', 2) FILE '/tmp/csvfile_21001.csv'
            d21002 USING meters (groupId) TAGS (2) FILE '/tmp/csvfile_21002.csv';
```

## More About Insert

For SQL statement like `insert`, stream parsing strategy is applied. That means before an error is found and the execution is aborted, the part prior to the error point has already been executed. Below is an experiment to help understand the behavior.

Firstly, a super table is created.

```sql
CREATE TABLE meters(ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS(location BINARY(30), groupId INT);
```

It can be proved that the super table has been created by `SHOW STABLES`, but no table exists by `SHOW TABLES`.

```
taos> SHOW STABLES;
              name              |      created_time       | columns |  tags  |   tables    |
============================================================================================
 meters                         | 2020-08-06 17:50:27.831 |       4 |      2 |           0 |
Query OK, 1 row(s) in set (0.001029s)

taos> SHOW TABLES;
Query OK, 0 row(s) in set (0.000946s)
```

Then, try to create table d1001 automatically when inserting data into it.

```sql
INSERT INTO d1001 USING meters TAGS('Beijing.Chaoyang', 2) VALUES('a');
```

The output shows the value to be inserted is invalid. But `SHOW TABLES` proves that the table has been created automatically by the `INSERT` statement.

```
DB error: invalid SQL: 'a' (invalid timestamp) (0.039494s)

taos> SHOW TABLES;
           table_name           |      created_time       | columns |          stable_name           |
======================================================================================================
 d1001                          | 2020-08-06 17:52:02.097 |       4 | meters                         |
Query OK, 1 row(s) in set (0.001091s)
```

From the above experiment, we can see that even though the value to be inserted is invalid but the table is still created.
