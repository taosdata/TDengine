---
sidebar_label: Update
title: Data Update
description: Use UPDATE to change data and tags in a table, subtable, or supertable
---

`UPDATE` changes time-series data that has already been written, or changes the tag values of a subtable (point information). Typical uses are correcting a bad device reading, adjusting a time range in one statement, filling a missing column value, and maintaining point attributes such as location and group.

The examples below use the tables from [Data Ingestion](./01-insert.md). The supertable `meters` has data columns `ts`, `current`, `voltage`, and `phase`, and tag columns `location` and `groupId`. `d1001` and `d1002` are its subtables.

## Update Syntax

```sql
UPDATE [db_name.]tb_name
  SET col_name = expr [, col_name = expr] ...
  [WHERE condition];
```

### Parameters

- `db_name`: optional. The database that contains the table. If omitted, the current database is used.
- `tb_name`: required. The table to update. It can be a normal table, a subtable, or a supertable. An update on a supertable applies to every subtable that matches the condition.
- `col_name`: the column to change. It can be an ordinary data column or a tag column. See [Columns That Can Be Updated](#columns-that-can-be-updated).
- `expr`: the new-value expression. It can be:
  - A constant, such as `220`, `'California.SanJose'`, or `NULL`.
  - A reference to another column or tag in the same row, such as `current * 1.05` or `voltage + 1`.
  - A combination of scalar functions and operators, such as `ROUND(current, 2)` or `CAST(voltage AS DOUBLE) / 1000`.
  - Aggregate functions, selection functions, window functions, time-series-specific functions, and subqueries are not supported.
- `condition`: optional. Selects the rows to update. The predicates supported are the same as a `WHERE` clause of `SELECT` on the same table. You can combine the primary timestamp, tag columns, `tbname`, and ordinary columns, and you can use `AND`, `OR`, and parentheses. Subqueries are not supported. If `WHERE` is omitted, every row in the table is updated. Use that form with care.

### Columns That Can Be Updated {#columns-that-can-be-updated}

| Column type | Can be updated | Description |
| --- | --- | --- |
| Primary timestamp column (the first column) | No | The timestamp identifies the row. To move a row in time, write the data at the new timestamp and then delete the old row. |
| Composite primary key column | No | The composite primary key and the timestamp together identify a row. They cannot be changed. |
| Ordinary data column | Yes | Changes the column value of matching rows. Columns that do not appear in `SET` keep their values. |
| Tag column | Yes (supertable and subtable only) | Changes the tag value of matching subtables. See [Update Tags](#update-tags). |

Updating a primary key column returns the error `Cannot update primary key column 'col_name'`. One `SET` clause cannot contain both an ordinary data column and a tag column, and it cannot name the same column twice.

Type conversion of the new value follows the same rules as `INSERT`. A number can be written to a numeric column whose range is large enough. A string cannot be longer than the column definition. An out-of-range or over-length value returns an error and is not truncated. `NULL` cannot be written to a column that does not allow nulls.

## Execution Semantics

### Update Data Columns

`UPDATE` runs in these steps:

1. Find the rows that match `WHERE`.
2. For each row, evaluate the `SET` expressions with the column values from before the update. Every expression in the statement uses the pre-update values, regardless of the order of columns in `SET`.
3. Keep the primary key, replace the columns named in `SET`, leave the other columns unchanged, and write the row back.

`UPDATE` changes only existing rows. It does not insert a row. If no row matches, the statement succeeds and the updated row count is 0.

### Update Tags {#update-tags}

When `SET` contains only tag columns, `UPDATE` changes the tag values of the matching subtables. This is equivalent to running `ALTER TABLE ... SET TAG` on each of those subtables. See [Tables · Modify Subtable Tag Value](../02-ddl/02-table.md#modify-subtable-tag-value). In this case:

- `WHERE` can use only tag columns and `tbname`, to select subtables. It cannot use the time column or ordinary data columns.
- The count is the number of subtables.
- The new tag values take effect for later queries. Queries, stream processing, and subscriptions that group by tags use the new values.
- A `JSON` tag cannot be changed with `UPDATE`. Use `ALTER TABLE ... SET TAG`.

### Atomicity and Consistency

- **One row is atomic**: all new values of one row are written together. A query never reads a row in which only some of the columns have been updated.
- **The statement is not globally atomic**: subtables matched by one `UPDATE` can reside on several vnodes, and each vnode runs independently. If one vnode fails, updates already completed on the other vnodes are not rolled back, and the statement returns an error. This matches [Partial Success](./01-insert.md#partial-success) for `INSERT`.
- **Retry**: when `SET` uses only constants, running the same statement again produces the same result, so you can retry it after a failure. When `SET` refers to the column being updated, for example `SET current = current * 1.05`, running it again calculates again. First use `SELECT` to see which rows are already updated, then narrow `WHERE` and retry.
- **Concurrent writes**: when `UPDATE` and `INSERT` change the same row at the same time, the write that finishes later is kept.

### Difference from INSERT Overwrite

Writing a row with the same primary key makes `INSERT` replace the old row with the new row. The two statements differ as follows:

| Item | `INSERT` overwrite | `UPDATE` |
| --- | --- | --- |
| How a row is selected | By the primary key values being written. One statement overwrites those rows. | By the `WHERE` condition. One statement can change many rows. |
| When the row does not exist | A new row is inserted | No row is inserted, and the updated row count is 0 |
| Compute from the current value | Not supported | Supported, for example `SET current = current * 1.05` |
| Change tags | Not supported | Supported |

### Effect on Storage, Subscriptions, and Stream Processing

- The updated row is stored as new data with the same primary key. The old version is removed during data reorganization, the same as an `INSERT` overwrite.
- A data subscription receives the updated row.
- Stream processing treats the update as a change to data at an existing time and recalculates according to the stream options for out-of-order data and updates. See [Stream Processing](../../07-stream-processing/index.md).

## Result

A successful data update returns the number of rows and tables:

```text
Updated 120 row(s) from 3 table(s) (0.035210s)
```

A tag update returns the number of subtables:

```text
Updated tags of 2 table(s) (0.012034s)
```

## Privileges

| Operation | Required privilege |
| --- | --- |
| Update data columns | `SELECT` and `INSERT` on the table. If `INSERT` is limited to columns, every column in `SET` must be among the granted columns. If the privilege has a row condition (`WITH`), only rows that match the condition can be updated. |
| Update tag columns | `ALTER` on the table, the same privilege required by `ALTER TABLE ... SET TAG`. |

For the grant syntax, see [Permissions](../07-user-and-privilege/02-grant.md).

## Examples

### Correct One Timestamp

Set the current of `d1001` at `2021-07-13 14:06:32.272` to `10.3`:

```sql
UPDATE d1001 SET current = 10.3 WHERE ts = '2021-07-13 14:06:32.272';
```

### Change Several Columns

```sql
UPDATE d1001
SET current = 10.3, voltage = 219, phase = 0.31
WHERE ts = '2021-07-13 14:06:32.272';
```

### Correct a Range

Set abnormal voltage readings above 250V to 220V for devices whose `groupId` is 2, during one hour:

```sql
UPDATE meters
SET voltage = 220
WHERE groupId = 2
  AND ts >= '2021-10-01 10:00:00'
  AND ts <  '2021-10-01 11:00:00'
  AND voltage > 250;
```

### Compute from the Current Value

After calibration, the current of `d1002` on one day is 5% low. Correct it from the stored value:

```sql
UPDATE d1002
SET current = current * 1.05
WHERE ts >= '2021-10-01 00:00:00' AND ts < '2021-10-02 00:00:00';
```

### Set a Column to NULL

```sql
UPDATE d1001 SET phase = NULL WHERE ts = '2021-07-13 14:06:32.272';
```

### Update Point Information

Change the location of point `d1001` to `California.SanJose`:

```sql
UPDATE meters SET location = 'California.SanJose' WHERE tbname = 'd1001';
```

Move every point in `California.SanFrancisco` to group 3:

```sql
UPDATE meters SET groupId = 3 WHERE location = 'California.SanFrancisco';
```

Query the point information to confirm the result:

```sql
SELECT
  DISTINCT tbname,
  location,
  groupId
FROM meters;
```

## Update Through Parameter Binding

With the STMT / STMT2 API, prepare an `UPDATE` statement and bind parameters in batches. This suits a program that frequently corrects single points:

```sql
UPDATE meters SET current = ?, voltage = ? WHERE tbname = ? AND ts = ?
```

Limits of parameter binding:

- Every new value in `SET` must be a `?` placeholder.
- `WHERE` must give every primary key with `?`: the timestamp column, and the composite primary key column if there is one.
- Other `WHERE` predicates have the same meaning as in SQL `UPDATE`. A row is updated only when it matches every condition.
- Tag columns cannot be updated.

For the API, see [Parameter Binding](../../10-developer-guide/03-stmt.md).

## Limits

- `UPDATE` is not supported on virtual tables, views, system tables, or tables in `information_schema` and `performance_schema`.
- `ORDER BY`, `LIMIT`, `JOIN`, and multi-table `UPDATE` are not supported.
- The primary timestamp column and composite primary key columns cannot be changed.
- `UPDATE` reads the matching rows and writes them back. The time is about the same as a `SELECT` with the same condition plus a write of the same amount of data. For a large correction, run it in time-range batches, and include a time range and tag conditions in `WHERE` to reduce the scan.

## Recommendations

- Before running the statement, run `SELECT COUNT(*)` with the same `WHERE` and confirm that the number of matching rows is what you expect.
- For important data, export the rows to be updated with `SELECT` before running `UPDATE`.
- To keep a record of the change, enable [Audit and Compliance](../../11-security-guide/07-audit-and-compliance.md). When `auditLevel` is 5, `UPDATE` is recorded in the audit log in the same way as `INSERT` and `DELETE`.
