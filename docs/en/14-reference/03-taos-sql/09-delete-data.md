---
title: Data Deletion
slug: /tdengine-reference/sql-manual/delete-data
---

Deleting data is a feature provided by TDengine that allows users to delete data records from specified tables or supertables within a specified time period, facilitating the cleanup of abnormal data caused by device failures and other reasons.

**Note**: Deleting data does not immediately free up the disk space occupied by the table. Instead, the data of the table is marked as deleted. These data will not appear in queries, but the release of disk space will be delayed until the system automatically or the user manually reorganizes the data.

**Syntax:**

```sql
DELETE FROM [ db_name. ] tb_name [WHERE condition];
```

**Functionality**: Delete data records from specified tables or supertables

**Parameters:**

- `db_name`: Optional parameter, specifies the database name where the table to be deleted is located. If not specified, it defaults to the current database.
- `tb_name`: Required parameter, specifies the name of the table from which data is to be deleted. It can be a basic table, a subtable, or a supertable.
- `condition`: Optional parameter, specifies the filtering condition for deleting data. If no filtering condition is specified, all data in the table will be deleted. Use with caution. Note that the where condition only supports filtering on the first column, which is the time column.

**Special Note:**

Once data is deleted, it cannot be recovered. Use with caution. To ensure that the data you are deleting is indeed what you intend to delete, it is recommended to first use the `select` statement with the `where` condition to view the data to be deleted. Confirm it is correct before executing the `delete` command.

**Example:**

`meters` is a supertable, and `groupid` is an int type tag column. Now, to delete all data from the `meters` table where the time is less than 2021-10-01 10:40:00.100, the SQL is as follows:

```sql
delete from meters where ts < '2021-10-01 10:40:00.100' ;
```

After execution, the result is displayed as:

```text
Deleted 102000 row(s) from 1020 table(s) (0.421950s)
```

This indicates that a total of 102000 rows of data were deleted from 1020 subtables.
