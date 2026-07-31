---
sidebar_label: Data Deletion
title: Data Deletion
description: Delete data from specified tables or supertables
---

## Delete Syntax

The `DELETE` statement deletes data from specified tables or supertables, which is useful for cleaning up abnormal data caused by device failures and other reasons.

```sql
DELETE FROM [db_name.]tb_name [WHERE condition] [SECURE_DELETE];
```

**Note:** Deleting data does not immediately free disk space; rows are marked as deleted. They no longer appear in queries, but space is reclaimed later when the database [`KEEP`](../02-ddl/01-database.md) setting takes effect, or when you manually run [Data Reorganization](../../12-operations-and-tooling/02-operations/04-maintenance.md#data-reorganization) (enterprise `COMPACT`). To physically overwrite on-disk data blocks in addition to writing a delete mark, use database-level or statement-level `SECURE_DELETE`; see [Data Security · Secure Delete](../../11-security-guide/03-data-security.md#secure-delete).

### Parameters

- `db_name`: Optional. Database that contains the table; defaults to the current database.
- `tb_name`: Required. Table to delete from; can be a basic table, subtable, or supertable.
- `condition`: Optional filter. Without a filter, all data in the table is deleted—use with caution. `WHERE` only supports filtering on the first column (the primary timestamp column).
- `SECURE_DELETE`: Optional keyword. When specified, this delete also physically overwrites on-disk data blocks in the matched range. You can also enable secure delete by default for a database with `SECURE_DELETE 1` (see [Databases](../02-ddl/01-database.md)).

### Important Notes

Once data is deleted, it cannot be recovered. Use with caution. To confirm the rows you intend to delete, first run a `SELECT` with the same `WHERE` condition, then run `DELETE`.

## Example

`meters` is a supertable and `groupId` is an `INT` tag column. Delete all data in `meters` with timestamps earlier than `2021-10-01 10:40:00.100`:

```sql
DELETE FROM meters WHERE ts < '2021-10-01 10:40:00.100';
```

After execution, the result is displayed as:

```text
Deleted 102000 row(s) from 1020 table(s) (0.421950s)
```

This indicates that a total of 102000 rows of data were deleted from 1020 subtables.
