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

**Note:** Deleting data does not immediately free disk space; rows are marked as deleted. They no longer appear in queries, but space is reclaimed later when the database [`KEEP`](../02-ddl/01-database.md) setting takes effect, or when you manually run [Data Reorganization](../../12-operations-and-tooling/02-operations/04-maintenance.md#data-reorganization) (enterprise `COMPACT`). To physically overwrite on-disk data blocks in addition to writing a delete mark, use database-level or statement-level `SECURE_DELETE`; see [Data Security · Secure Delete](../../11-security-guide/06-data-security.md#secure-delete).

### Parameters

- `db_name`: Optional. Database that contains the table; defaults to the current database.
- `tb_name`: Required. Table to delete from; can be a basic table, subtable, or supertable.
- `condition`: Optional filter. Without a filter, all data in the table is deleted—use with caution. `WHERE` accepts three kinds of clauses, in any combination and any of them may be omitted, but they can only be joined with `AND`:
  - **Primary timestamp condition**: a filter on the first column, which must reduce to one definite closed time window (for example `ts > '2021-10-01 10:00:00' AND ts < '2021-10-01 11:00:00'`).
  - **Tag condition**: selects which subtables of a supertable the delete applies to; it does not decide which rows are removed.
  - **Ordinary column condition**: a filter on the value of a non-primary-key, non-tag column, used to pin down the exact rows to remove within the selected subtables and time window. Only simple comparison predicates between a single column and a constant are supported (`=`, `!=`, `>`, `>=`, `<`, `<=`, `BETWEEN`, `IN`, `IS [NOT] NULL`, plus `LIKE` and `MATCH` for string columns); function expressions, cross-column comparisons and subqueries are not. Several ordinary column conditions may be joined with `OR`.
- `SECURE_DELETE`: Optional keyword. When specified, this delete also physically overwrites on-disk data blocks in the matched range. You can also enable secure delete by default for a database with `SECURE_DELETE 1` (see [Databases](../02-ddl/01-database.md)).

### Important Notes

Once data is deleted, it cannot be recovered. Use with caution. To confirm the rows you intend to delete, first run a `SELECT` with the same `WHERE` condition, then run `DELETE`.

Additional notes for ordinary column filters:

- The candidate rows in the time window are scanned and evaluated one by one, so the statement costs about as much as an equivalent `SELECT`. Without a time range it amounts to a full table scan, so add one whenever possible to narrow the impact.
- Tables with a composite primary key do not support ordinary column filters. A composite primary key allows several rows to share one timestamp, but a delete mark can only address a time range and cannot tell those rows apart, so this case is rejected rather than deleting more than intended.
- An ordinary column filter cannot be combined with `SECURE_DELETE`.
- If the matched rows are very scattered in time, a single statement may exceed the limit on generated delete marks and fail. Narrow the time range or the filter and delete in batches.

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

Delete the readings in `meters` whose voltage is abnormal (above 250V):

```sql
DELETE FROM meters WHERE voltage > 250;
```

Combine a time range with a tag condition to clean up only the negative-current readings recorded by the devices in `groupId` 2 during a given period:

```sql
DELETE FROM meters
WHERE groupId = 2
  AND ts >= '2021-10-01 10:00:00'
  AND ts <  '2021-10-01 11:00:00'
  AND current < 0;
```
