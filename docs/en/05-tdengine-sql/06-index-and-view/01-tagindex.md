---
sidebar_label: Tag Indices
title: Tag Indices
description: Create, drop, and view indexes on supertable tag columns
---

This section explains the indexing mechanism of TDengine. Prior to version 3.0.3.0 (exclusive), an index is created by default on the first column tag, but it does not support dynamically adding indexes to other columns. Starting from version 3.0.3.0, indexes can be dynamically added to other tag columns. The index automatically created on the first tag column is enabled by default in queries, and users cannot intervene in any way. Proper use of indexes can effectively improve query performance.

## Create Index

```sql
CREATE INDEX [IF NOT EXISTS] index_name ON tbl_name (tag_col_name);
```

- `index_name`: index name.
- `tbl_name`: supertable name.
- `tag_col_name`: name of the tag column to index; any tag column type is supported.

## Drop Index

```sql
DROP INDEX [IF EXISTS] index_name;
```

`index_name` is the name of an existing index. If the index does not exist and `IF EXISTS` is not used, the command fails but does not affect other objects.

## View Indexes

Query existing indexes via the system table (you can add filter conditions to narrow the scope):

```sql
SELECT * FROM information_schema.ins_indexes;
```

For column descriptions, see [INS_INDEXES](../09-system-info/01-meta.md#ins_indexes).

Or use `SHOW` to view indexes on a specified table:

```sql
SHOW INDEXES FROM tbl_name [FROM db_name];
SHOW INDEXES FROM [db_name.]tbl_name;
```

## Usage Instructions

1. Proper use of indexes can improve the efficiency of data filtering. Currently supported filtering operators include `=`, `>`, `>=`, `<`, `<=`. If these operators are used in the query filtering conditions, the index can significantly improve query efficiency. However, if other operators are used in the query filtering conditions, the index does not work, and there is no change in query efficiency.
2. Only one index can be created for a tag column, and an error will be reported if an index is created repeatedly.
3. Only one index can be created for a tag column at a time; it is not possible to create indexes for multiple tags simultaneously.
4. Regardless of the type of index, its name must be unique throughout the system.
5. There is no limit to the number of indexes, but each additional index will increase the metadata in the system. Too many indexes can reduce the efficiency of metadata access and thus degrade the overall system performance. Therefore, please avoid adding unnecessary indexes.
6. Indexes cannot be created for ordinary and subtables.
7. It is not recommended to create an index for a tag column with few unique values, as the effect in such cases is minimal.
8. For a newly created supertable, an index name is automatically generated for the first column tag, following the rule: the name of that tag column + a 23-byte suffix. This can be viewed in the system table and can be dropped as needed, behaving the same as indexes on other column tags.
