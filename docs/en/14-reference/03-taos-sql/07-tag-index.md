---
title: Manage Tag Indices
description: Using tag indexes to improve query performance
slug: /tdengine-reference/sql-manual/manage-tag-indices
---

This section explains the indexing mechanism in TDengine. Prior to version 3.0.3.0 (exclusive), an index was created by default on the first tag column, but adding indexes dynamically to other columns was not supported. Starting from version 3.0.3.0, indexes can be dynamically added to other tag columns. The automatically created index on the first tag column is enabled by default in queries, and users cannot intervene in it. Proper use of indexes can effectively enhance query performance.

## Syntax

The syntax for creating an index is as follows:

```sql
CREATE INDEX index_name ON tbl_name (tagColName)
```

Where `index_name` is the name of the index, `tbl_name` is the name of the supertable, and `tagColName` is the name of the tag column on which the index will be created. There are no restrictions on the type of `tagColName`; any type of tag column can have an index created on it.

The syntax for dropping an index is as follows:

```sql
DROP INDEX index_name
```

Where `index_name` is the name of an already established index; if the index does not exist, the command will fail but will not have any other effect on the system.

To view the indexes that already exist in the system:

```sql
SELECT * FROM information_schema.INS_INDEXES
```

You can also add filter conditions to narrow the query scope for the above query.

Alternatively, you can use the SHOW command to view indexes on a specified table:

```sql
SHOW INDEXES FROM tbl_name [FROM db_name];
SHOW INDEXES FROM [db_name.]tbl_name;
```

## Usage Instructions

1. Proper use of indexes can improve data filtering efficiency. Currently supported filtering operators include `=`, `>`, `>=`, `<`, `<=`. If these operators are used in the query filter conditions, indexes can significantly enhance query efficiency. However, if other operators are used, the indexes will have no effect, and the query efficiency will remain unchanged. More operators will be gradually added in the future.

2. An index can only be created for one tag column; if an attempt is made to create a duplicate index, an error will occur.

3. Indexes can only be created one at a time for a single tag column; multiple tag columns cannot have indexes created simultaneously.

4. The names of all types of indexes in the system must be unique.

5. There is no limit on the number of indexes; however, each additional index increases the metadata in the system, and too many indexes can reduce the efficiency of metadata access, thereby degrading overall system performance. Therefore, it is advisable to avoid adding unnecessary indexes.

6. Indexes cannot be created for basic tables and subtables.

7. If a tag column has a low number of unique values, it is not recommended to create an index on it, as the benefits will be minimal.

8. A new supertable will generate a random index name for the first tag column, with the naming rule being: the name of tag0 + 23 bytes. This can be checked in the system table, and it can also be dropped as needed, functioning the same as the indexes of other tag columns.
