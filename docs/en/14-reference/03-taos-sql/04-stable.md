---
title: Manage Supertables
description: Various management operations on supertables
slug: /tdengine-reference/sql-manual/manage-supertables
---

## Create Supertable

```sql
CREATE STABLE [IF NOT EXISTS] stb_name (create_definition [, create_definition] ...) TAGS (create_definition [, create_definition] ...) [table_options]
 
create_definition:
    col_name column_definition
 
column_definition:
    type_name [PRIMARY KEY] [ENCODE 'encode_type'] [COMPRESS 'compress_type'] [LEVEL 'level_type']

table_options:
    table_option ...

table_option: {
    COMMENT 'string_value'
  | SMA(col_name [, col_name] ...)  
}
```

**Usage Instructions**

1. The maximum number of columns in a supertable is 4096, including tag columns; the minimum number is 3, which includes one timestamp primary key, one tag column, and one data column.
2. In addition to the timestamp primary key column, a second column can also be designated as an additional primary key using the PRIMARY KEY keyword. The second column designated as the primary key must be of integer or string type (varchar).
3. The `TAGS` syntax specifies the tag columns for the supertable, which must adhere to the following conventions:
    - The TIMESTAMP data type in a tag columnrequires a provided value when writing data and does not support arithmetic operations, such as NOW + 10s.
    - Tag column names cannot be the same as other column names.
    - Tag column names cannot be reserved keywords.
    - Tag allows a maximum of 128 tags, at least 1, with a total length not exceeding 16 KB.
4. For the usage of `ENCODE` and `COMPRESS`, please refer to [Column Compression](../manage-data-compression/).
5. For parameter descriptions in table_options, please refer to [Table Creation SQL Description](../manage-tables/).

## View Supertable

### Show All Supertables in the Current Database

```sql
SHOW STABLES [LIKE tb_name_wildcard];
```

View all supertables within the database.

### Show Creation Statement of a Supertable

```sql
SHOW CREATE STABLE stb_name;
```

Commonly used for database migration. For an existing supertable, it returns its creation statement; executing this statement in another cluster will create a supertable with the same structure.

### Get Structure Information of a Supertable

```sql
DESCRIBE [db_name.]stb_name;
```

### Get Tag Information of All Subtables in a Supertable

```sql
SHOW TABLE TAGS FROM table_name [FROM db_name];
SHOW TABLE TAGS FROM [db_name.]table_name;
```

```text
taos> SHOW TABLE TAGS FROM st1;
             tbname             |     id      |         loc          |
======================================================================
 st1s1                          |           1 | beijing              |
 st1s2                          |           2 | shanghai             |
 st1s3                          |           3 | guangzhou            |
Query OK, 3 rows in database (0.004455s)
```

The first column of the returned result set is the subtable name, and the subsequent columns are the tag columns.

If you already know the tag column names, you can use the following statement to obtain the values of the specified tag columns.

```text
taos> SELECT DISTINCT TBNAME, id FROM st1;
             tbname             |     id      |
===============================================
 st1s1                          |           1 |
 st1s2                          |           2 |
 st1s3                          |           3 |
Query OK, 3 rows in database (0.002891s)
```

Note that the DISTINCT in the SELECT statement and TBNAME are both essential; TDengine will optimize the statement based on these to ensure that tag values are returned correctly and quickly, regardless of whether there is no data or the data volume is very large.

### Get Tag Information of a Specific Subtable

```text
taos> SHOW TAGS FROM st1s1;
   table_name    |     db_name     |   stable_name   |    tag_name     |    tag_type     |    tag_value    |
============================================================================================================
 st1s1           | test            | st1             | id              | INT             | 1               |
 st1s1           | test            | st1             | loc             | VARCHAR(20)     | beijing         |
Query OK, 2 rows in database (0.003684s)
```

Similarly, you can also use a SELECT statement to query the values of specified tag columns.

```text
taos> SELECT DISTINCT TBNAME, id, loc FROM st1s1;
     tbname      |     id      |       loc       |
==================================================
 st1s1           |           1 | beijing         |
Query OK, 1 rows in database (0.001884s)
```

## Drop Supertable

```sql
DROP STABLE [IF EXISTS] [db_name.]stb_name
```

Dropping an STABLE will automatically delete all subtables created through the STABLE and all data within those subtables.

:::note

Dropping a supertable does not immediately release the disk space occupied by that table; instead, it marks the data of that table as deleted, and these data will not appear in queries. However, the release of disk space will be delayed until the system automatically or manually performs data compaction.

:::

## Modify Supertable

```sql
ALTER STABLE [db_name.]tb_name alter_table_clause
 
alter_table_clause: {
    alter_table_options
  | ADD COLUMN col_name column_type
  | DROP COLUMN col_name
  | MODIFY COLUMN col_name column_type
  | ADD TAG tag_name tag_type
  | DROP TAG tag_name
  | MODIFY TAG tag_name tag_type
  | RENAME TAG old_tag_name new_tag_name
}
 
alter_table_options:
    alter_table_option ...
 
alter_table_option: {
    COMMENT 'string_value'
}
```

**Usage Instructions**

Modifying the structure of a supertable will take effect on all its subtables. It is not possible to modify the table structure for a specific subtable. Changes to tag structures must be issued on the supertable, and TDengine will automatically apply these changes to all subtables of that supertable.

- ADD COLUMN: Add a column.
- DROP COLUMN: Remove a column.
- MODIFY COLUMN: Change the width of a column; the data column type must be nchar or binary, and this instruction can be used to increase its width only, not decrease it.
- ADD TAG: Add a tag to the supertable.
- DROP TAG: Remove a tag from the supertable. When a tag is removed from the supertable, all subtables of that supertable will automatically delete that tag as well.
- MODIFY TAG: Change the width of a tag in the supertable. The tag type can only be nchar or binary, and this instruction can be used to increase its width only, not decrease it.
- RENAME TAG: Change the name of a tag in the supertable. When a tag name is changed in the supertable, all subtables of that supertable will automatically update that tag name as well.
- Similar to basic tables, the primary key column of a supertable cannot be modified, nor can primary key columns be added or deleted using ADD/DROP COLUMN.

### Add Column

```sql
ALTER STABLE stb_name ADD COLUMN col_name column_type;
```

### Drop Column

```sql
ALTER STABLE stb_name DROP COLUMN col_name;
```

### Modify Column Width

```sql
ALTER STABLE stb_name MODIFY COLUMN col_name data_type(length);
```

If the data column type is variable-length (BINARY or NCHAR), this instruction can be used to modify its width (only to increase, not decrease).

### Add Tag

```sql
ALTER STABLE stb_name ADD TAG tag_name tag_type;
```

Add a new tag to the supertable and specify the type of the new tag. The total number of tags cannot exceed 128, with a total length not exceeding 16KB.

### Drop Tag

```sql
ALTER STABLE stb_name DROP TAG tag_name;
```

Remove a tag from the supertable. When a tag is removed from the supertable, all subtables of that supertable will automatically delete that tag.

### Rename Tag

```sql
ALTER STABLE stb_name RENAME TAG old_tag_name new_tag_name;
```

Change the name of a tag in the supertable. When a tag name is changed in the supertable, all subtables of that supertable will automatically update that tag name as well.

### Modify Tag Width

```sql
ALTER STABLE stb_name MODIFY TAG tag_name data_type(length);
```

If the tag type is variable-length (BINARY or NCHAR), this instruction can be used to modify its width (only to increase, not decrease). (Added in version 2.1.3.0)

### Supertable Query

You can use the SELECT statement to perform projection and aggregation queries on supertables. In the WHERE clause, you can filter and select tags and columns.

If no ORDER BY clause is added in the supertable query, the returned order is to first return all data of one subtable, and then return all data of the next subtable, so the returned data is unordered. If an ORDER BY clause is added, the data will be returned strictly in the order specified by the ORDER BY clause.

:::note

Except for updating the tag values, which operate on subtables, all other tag operations (adding tags, dropping tags, etc.) can only act on the STABLE and cannot operate on individual subtables. After adding a tag to the STABLE, all tables established based on that STABLE will automatically have the new tag, and all newly added tags will have their default values set to NULL.

:::
