---
title: Supertables
slug: /tdengine-reference/sql-manual/manage-supertables
---

## Create Supertable

```sql
CREATE STABLE [IF NOT EXISTS] stb_name (create_definition [, create_definition] ...) TAGS (create_definition [, create_definition] ...) [table_options]
 
create_definition:
    col_name column_definition
 
column_definition:
    type_name [COMPOSITE KEY] [ENCODE 'encode_type'] [COMPRESS 'compress_type'] [LEVEL 'level_type']

table_options:
    table_option ...

table_option: {
    COMMENT 'string_value'
  | SMA(col_name [, col_name] ...)  
  | KEEP value
}
```

Instructions:

1. The maximum number of columns in a supertable is 4096, including tag columns, with a minimum of 3 columns: a timestamp primary key, one tag column, and one data column.
2. Besides the timestamp primary key column, a second column can be designated as an additional composite primary key using the COMPOSITE KEY keyword. This second composite primary key column must be of integer or string type (varchar).
3. TAGS syntax specifies the label columns of the supertable, which must adhere to the following conventions:
    - The TIMESTAMP column in TAGS requires a given value when writing data and does not support arithmetic operations, such as expressions like NOW + 10s.
    - TAGS column names cannot be the same as other column names.
    - TAGS column names cannot be reserved keywords.
    - TAGS can have up to 128 columns, at least 1, with a total length not exceeding 16 KB.
4. For the use of `ENCODE` and `COMPRESS`, please refer to [Column Compression](../manage-data-compression/)
5. For explanations of parameters in table_option, please refer to [Table SQL Description](../manage-tables/)
6. Regarding the keep parameter in table_option, it only takes effect for supertables. For detailed information on the keep parameter, refer to Database Description. However, the keep parameter for supertables differs from the keep parameter for databases in the following ways:
    - The supertable's keep parameter does not immediately affect query results. Data is only cleaned up and becomes invisible to queries after compaction is complete.
    - The supertable's keep parameter must be less than the database's keep parameter.
    - A flush operation must be performed before compaction; otherwise, the keep setting may not take effect.
    - After compaction, if you use ALTER STABLE to change the keep value and then compact again, some data may not be cleaned up correctly. This depends on whether new data has been written to the corresponding files since the last compaction.

## View Supertables

### Show all supertable information in the current database

```sql
SHOW STABLES [LIKE tb_name_wildcard];
```

View all supertables in the database.

### Show the creation statement of a supertable

```sql
SHOW CREATE STABLE stb_name;
```

Commonly used for database migration. For an existing supertable, it returns its creation statement; executing this statement in another cluster will create a supertable with the exact same structure.

### Get the structure information of a supertable

```sql
DESCRIBE [db_name.]stb_name;
```

### Get the tag information of all subtables in a supertable

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

The first column of the result set is the subtable name, followed by columns for tags.

If you already know the name of the tag column, you can use the following statement to get the value of a specific tag column.

```text
taos> SELECT DISTINCT TBNAME, id FROM st1;
             tbname             |     id      |
===============================================
 st1s1                          |           1 |
 st1s2                          |           2 |
 st1s3                          |           3 |
Query OK, 3 rows in database (0.002891s)
```

It should be noted that both DISTINCT and TBNAME in the SELECT statement are essential. TDengine optimizes the statement based on them, allowing it to return tag values correctly and quickly, whether there is no data or an abundance of data.

### Retrieve tag information for a specific subtable

```text
taos> SHOW TAGS FROM st1s1;
   table_name    |     db_name     |   stable_name   |    tag_name     |    tag_type     |    tag_value    |
============================================================================================================
 st1s1           | test            | st1             | id              | INT             | 1               |
 st1s1           | test            | st1             | loc             | VARCHAR(20)     | beijing         |
Query OK, 2 rows in database (0.003684s)
```

Similarly, you can also use the SELECT statement to query the values of specified tag columns.

```text
taos> SELECT DISTINCT TBNAME, id, loc FROM st1s1;
     tbname      |     id      |       loc       |
==================================================
 st1s1           |           1 | beijing         |
Query OK, 1 rows in database (0.001884s)
```

## Delete Supertable

```sql
DROP STABLE [IF EXISTS] [db_name.]stb_name
```

Deleting an STable automatically removes the subtables created through the STable and all data within those subtables.

**Note**: Deleting a supertable does not immediately free up the disk space it occupies. Instead, the data of the table is marked as deleted. These data will not appear in queries, but the release of disk space will be delayed until the system automatically or the user manually reorganizes the data.

## Modify Supertable

```sql
ALTER STABLE [db_name.]tb_name alter_table_clause
 
alter_table_clause: {
    alter_table_options
  | ADD COLUMN col_name column_type
  | DROP COLUMN col_name
  | MODIFY COLUMN col_name column_type
  | ADD tag tag_name tag_type
  | DROP tag tag_name
  | MODIFY tag tag_name tag_type
  | RENAME tag old_tag_name new_tag_name
}
 
alter_table_options:
    alter_table_option ...
 
alter_table_option: {
    COMMENT 'string_value'
  | KEEP value
}

```

Usage Instructions:

Modifying the structure of a supertable affects all its subtables. It is not possible to modify the table structure for a specific subtable. Modifications to the tag structure need to be issued to the supertable, and TDengine will automatically apply them to all subtables of this supertable.

- ADD COLUMN: Add a column.
- DROP COLUMN: Delete a column.
- MODIFY COLUMN: Modify the width of a column. The data column types must be nchar and binary, and this command can be used to increase their width, but not decrease.
- ADD tag: Add a tag to the supertable.
- DROP tag: Remove a tag from the supertable. After a tag is removed from a supertable, it is automatically deleted from all its subtables.
- MODIFY tag: Modify the width of a tag in the supertable. The tag types can only be nchar and binary, and this command can be used to increase their width, but not decrease.
- RENAME tag: Change the name of a tag in the supertable. After a tag name is changed in a supertable, all its subtables automatically update to the new tag name.
- Like basic tables, the primary key columns of a supertable cannot be modified, nor can they be added or removed through ADD/DROP COLUMN.

### Add Column

```sql
ALTER STABLE stb_name ADD COLUMN col_name column_type;
```

### Delete Column

```sql
ALTER STABLE stb_name DROP COLUMN col_name;
```

### Modify Column Width

```sql
ALTER STABLE stb_name MODIFY COLUMN col_name data_type(length);
```

If the data column type is variable length (BINARY or NCHAR), this command can be used to modify its width (can only increase, not decrease).

### Add Tag

```sql
ALTER STABLE stb_name ADD tag tag_name tag_type;
```

Add a new tag to an STable and specify the type of the new tag. The total number of tags cannot exceed 128, and the total length cannot exceed 16KB.

### Delete Tag

```sql
ALTER STABLE stb_name DROP tag tag_name;
```

Delete a tag from a supertable; after a tag is deleted from a supertable, all subtables under that supertable will automatically delete that tag.

### Rename Tag

```sql
ALTER STABLE stb_name RENAME tag old_tag_name new_tag_name;
```

Change the name of a tag in a supertable; after a tag name is changed in a supertable, all subtables under that supertable will automatically update that tag name.

### Modify Tag Column Width

```sql
ALTER STABLE stb_name MODIFY tag tag_name data_type(length);
```

If the tag type is variable length (BINARY or NCHAR), this command can be used to modify its width (can only increase, not decrease). (Added in version 2.1.3.0)

### Supertable Query

Using the SELECT statement, you can perform projection and aggregation queries on a supertable. In the WHERE clause, you can filter and select based on tags and columns.

If the supertable query statement does not include ORDER BY, the return order is to return all data from one subtable first, then all data from the next subtable, so the returned data is unordered. If an ORDER BY clause is added, the data will be returned strictly in the order specified by the ORDER BY clause.

:::note
Except for updating the value of tags, which is done on subtables, all other tag operations (adding tags, deleting tags, etc.) can only be applied to STables and not to individual subtables. After adding tags to an STable, all tables built on that STable will automatically have a new tag added, and the default value for all new tags is NULL.

:::
