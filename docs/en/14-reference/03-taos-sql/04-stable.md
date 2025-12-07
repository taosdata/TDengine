---
title: Supertables
slug: /tdengine-reference/sql-manual/manage-supertables
---

## Create a Supertable

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
  | VIRTUAL {0 | 1}
}
```

Notes:

- A supertable can have a maximum of 4096 columns, including tag columns.

- A supertable must have at least three columns: one timestamp column (the primary key column), one metric column, and one tag column.

- `COMPOSITE KEY`: You can specify a second column for the primary key by using the `COMPOSITE KEY` keyword. The second primary key column must be of integer or `VARCHAR` type. This column, together with the timestamp column, forms a composite key.

  If a supertable has a composite key, two records in the supertable are considered duplicates only when both the timestamp column and the second primary key column are identical. In such cases, the database keeps only the most recent record; otherwise, both records are retained.

- `TAGS`: The `TAGS` clause defines the tag columns of a supertable. The following restrictions apply:
  - For tag columns of type `TIMESTAMP`, you must provide a literal timestamp value when inserting data. Arithmetic expressions such as `NOW + 10s` are not supported.
  - The names of tag columns must be unique among all columns. You cannot use the same name for a metric column and a tag column.
  - Tag column names cannot use reserved keywords.
  - A supertable can have a maximum of 128 tag columns. At least one tag column is required.
  - The total length of all tag columns cannot exceed 16 KB.

- `ENCODE` and `COMPRESS`: See [Data Compression](./manage-data-compression/).

- `COMMENT` and `SMA`: See [Tables](./manage-tables/).

- `KEEP`: See [Databases](./manage-databases/) for details. However, note the following differences between supertable-level `KEEP` and database-level `KEEP`:
  - The supertable-level `KEEP` value must be smaller than the database-level `KEEP` value.
  - Supertable-level `KEEP` does not take effect immediately. You must flush and then compact the database before data marked as expired by the supertable-level `KEEP` is removed.
  - If you compact the database, then alter the supertable-level `KEEP` value, then compact the database again, expired data might not be completely removed.

- `VIRTUAL`: Specify 1 to create a virtual supertable. The following restrictions apply:
  - `COMPOSITE KEY` is not supported.
  - Compression options (`ENCODE` and `COMPRESS`) are not supported.

## View Supertables

### All Supertables in the Current Database

The following statement displays information about all supertables in the current database:

```sql
SHOW STABLES [LIKE tb_name_wildcard];
```

### Supertable Creation Statement

The following statement displays the SQL statement that was used to create the specified supertable.

```sql
SHOW CREATE STABLE stb_name;
```

This can be helpful when migrating or cloning existing supertables.

### Supertable Schema

The following statement displays the schema of the specified supertable:

```sql
DESCRIBE [db_name.]stb_name;
```

### All Subtable Tags

The following statement displays the tag values of all subtables within a specified supertable:

```sql
SHOW TABLE TAGS FROM stb_name [FROM db_name];
```

or

```sql
SHOW TABLE TAGS FROM [db_name.]stb_name;
```

The subtable name and the value of each tag are shown as follows:

```sql
taos> SHOW TABLE TAGS FROM st1;
             tbname             |     id      |         loc          |
======================================================================
 st1s1                          |           1 | losangeles           |
 st1s2                          |           2 | sanfrancisco         |
 st1s3                          |           3 | sacramento           |
Query OK, 3 rows in database (0.004455s)
```

You can display the values of only specified tags with the following statement:

```sql
taos> SELECT DISTINCT TBNAME, id FROM st1;
             tbname             |     id      |
===============================================
 st1s1                          |           1 |
 st1s2                          |           2 |
 st1s3                          |           3 |
Query OK, 3 rows in database (0.002891s)
```

Note that you must include `DISTINCT` and `TBNAME` in this statement to ensure accurate and fast results in the event that data is missing or the dataset is large.

### Specified Subtable Tags

The following statement displays the tag values of a specified subtable:

```sql
taos> SHOW TAGS FROM st1s1;
   table_name    |     db_name     |   stable_name   |    tag_name     |    tag_type     |    tag_value    |
============================================================================================================
 st1s1           | test            | st1             | id              | INT             | 1               |
 st1s1           | test            | st1             | loc             | VARCHAR(20)     | sanfrancisco    |
Query OK, 2 rows in database (0.003684s)
```

You can display the values of only specified tags with the following statement:

```sql
taos> SELECT DISTINCT TBNAME, id, loc FROM st1s1;
     tbname      |     id      |       loc       |
==================================================
 st1s1           |           1 | sanfrancisco    |
Query OK, 1 rows in database (0.001884s)
```

## Delete a Supertable

The following statement deletes the specified supertable:

```sql
DROP STABLE [IF EXISTS] [db_name.]stb_name;
```

:::important

When you delete a supertable, all subtables created in the supertable along with their data are also deleted. Use this statement with caution.

:::

Note that deleting a supertable does not immediately free all disk space used by the supertable. The supertable is immediately marked for deletion and queries no longer return results from the supertable, but disk space is not freed until the operating system automatically does so or you manually compact the database.

## Modify a Supertable

The following statement modifies the parameters of an existing supertable:

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
  | KEEP value
}
```

Notes:

Modifying the schema of a supertable affects all subtables in the supertable. You cannot alter the schema of any subtable individually. Tag definitions can also only be altered at the supertable level and apply to all subtables in the supertable.

You can perform the following actions:

- `ADD COLUMN`: Add a metric column to the supertable.
- `DROP COLUMN`: Delete a metric column from the supertable. Note that this action deletes the metric column from all subtables within the supertable.
- `MODIFY COLUMN`: Extend the length of a metric column of type `NCHAR` or `BINARY`.
  - You cannot modify metric columns of other types.
  - You cannot modify the length of a column to be shorter.
- `ADD TAG`: Add a tag column to the supertable.
- `DROP TAG`: Delete a tag column from the supertable. Note that this action deletes the tag column from all subtables within the supertable.
- `MODIFY TAG`: Extend the length of a tag column of type `NCHAR` or `BINARY`.
  - You cannot modify tag columns of other types.
  - You cannot modify the length of a column to be shorter.
- `RENAME TAG`: Change the name of a tag column in the supertable. Note that this action affects all subtables within the supertable.

:::important

You cannot add, delete, or modify the primary key column of a supertable.

:::

### Add a Metric

The following statement adds a metric column to the specified supertable:

```sql
ALTER STABLE stb_name ADD COLUMN col_name column_type;
```

### Delete a Metric

The following statement deletes a metric column from the specified supertable:

```sql
ALTER STABLE stb_name DROP COLUMN col_name;
```

### Modify Metric Length

The following statement modifies the length of a specified metric column in the supertable:

```sql
ALTER STABLE stb_name MODIFY COLUMN col_name data_type(length);
```

- The specified column must be of type `NCHAR` or `BINARY`.
- The new length must be greater than the existing length. You cannot make a column shorter.

### Add a Tag

The following statement adds a tag column to the specified supertable:

```sql
ALTER STABLE stb_name ADD TAG tag_name tag_type;
```

Note that the total number of tag columns cannot exceed 128 and the total length cannot exceed 16 KB.

### Delete a Tag

The following statement deletes a tag column from the specified supertable:

```sql
ALTER STABLE stb_name DROP TAG tag_name;
```

### Modify Tag Name

The following statement changes the name of a specified tag column in the supertable:

```sql
ALTER STABLE stb_name RENAME TAG old_tag_name new_tag_name;
```

### Modify Tag Length

The following statement modifies the length of a specified tag column in the supertable:

```sql
ALTER STABLE stb_name MODIFY TAG tag_name data_type(length);
```

- The specified column must be of type `NCHAR` or `BINARY`.
- The new length must be greater than the existing length. You cannot make a column shorter.

### Querying Supertables

You can perform projection and aggregation queries on a supertable using the `SELECT` statement. You can filter on metrics and tags in the `WHERE` clause.

If a supertable query does not include an `ORDER BY` clause, the results returned are grouped by subtable. All records from one subtable are returned first, followed by all records from the next subtable. This means that the data returned is not ordered.

You can include an `ORDER BY` to ensure that the result set strictly follows the specified ordering.

:::note

All tag operations, except for updating tag values, can be performed only on the supertable. You cannot add, delete, or modify tag columns on a specific subtable individually.

When you add a tag column to a supertable, all subtables in that supertable automatically inherit the new tag, and the default value of the new tag is `NULL`.

:::
