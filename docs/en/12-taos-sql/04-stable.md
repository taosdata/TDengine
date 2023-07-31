---
title: Supertable
sidebar_label: Supertable
description: This document describes how to create and perform operations on supertables.
---

## Create a Supertable

```sql
CREATE STABLE [IF NOT EXISTS] stb_name (create_definition [, create_definition] ...) TAGS (create_definition [, create_definition] ...) [table_options]
 
create_definition:
    col_name column_definition
 
column_definition:
    type_name
```

**More explanations**
- Each supertable can have a maximum of 4096 columns, including tags. The minimum number of columns is 3: a timestamp column used as the key, one tag column, and one data column.
- The TAGS keyword defines the tag columns for the supertable. The following restrictions apply to tag columns:
    - A tag column can use the TIMESTAMP data type, but the values in the column must be fixed numbers. Timestamps including formulae, such as "now + 10s", cannot be stored in a tag column.
    - The name of a tag column cannot be the same as the name of any other column.
    - The name of a tag column cannot be a reserved keyword.
    - Each supertable must contain between 1 and 128 tags. The total length of the TAGS keyword cannot exceed 16 KB.
- For more information about table parameters, see Create a Table.

## View a Supertable

### View All Supertables

```
SHOW STABLES [LIKE tb_name_wildcard];
```

The preceding SQL statement shows all supertables in the current TDengine database.

### View the CREATE Statement for a Supertable

```
SHOW CREATE STABLE stb_name;
```

The preceding SQL statement can be used in migration scenarios. It returns the CREATE statement that was used to create the specified supertable. You can then use the returned statement to create an identical supertable on another TDengine database.

## View the Supertable Schema

```
DESCRIBE [db_name.]stb_name;
```

### View tag information for all child tables in the supertable

```
SHOW TABLE TAGS FROM table_name [FROM db_name];
SHOW TABLE TAGS FROM [db_name.]table_name;
```

```
taos> SHOW TABLE TAGS FROM st1;
             tbname             |     id      |         loc          |
======================================================================
 st1s1                          |           1 | beijing              |
 st1s2                          |           2 | shanghai             |
 st1s3                          |           3 | guangzhou            |
Query OK, 3 rows in database (0.004455s)
```

The first column of the returned result set is the subtable name, and the subsequent columns are the tag columns.

If you already know the name of the tag column, you can use the following statement to get the value of the specified tag column.

```
taos> SELECT DISTINCT TBNAME, id FROM st1;
             tbname             |     id      |
===============================================
 st1s1                          |           1 |
 st1s2                          |           2 |
 st1s3                          |           3 |
Query OK, 3 rows in database (0.002891s)
```

It should be noted that DISTINCT and TBNAME in the SELECT statement are essential, and TDengine will optimize the statement according to them, so that it can return the tag value correctly and quickly even when there is no data or a lot of data.

### View the tag information of a subtable

```
taos> SHOW TAGS FROM st1s1;
   table_name    |     db_name     |   stable_name   |    tag_name     |    tag_type     |    tag_value    |
============================================================================================================
 st1s1           | test            | st1             | id              | INT             | 1               |
 st1s1           | test            | st1             | loc             | VARCHAR(20)     | beijing         |
Query OK, 2 rows in database (0.003684s)
```

Similarly, you can also use the SELECT statement to query the value of the specified tag column.

```
taos> SELECT DISTINCT TBNAME, id, loc FROM st1s1;
     tbname      |     id      |       loc       |
==================================================
 st1s1           |           1 | beijing         |
Query OK, 1 rows in database (0.001884s)
```

## Drop STable

```
DROP STABLE [IF EXISTS] [db_name.]stb_name
```

Note: Deleting a supertable will delete all subtables created from the supertable, including all data within those subtables.

## Modify a Supertable

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

**More explanations**

Modifications to the table schema of a supertable take effect on all subtables within the supertable. You cannot modify the table schema of subtables individually. When you modify the tag schema of a supertable, the modifications automatically take effect on all of its subtables.

- ADD COLUMN: adds a column to the supertable.
- DROP COLUMN: deletes a column from the supertable.
- MODIFY COLUMN: changes the length of a BINARY or NCHAR column. Note that you can only specify a length greater than the current length.
- ADD TAG: adds a tag to the supertable.
- DROP TAG: deletes a tag from the supertable. When you delete a tag from a supertable, it is automatically deleted from all subtables within the supertable.
- MODIFY TAG: modifies the definition of a tag in the supertable. You can use this keyword to change the length of a BINARY or NCHAR tag column. Note that you can only specify a length greater than the current length.
- RENAME TAG: renames a specified tag in the supertable. When you rename a tag in a supertable, it is automatically renamed in all subtables within the supertable.

### Add a Column

```
ALTER STABLE stb_name ADD COLUMN col_name column_type;
```

### Delete a Column

```
ALTER STABLE stb_name DROP COLUMN col_name;
```

### Modify the Data Length

```
ALTER STABLE stb_name MODIFY COLUMN col_name data_type(length);
```

The preceding SQL statement changes the length of a BINARY or NCHAR data column. Note that you can only specify a length greater than the current length.

### Add A Tag

```
ALTER STABLE stb_name ADD TAG tag_name tag_type;
```

The preceding SQL statement adds a tag of the specified type to the supertable. A supertable cannot contain more than 128 tags. The total length of all tags in a supertable cannot exceed 16 KB.

### Remove A Tag

```
ALTER STABLE stb_name DROP TAG tag_name;
```

The preceding SQL statement deletes a tag from the supertable. When you delete a tag from a supertable, it is automatically deleted from all subtables within the supertable.

### Change A Tag

```
ALTER STABLE stb_name RENAME TAG old_tag_name new_tag_name;
```

The preceding SQL statement renames a tag in the supertable. When you rename a tag in a supertable, it is automatically renamed in all subtables within the supertable.

### Change Tag Length

```
ALTER STABLE stb_name MODIFY TAG tag_name data_type(length);
```

The preceding SQL statement changes the length of a BINARY or NCHAR tag column. Note that you can only specify a length greater than the current length. (Available in 2.1.3.0 and later versions)

### View a Supertable
You can run projection and aggregate SELECT queries on supertables, and you can filter by tag or column by using the WHERE keyword.

If you do not include an ORDER BY clause, results are returned by subtable. These results are not ordered. You can include an ORDER BY clause in your query to strictly order the results.



:::note
All tag operations except for updating the value of a tag must be performed on the supertable and not on individual subtables. If you add a tag to an existing supertable, the tag is automatically added with a null value to all subtables within the supertable.

:::
