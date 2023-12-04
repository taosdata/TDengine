---
title: Table
description: This document describes how to create and perform operations on standard tables and subtables.
---

## Create Table

You create standard tables and subtables with the `CREATE TABLE` statement.

```sql
CREATE TABLE [IF NOT EXISTS] [db_name.]tb_name (create_definition [, create_definition] ...) [table_options]

CREATE TABLE create_subtable_clause

CREATE TABLE [IF NOT EXISTS] [db_name.]tb_name (create_definition [, create_definition] ...)
    [TAGS (create_definition [, create_definition] ...)]
    [table_options]

create_subtable_clause: {
    create_subtable_clause [create_subtable_clause] ...
  | [IF NOT EXISTS] [db_name.]tb_name USING [db_name.]stb_name [(tag_name [, tag_name] ...)] TAGS (tag_value [, tag_value] ...)
}

create_definition:
    col_name column_definition

column_definition:
    type_name [comment 'string_value']

table_options:
    table_option ...

table_option: {
    COMMENT 'string_value'
  | SMA(col_name [, col_name] ...)
  | TTL value
}

```

**More explanations**

1. The first column of a table MUST be of type TIMESTAMP. It is automatically set as the primary key.
2. The maximum length of the table name is 192 bytes.
3. The maximum length of each row is 48k(64k since version 3.0.5.0) bytes, please note that the extra 2 bytes used by each BINARY/NCHAR/GEOMETRY column are also counted.
4. The name of the subtable can only consist of characters from the English alphabet, digits and underscore. Table names can't start with a digit. Table names are case insensitive.
5. The maximum length in bytes must be specified when using BINARY/NCHAR/GEOMETRY types.
6. Escape character "\`" can be used to avoid the conflict between table names and reserved keywords, above rules will be bypassed when using escape character on table names, but the upper limit for the name length is still valid. The table names specified using escape character are case sensitive.
   For example \`aBc\` and \`abc\` are different table names but `abc` and `aBc` are same table names because they are both converted to `abc` internally.
   Only ASCII visible characters can be used with escape character.

**Parameter description**
1. COMMENT: specifies comments for the table. This parameter can be used with supertables, standard tables, and subtables.
2. SMA: specifies functions on which to enable small materialized aggregates (SMA). SMA is user-defined precomputation of aggregates based on data blocks. Enter one of the following values: max, min, or sum This parameter can be used with supertables and standard tables.
3. TTL: specifies the time to live (TTL) for the table. If TTL is specified when creatinga table, after the time period for which the table has been existing is over TTL, TDengine will automatically delete the table. Please be noted that the system may not delete the table at the exact moment that the TTL expires but guarantee there is such a system and finally the table will be deleted. The unit of TTL is in days. The default value is 0, i.e. never expire.

## Create Subtables

### Create a Subtable

```sql
CREATE TABLE [IF NOT EXISTS] tb_name USING stb_name TAGS (tag_value1, ...);
```

### Create a Subtable with Specified Tags

```sql
CREATE TABLE [IF NOT EXISTS] tb_name USING stb_name (tag_name1, ...) TAGS (tag_value1, ...);
```

The preceding SQL statement creates a subtable based on a supertable but specifies a subset of tags to use. Tags that are not included in this subset are assigned a null value.

### Create Multiple Subtables

```sql
CREATE TABLE [IF NOT EXISTS] tb_name1 USING stb_name TAGS (tag_value1, ...) [IF NOT EXISTS] tb_name2 USING stb_name TAGS (tag_value2, ...) ...;
```

You can create multiple subtables in a single SQL statement provided that all subtables use the same supertable. For performance reasons, do not create more than 3000 tables per statement.

## Modify a Table

```sql
ALTER TABLE [db_name.]tb_name alter_table_clause

alter_table_clause: {
    alter_table_options
  | ADD COLUMN col_name column_type
  | DROP COLUMN col_name
  | MODIFY COLUMN col_name column_type
  | RENAME COLUMN old_col_name new_col_name
}

alter_table_options:
    alter_table_option ...

alter_table_option: {
    TTL value
  | COMMENT 'string_value'
}

```

**More explanations**
You can perform the following modifications on existing tables:
1. ADD COLUMN: adds a column to the supertable.
2. DROP COLUMN: deletes a column from the supertable.
3. MODIFY COLUMN: changes the length of the data type specified for the column. Note that you can only specify a length greater than the current length.
4. RENAME COLUMN: renames a specified column in the table.

### Add a Column

```sql
ALTER TABLE tb_name ADD COLUMN field_name data_type;
```

### Delete a Column

```sql
ALTER TABLE tb_name DROP COLUMN field_name;
```

### Modify the Data Length

```sql
ALTER TABLE tb_name MODIFY COLUMN field_name data_type(length);
```

### Rename a Column

```sql
ALTER TABLE tb_name RENAME COLUMN old_col_name new_col_name
```

## Modify a Subtable

```sql
ALTER TABLE [db_name.]tb_name alter_table_clause

alter_table_clause: {
    alter_table_options
  | SET TAG tag_name = new_tag_value
}

alter_table_options:
    alter_table_option ...

alter_table_option: {
    TTL value
  | COMMENT 'string_value'
}
```

**More explanations**
1. Only the value of a tag can be modified directly. For all other modifications, you must modify the supertable from which the subtable was created.

### Change Tag Value Of Sub Table

```
ALTER TABLE tb_name SET TAG tag_name=new_tag_value;
```

## Delete a Table

The following SQL statement deletes one or more tables.

```sql
DROP TABLE [IF EXISTS] [db_name.]tb_name [, [IF EXISTS] [db_name.]tb_name] ...
```

## View Tables

### View All Tables

The following SQL statement shows all tables in the current database.

```sql
SHOW TABLES [LIKE tb_name_wildchar];
```

### View the CREATE Statement for a Table

```
SHOW CREATE TABLE tb_name;
```

This command is useful in migrating data from one TDengine cluster to another because it can be used to create the exact same tables in the target database.

## View the Table Schema

```
DESCRIBE [db_name.]tb_name;
```
