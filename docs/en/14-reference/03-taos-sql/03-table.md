---
title: Manage Tables
description: Various management operations on tables
slug: /tdengine-reference/sql-manual/manage-tables
---

## Create Table

The `CREATE TABLE` statement is used to create basic tables and subtables based on supertables as templates.

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
    type_name [PRIMARY KEY] [ENCODE 'encode_type'] [COMPRESS 'compress_type'] [LEVEL 'level_type']

table_options:
    table_option ...

table_option: {
    COMMENT 'string_value'
  | SMA(col_name [, col_name] ...)
  | TTL value
}
```

**Usage Instructions**

1. The naming rules for table (column) names refer to [Naming Rules](../names/).
2. The maximum length of a table name is 192.
3. The first field of a table must be TIMESTAMP, which is automatically set as the primary key by the system.
4. In addition to the timestamp primary key column, a second column can also be designated as an additional primary key using the PRIMARY KEY keyword. The second column designated as the primary key must be of integer or string type (varchar).
5. The length of each row in a table cannot exceed 48KB (64KB starting from version 3.0.5.0); (Note: Each BINARY/NCHAR/GEOMETRY type column will also occupy an additional 2 bytes of storage space).
6. For the data types BINARY/NCHAR/GEOMETRY, the maximum byte size must be specified, such as BINARY(20), indicating 20 bytes.
7. For the usage of `ENCODE` and `COMPRESS`, please refer to [Column Compression](../manage-data-compression/).

**Parameter Description**

1. COMMENT: Table comment. Applicable to supertables, subtables, and basic tables. The maximum length is 1024 bytes.
2. SMA: Small Materialized Aggregates, provides a custom pre-computation feature based on data blocks. The pre-computation types include MAX, MIN, and SUM. Applicable to supertables/basic tables.
3. TTL: Time to Live, is a parameter used to specify the lifecycle of the table. If this parameter is specified when creating the table, TDengine will automatically delete the table after its existence exceeds the specified TTL. This TTL time is approximate, and the system does not guarantee deletion at the specified time, but it ensures that such a mechanism exists and that it will eventually be deleted. TTL is measured in days, with a range of [0, 2147483647], defaulting to 0, which means no limit; the expiration time is the table creation time plus the TTL time. TTL is not related to the KEEP parameter of the database; if KEEP is smaller than TTL, the data may be deleted before the table is deleted.

## Create Subtable

### Create Subtable

```sql
CREATE TABLE [IF NOT EXISTS] tb_name USING stb_name TAGS (tag_value1, ...);
```

### Create Subtable and Specify Tag Values

```sql
CREATE TABLE [IF NOT EXISTS] tb_name USING stb_name (tag_name1, ...) TAGS (tag_value1, ...);
```

This creates a data table using the specified supertable as a template and can also specify values for some tag columns (unspecified tag columns will be set to NULL).

### Batch Create Subtables

```sql
CREATE TABLE [IF NOT EXISTS] tb_name1 USING stb_name TAGS (tag_value1, ...) [IF NOT EXISTS] tb_name2 USING stb_name TAGS (tag_value2, ...) ...;
```

The batch table creation method requires that the data tables be based on a supertable as a template. Within the limits of SQL statement length, it is recommended to control the number of tables in a single statement to be between 1000 and 3000 to achieve optimal table creation speed.

### Use CSV to Batch Create Subtables

```sql
CREATE TABLE [IF NOT EXISTS] USING [db_name.]stb_name (field1_name [, field2_name] ....) FILE csv_file_path;
```

**Parameter Description**

1. The FILE syntax indicates that the data comes from a CSV file (comma-separated, with each value enclosed in single quotes). The CSV file does not require a header. The CSV file should contain only the table name and tag values. For inserting data, please refer to the data writing chapter.
2. The specified stb_name must already exist for creating the subtable.
3. The order of the field_name list must match the order of the columns in the CSV file. The list cannot contain duplicates and must include `tbname`, which can include zero or more of the already defined tag columns in the supertable. Tag values not included in the list will be set to NULL.

## Modify Basic Table

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

**Usage Instructions**
The following modifications can be performed on basic tables:

1. ADD COLUMN: Add a column.
2. DROP COLUMN: Delete a column.
3. MODIFY COLUMN: Modify the column definition. If the data column type is variable-length, this command can be used to modify its width, but it can only be increased, not decreased.
4. RENAME COLUMN: Change the column name.
5. The primary key column of a basic table cannot be modified, nor can primary key columns be added or deleted using ADD/DROP COLUMN.

**Parameter Description**

1. COMMENT: Table comment. Applicable to supertables, subtables, and basic tables. The maximum length is 1024 bytes.
2. TTL: Time to Live, is a parameter used to specify the lifecycle of the table. If this parameter is specified when creating the table, TDengine will automatically delete the table after its existence exceeds the specified TTL. This TTL time is approximate, and the system does not guarantee deletion at the specified time, but it ensures that such a mechanism exists and that it will eventually be deleted. TTL is measured in days, with a range of [0, 2147483647], defaulting to 0, which means no limit; the expiration time is the table creation time plus the TTL time. TTL is not related to the KEEP parameter of the database; if KEEP is smaller than TTL, the data may be deleted before the table is deleted.

### Add Column

```sql
ALTER TABLE tb_name ADD COLUMN field_name data_type;
```

### Drop Column

```sql
ALTER TABLE tb_name DROP COLUMN field_name;
```

### Modify Column Width

```sql
ALTER TABLE tb_name MODIFY COLUMN field_name data_type(length);
```

### Rename Column

```sql
ALTER TABLE tb_name RENAME COLUMN old_col_name new_col_name
```

### Modify Table Lifecycle

```sql
ALTER TABLE tb_name TTL value
```

### Modify Table Comment

```sql
ALTER TABLE tb_name COMMENT 'string_value'
```

## Modify Subtable

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

**Usage Instructions**

1. For modifications to subtable columns and tags, except for changing tag values, all modifications must be made through the supertable.

**Parameter Description**

1. COMMENT: Table comment. Applicable to supertables, subtables, and basic tables. The maximum length is 1024 bytes.
2. TTL: Time to Live, is a parameter used to specify the lifecycle of the table. If this parameter is specified when creating the table, TDengine will automatically delete the table after its existence exceeds the specified TTL. This TTL time is approximate, and the system does not guarantee deletion at the specified time, but it ensures that such a mechanism exists and that it will eventually be deleted. TTL is measured in days, with a range of [0, 2147483647], defaulting to 0, which means no limit; the expiration time is the table creation time plus the TTL time. TTL is not related to the KEEP parameter of the database; if KEEP is smaller than TTL, the data may be deleted before the table is deleted.

### Modify Subtable Tag Value

```sql
ALTER TABLE tb_name SET TAG tag_name=new_tag_value;
```

### Modify Table Lifecycle

```sql
ALTER TABLE tb_name TTL value
```

### Modify Table Comment

```sql
ALTER TABLE tb_name COMMENT 'string_value'
```

## Drop Table

You can drop one or more basic tables or subtables in a single SQL statement.

```sql
DROP TABLE [IF EXISTS] [db_name.]tb_name [, [IF EXISTS] [db_name.]tb_name] ...
```

**Note**: Dropping a table does not immediately free the disk space occupied by that table; instead, it marks the data of that table as deleted, and these data will not appear in queries. However, the release of disk space will be delayed until the system automatically or manually performs data compaction.

## View Table Information

### Show All Tables

The following SQL statement can list all table names in the current database.

```sql
SHOW TABLES [LIKE tb_name_wildchar];
```

### Show Table Creation Statement

```sql
SHOW CREATE TABLE tb_name;
```

Commonly used for database migration. For an existing table, it returns its creation statement; executing this statement in another cluster will create a table with the same structure.

### Get Table Structure Information

```sql
DESCRIBE [db_name.]tb_name;
```
