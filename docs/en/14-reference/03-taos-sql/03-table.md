---
title: Tables
slug: /tdengine-reference/sql-manual/manage-tables
---

## Create Table

The `CREATE TABLE` statement is used to create basic tables and subtables using a supertable as a template.

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
    type_name [COMPOSITE KEY] [ENCODE 'encode_type'] [COMPRESS 'compress_type'] [LEVEL 'level_type']

table_options:
    table_option ...

table_option: {
    COMMENT 'string_value'
  | SMA(col_name [, col_name] ...)
  | TTL value
}

```

Usage Notes:

1. For table (column) naming conventions, see [Naming Rules](../names/).
2. The maximum length for table names is 192 characters.
3. The first field of the table must be TIMESTAMP, and the system automatically sets it as the primary key.
4. In addition to the timestamp primary key column, a second column can be designated as an additional composite primary key column using the COMPOSITE KEY keyword. The second column designated as a composite primary key must be of integer or string type (VARCHAR).
5. The maximum row length of a table cannot exceed 48KB (from version 3.0.5.0 onwards, 64KB); (Note: Each VARCHAR/NCHAR/GEOMETRY type column will also occupy an additional 2 bytes of storage space).
6. When using data types VARCHAR/NCHAR/GEOMETRY, specify the maximum number of bytes, e.g., VARCHAR(20) indicates 20 bytes.
7. For the use of `ENCODE` and `COMPRESS`, please refer to [Column Compression](../manage-data-compression/)

Parameter Description:

1. COMMENT: Table comment. Can be used for supertables, subtables, and basic tables. The maximum length is 1024 bytes.
2. SMA: Small Materialized Aggregates, provides custom pre-computation based on data blocks. Pre-computation types include MAX, MIN, and SUM. Available for supertables/basic tables.
3. TTL: Time to Live, a parameter used by users to specify the lifespan of a table. If this parameter is specified when creating a table, TDengine automatically deletes the table after its existence exceeds the specified TTL time. This TTL time is approximate, the system does not guarantee deletion at the exact time but ensures that such a mechanism exists and will eventually delete it. TTL is measured in days, with a range of [0, 2147483647], defaulting to 0, meaning no limit, with the expiration time being the table creation time plus TTL time. TTL is not associated with the database KEEP parameter; if KEEP is smaller than TTL, data may be deleted before the table is removed.

## Create Subtable

### Create Subtable

```sql
CREATE TABLE [IF NOT EXISTS] tb_name USING stb_name TAGS (tag_value1, ...);
```

### Create Subtable and Specify Tag Values

```sql
CREATE TABLE [IF NOT EXISTS] tb_name USING stb_name (tag_name1, ...) TAGS (tag_value1, ...);
```

Using the specified supertable as a template, you can also create tables by specifying some of the TAGS column values (TAGS columns that are not specified will be set to null values).

### Batch creation of subtables

```sql
CREATE TABLE [IF NOT EXISTS] tb_name1 USING stb_name TAGS (tag_value1, ...) [IF NOT EXISTS] tb_name2 USING stb_name TAGS (tag_value2, ...) ...;
```

The batch table creation method requires that the tables must use a supertable as a template. Under the premise of not exceeding the SQL statement length limit, it is recommended to control the number of tables created in a single statement between 1000 and 3000 to achieve an ideal table creation speed.

### Using CSV to batch create subtables

```sql
CREATE TABLE [IF NOT EXISTS] USING [db_name.]stb_name (field1_name [, field2_name] ....) FILE csv_file_path;
```

Parameter Description:

1. FILE syntax indicates that the data comes from a CSV file (separated by English commas, each value enclosed in English single quotes), and the CSV file does not need a header. The CSV file should only contain the table name and tag values. If you need to insert data, please refer to the 'Data Writing' section.
2. Create subtables for the specified stb_name, which must already exist.
3. The order of the field_name list must be consistent with the order of the columns in the CSV file. The list must not contain duplicates and must include `tbname`, and it may contain zero or more tag columns already defined in the supertable. Tag values not included in the list will be set to NULL.

## Modify basic tables

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

Usage Instructions:

The following modifications can be made to basic tables:

1. ADD COLUMN: Add a column.
2. DROP COLUMN: Delete a column.
3. MODIFY COLUMN: Modify the column definition. If the data column type is variable length, this command can be used to increase its width, but not decrease it.
4. RENAME COLUMN: Change the column name.
5. The primary key columns of basic tables cannot be modified, nor can they be added or removed through ADD/DROP COLUMN.

Parameter Description:

1. COMMENT: Table comment. Can be used for supertables, subtables, and basic tables. The maximum length is 1024 bytes.
2. TTL: Time to Live, a parameter used by users to specify the lifespan of a table. If this parameter is specified when creating a table, TDengine automatically deletes the table after its existence exceeds the specified TTL time. This TTL time is approximate, and the system does not guarantee that it will definitely delete the table at that time, but only ensures that there is such a mechanism and it will eventually be deleted. The TTL unit is days, with a range of [0, 2147483647], defaulting to 0, meaning no limit, and the expiration time is the table creation time plus the TTL time. TTL is not related to the database KEEP parameter. If KEEP is smaller than TTL, data may already be deleted before the table is deleted.

### Add column

```sql
ALTER TABLE tb_name ADD COLUMN field_name data_type;
```

### Delete column

```sql
ALTER TABLE tb_name DROP COLUMN field_name;
```

### Modify column width

```sql
ALTER TABLE tb_name MODIFY COLUMN field_name data_type(length);
```

### Change column name

```sql
ALTER TABLE tb_name RENAME COLUMN old_col_name new_col_name
```

### Modify table lifespan

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
  | SET tag tag_name = new_tag_value, tag_name2=new_tag2_value ...
}

alter_table_options:
    alter_table_option ...

alter_table_option: {
    TTL value
  | COMMENT 'string_value'
}
```

Usage Notes:

1. Modifications to columns and tags of subtables, except for changing tag values, must be done through the supertable.

Parameter Description:

1. COMMENT: Table comment. Can be used for supertables, subtables, and regular tables. The maximum length is 1024 bytes.
2. TTL: Time to Live, a parameter used by users to specify the lifespan of a table. If this parameter is specified when creating a table, TDengine automatically deletes the table after its existence exceeds the time specified by TTL. This TTL time is approximate; the system does not guarantee that it will delete the table exactly at that time, but it ensures that there is such a mechanism and it will eventually delete the table. TTL is measured in days, with a range of [0, 2147483647], default is 0, meaning no limit, and the expiration time is the table creation time plus TTL time. TTL is not related to the database KEEP parameter; if KEEP is smaller than TTL, data might be deleted before the table is.

### Modify Subtable Tag Value

```sql
ALTER TABLE tb_name SET TAG tag_name1=new_tag_value1, tag_name2=new_tag_value2 ...;
```

### Modify Table Lifespan

```sql
ALTER TABLE tb_name TTL value
```

### Modify Table Comment

```sql
ALTER TABLE tb_name COMMENT 'string_value'
```

## Delete Table

You can delete one or more regular tables or subtables in a single SQL statement.

```sql
DROP TABLE [IF EXISTS] [db_name.]tb_name [, [IF EXISTS] [db_name.]tb_name] ...
```

**Note**: Deleting a table does not immediately free up the disk space occupied by the table. Instead, the table's data is marked as deleted. This data will not appear in queries, but freeing up disk space is delayed until the system automatically or the user manually reorganizes the data.

## View Table Information

### Show All Tables

The following SQL statement can list all the table names in the current database.

```sql
SHOW TABLES [LIKE tb_name_wildcard];
```

### Show Table Creation Statement

```sql
SHOW CREATE TABLE tb_name;
```

Commonly used for database migration. For an existing table, it returns its creation statement; executing this statement in another cluster will produce a table with the exact same structure.

### Get Table Structure Information

```sql
DESCRIBE [db_name.]tb_name;
```
