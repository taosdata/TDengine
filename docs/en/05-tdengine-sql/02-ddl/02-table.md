---
title: Tables
---

## Create Table

The `CREATE TABLE` statement is used to create basic tables and subtables using a supertable as a template. You can also create a supertable by specifying the `TAGS` clause, or create a basic table with owned tags.

```sql
CREATE TABLE [IF NOT EXISTS] [db_name.]tb_name (create_definition [, create_definition] ...) [table_options]

CREATE TABLE create_subtable_clause

CREATE TABLE [IF NOT EXISTS] [db_name.]tb_name (create_definition [, create_definition] ...)
    [TAGS (tag_def [, tag_def] ...)]
    [table_options]

create_subtable_clause: {
    create_subtable_clause [create_subtable_clause] ...
  | [IF NOT EXISTS] [db_name.]tb_name USING [db_name.]stb_name [(tag_name [, tag_name] ...)] TAGS (tag_value [, tag_value] ...)
}

create_definition:
    col_name column_definition

column_definition:
    type_name [COMPOSITE KEY] [ENCODE 'encode_type'] [COMPRESS 'compress_type'] [LEVEL 'level_type']

tag_def:
    tag_name type_name [= const_value]

table_options:
    table_option ...

table_option: {
    COMMENT 'string_value'
  | SMA(col_name [, col_name] ...)
  | TTL value
}

```

Usage Notes:

1. For table (column) naming conventions, see [Naming Rules](../11-appendix/02-limit.md).
2. The maximum length for table names is 192 characters.
3. The first field of the table must be TIMESTAMP, and the system automatically sets it as the primary key.
4. In addition to the timestamp primary key column, a second column can be designated as an additional composite primary key column using the `COMPOSITE KEY` keyword. The second column designated as a composite primary key must be an integer type such as `INT`, `BIGINT`, `INT UNSIGNED`, or `BIGINT UNSIGNED`, or a string type such as `VARCHAR` or `BINARY`.
5. The maximum row length of a table cannot exceed 64 KB. Each `BINARY`, `VARCHAR`, `NCHAR`, `GEOMETRY`, and `VARBINARY` column also occupies an additional 2 bytes of storage space.
6. When using `BINARY`, `VARCHAR`, `VARBINARY`, or `GEOMETRY`, specify the maximum number of bytes. For example, `VARCHAR(20)` indicates up to 20 single-byte characters. When using `NCHAR`, specify the maximum number of characters. For example, `NCHAR(10)` indicates up to 10 `NCHAR` characters.
7. For the use of `ENCODE` and `COMPRESS`, please refer to [Column Compression](../03-data-write/03-compress.md)
8. The semantics of the `TAGS` clause depend on whether the tags carry values: when no tag has a value, a supertable is created; when every tag has an explicit `= const_value`, a basic table with owned tags is created (`= NULL` is a valid explicit value); mixing valued and valueless tags is rejected. Owned tags of a basic table only support literal values, not `FROM` references; the `DECIMAL` type is not supported for tags, and a `JSON` tag can only be declared at table creation and must be the only tag. For how to query and maintain basic-table tags, see "Create a Basic Table with Tags" and "Modify basic tables" below.

Parameter Description:

1. COMMENT: Table comment. Can be used for supertables, subtables, and basic tables. The maximum length is 1024 bytes.
2. SMA: Small Materialized Aggregates, provides block-based pre-computation to accelerate aggregation queries. Pre-computation types include MAX, MIN, and SUM. By default, the system creates block-wise SMA for most columns (some types such as `BINARY` and `NCHAR` are not created by default); if `SMA(col_name, ...)` is specified at table creation, block-wise SMA is created only for the listed columns. Available for supertables and basic tables.
3. TTL: Time to Live, a parameter used by users to specify the lifespan of a table. If this parameter is specified when creating a table, TDengine automatically deletes the table after its existence exceeds the specified TTL time. This TTL time is approximate, the system does not guarantee deletion at the exact time but ensures that such a mechanism exists and will eventually delete it. TTL is measured in days, with a range of [0, 2147483647], defaulting to 0, meaning no limit, with the expiration time being the table creation time plus TTL time. TTL is not associated with the database KEEP parameter; if KEEP is smaller than TTL, data may be deleted before the table is removed.

## Create Basic Table

```sql
CREATE TABLE [IF NOT EXISTS] tb_name (create_definition [, create_definition] ...);
```

### Create a Basic Table with Tags

```sql
CREATE TABLE [IF NOT EXISTS] tb_name (create_definition [, create_definition] ...)
    TAGS (tag_name1 tag_type1 = const_value1 [, ...]);
```

When every tag in the `TAGS` clause carries an explicit `= const_value`, the statement creates a basic table with owned tags, for example:

```sql
CREATE TABLE ntb (ts TIMESTAMP, v INT) TAGS (loc INT = 5, dept VARCHAR(16) = 'rd');
```

An owned tag of a basic table is a table-level constant: projecting it returns that constant for every row, and using it in a `WHERE` filter is evaluated with constant semantics; tag rows are marked `TAG` in the `DESC` output. `SHOW CREATE TABLE` returns a single `CREATE TABLE ... TAGS(...)` statement in which owned tags are inlined as `= value` (a NULL value is emitted as `= NULL`), ready for replay.

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
CREATE TABLE [IF NOT EXISTS] USING [db_name.]stb_name (tbname [, tag_name] ...) FILE 'csv_file_path';
```

Parameter Description:

1. `FILE` syntax indicates that the data comes from a CSV file (separated by English commas, with each value enclosed in English single quotes), and the CSV file does not need a header. The CSV file should contain only `tbname` and tag values. If you need to insert data, see [Data Writing](../03-data-write/01-insert.md).
2. Create subtables for the specified stb_name, which must already exist.
3. The order of the field list must be consistent with the order of the columns in the CSV file. The list must not contain duplicates and must include `tbname`. It may contain zero or more tag columns already defined in the supertable. Tag values not included in the list will be set to `NULL`.

## Modify basic tables

```sql
ALTER TABLE [db_name.]tb_name alter_table_clause

alter_table_clause: {
    alter_table_options
  | ADD COLUMN col_name column_type
  | DROP COLUMN col_name
  | MODIFY COLUMN col_name column_type
  | RENAME COLUMN old_col_name new_col_name
  | ADD TAG tag_name tag_type
  | SET TAG tag_name = new_tag_value
  | DROP TAG tag_name
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
4. After `MODIFY COLUMN`, you can also specify column compression options such as `ENCODE`, `COMPRESS`, and `LEVEL`. See [Column Compression](../03-data-write/03-compress.md).
5. RENAME COLUMN: Change the column name.
6. The primary key columns of basic tables cannot be modified, nor can they be added or removed through `ADD COLUMN` or `DROP COLUMN`.
7. `ADD TAG`: Add an owned tag with an initial value of `NULL`; assign a value later with `SET TAG`.
8. `SET TAG`: Set the value of an owned tag.
9. `DROP TAG`: Drop a tag.
10. Basic tables do not support tag references: `ADD TAG ... FROM ...` and `SET TAG ... = db_name.table_name.tag_name` are both rejected; `JSON` tags cannot be added with `ADD TAG`.

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
ALTER TABLE tb_name RENAME COLUMN old_col_name new_col_name;
```

### Add Tag

```sql
ALTER TABLE tb_name ADD TAG tag_name tag_type;
```

### Set Tag Value

```sql
ALTER TABLE tb_name SET TAG tag_name = new_tag_value;
```

### Drop Tag

```sql
ALTER TABLE tb_name DROP TAG tag_name;
```

### Modify table lifespan

```sql
ALTER TABLE tb_name TTL value;
```

### Modify Table Comment

```sql
ALTER TABLE tb_name COMMENT 'string_value';
```

## Modify Subtable

```sql
ALTER TABLE [db_name.]tb_name alter_table_clause;

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

### Batch Modify Subtable Tag Value

```sql
ALTER TABLE tb_name1 SET TAG tag_name1=new_tag_value1, tag_name2=new_tag_value2 tb_name2 SET TAG tag_name3=new_tag_value3 ...;
```

### Modify Table Lifespan

```sql
ALTER TABLE tb_name TTL value;
```

### Modify Table Comment

```sql
ALTER TABLE tb_name COMMENT 'string_value';
```

## Delete Table

You can delete one or more regular tables or subtables in a single SQL statement.

```sql
DROP TABLE [IF EXISTS] [db_name.]tb_name [, [IF EXISTS] [db_name.]tb_name] ...;
```

**Note**: Deleting a table does not immediately free up the disk space occupied by the table. Instead, the table's data is marked as deleted. This data will not appear in queries, but freeing up disk space is delayed until the system automatically reorganizes the data, or until the user manually compacts the data by using the enterprise-only `COMPACT` feature.

## View Table Information

### Show All Tables

The following SQL statement can list all basic and child tables in the specified database or the current database. `NORMAL` displays only basic tables, and `CHILD` displays only child tables.

```sql
SHOW [NORMAL | CHILD] [db_name.]TABLES [LIKE 'pattern'];
```

### Show Table Creation Statement

```sql
SHOW CREATE TABLE [db_name.]tb_name;
```

Commonly used for database migration. For an existing table, it returns its creation statement; executing this statement in another cluster will produce a table with the exact same structure.

### Get Table Structure Information

```sql
DESCRIBE [db_name.]tb_name;
```
