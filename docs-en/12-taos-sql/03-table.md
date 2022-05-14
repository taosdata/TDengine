---
sidebar_label: Table
title: Table
description: create super table, normal table and sub table, drop tables and change tables
---

## Create Table

```
CREATE TABLE [IF NOT EXISTS] tb_name (timestamp_field_name TIMESTAMP, field1_name data_type1 [, field2_name data_type2 ...]);
```

:::info

1. The first column of a table must be in TIMESTAMP type, and it will be set as primary key automatically
2. The maximum length of table name is 192 bytes.
3. The maximum length of each row is 16k bytes, please be notes that the extra 2 bytes used by each BINARY/NCHAR column are also counted in.
4. The name of sub-table can only be consisted of English characters, digits and underscore, and can't be started with digit. Table names are case insensitive.
5. The maximum length in bytes must be specified when using BINARY or NCHAR type.
6. Escape character "\`" can be used to avoid the conflict between table names and reserved keywords, above rules will be bypassed when using escape character on table names, but the upper limit for name length is still valid. The table names specified using escape character are case sensitive. Only ASCII visible characters can be used with escape character.
   For example \`aBc\` and \`abc\` are different table names but `abc` and `aBc` are same table names because they are both converted to `abc` internally.

:::

### Create Table Using STable As Template

```
CREATE TABLE [IF NOT EXISTS] tb_name USING stb_name TAGS (tag_value1, ...);
```

The above command creates a sub table using the specified super table as template and the specified tab values.

### Create Table Using STable As Template With A Part of Tags

```
CREATE TABLE [IF NOT EXISTS] tb_name USING stb_name (tag_name1, ...) TAGS (tag_value1, ...);
```

The tags for which no value is specified will be set to NULL.

### Create Tables in Batch

```
CREATE TABLE [IF NOT EXISTS] tb_name1 USING stb_name TAGS (tag_value1, ...) [IF NOT EXISTS] tb_name2 USING stb_name TAGS (tag_value2, ...) ...;
```

This way can be used to create a lot of tables in a single SQL statement to accelerate the speed of the creating tables.

:::info

- Creating tables in batch must use super table as template.
- The length of single statement is suggested to be between 1,000 and 3,000 bytes for best performance.

:::

## Drop Tables

```
DROP TABLE [IF EXISTS] tb_name;
```

## Show All Tables In Current Database

```
SHOW TABLES [LIKE tb_name_wildcar];
```

## Show Create Statement of A Table

```
SHOW CREATE TABLE tb_name;
```

This way is useful when migrating the data in one TDengine cluster to another one because it can be used to create exactly same tables in the target database.

## Show Table Definition

```
DESCRIBE tb_name;
```

## Change Table Definition

### Add A Column

```
ALTER TABLE tb_name ADD COLUMN field_name data_type;
```

:::info

1. The maximum number of columns is 4096, the minimum number of columns is 2.
2. The maximum length of column name is 64 bytes.

:::

### Remove A Column

```
ALTER TABLE tb_name DROP COLUMN field_name;
```

:::note
If a table is created using a super table as template, the table definition can only be changed on the corresponding super table, but the change will be automatically applied to all the sub tables created using this super table as template. For tables created in normal way, the table definition can be changed directly on the table.

:::

### Change Column Length

```
ALTER TABLE tb_name MODIFY COLUMN field_name data_type(length);
```

The the type of a column is variable length, like BINARY or NCHAR, this way can be used to change (or increase) the length of the column.

:::note
If a table is created using a super table as template, the table definition can only be changed on the corresponding super table, but the change will be automatically applied to all the sub tables created using this super table as template. For tables created in normal way, the table definition can be changed directly on the table.

:::

### Change Tag Value Of Sub Table

```
ALTER TABLE tb_name SET TAG tag_name=new_tag_value;
```

This command can be used to change the tag value if the table is created using a super table as template.
