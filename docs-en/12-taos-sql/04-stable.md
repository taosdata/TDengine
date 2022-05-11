---
sidebar_label: STable
title: Super Table
---

:::note

Keyword `STABLE`, abbreviated for super table, is supported since version 2.0.15.

:::

## Crate STable

```
CREATE STABLE [IF NOT EXISTS] stb_name (timestamp_field_name TIMESTAMP, field1_name data_type1 [, field2_name data_type2 ...]) TAGS (tag1_name tag_type1, tag2_name tag_type2 [, tag3_name tag_type3]);
```

The SQL statement of creating STable is similar to that of creating table, but a special column named as `TAGS` must be specified with the names and types of the tags.

:::info

1. The tag types specified in TAGS should NOT be timestamp. Since 2.1.3.0 timestamp type can be used in TAGS column, but its value must be fixed and arithmetic operation can't be applied on it.
2. The tag names specified in TAGS should NOT be same as other columns.
3. The tag names specified in TAGS should NOT be same as any reserved keywords.(Please refer to [keywords](/taos-sql/keywords/)
4. The maximum number of tags specified in TAGS is 128, but there must be at least one tag, and the total length of all tag columns should NOT exceed 16KB.

:::

## Drop STable

```
DROP STABLE [IF EXISTS] stb_name;
```

All the sub-tables created using the deleted stable will be deleted automatically.

## Show All STables

```
SHOW STABLES [LIKE tb_name_wildcard];
```

This command can be used to display the information of all STables in the current database, including name, creation time, number of columns, number of tags, number of tables created using this STable.

## Show The Create Statement of A STable

```
SHOW CREATE STABLE stb_name;
```

This command is useful in migrating data from one TDengine cluster to another one because it can be used to create an exactly same STable in the target database.

## Get STable Definition

```
DESCRIBE stb_name;
```

## Change Columns Of STable

### Add A Column

```
ALTER STABLE stb_name ADD COLUMN field_name data_type;
```

### Remove A Column

```
ALTER STABLE stb_name DROP COLUMN field_name;
```

### Change Column Length

```
ALTER STABLE stb_name MODIFY COLUMN field_name data_type(length);
```

This command can be used to change (or incerase, more specifically) the length of a column of variable length types, like BINARY or NCHAR.

## Change Tags of A STable

### Add A Tag

```
ALTER STABLE stb_name ADD TAG new_tag_name tag_type;
```

This command is used to add a new tag for a STable and specify the tag type.

### Remove A Tag

```
ALTER STABLE stb_name DROP TAG tag_name;
```

The tag will be removed automatically from all the sub tables crated using the super table as template once a tag is removed from a super table.

### Change A Tag

```
ALTER STABLE stb_name CHANGE TAG old_tag_name new_tag_name;
```

The tag name will be changed automatically from all the sub tables crated using the super table as template once a tag name is changed for a super table.

### Change Tag Length

```
ALTER STABLE stb_name MODIFY TAG tag_name data_type(length);
```

This command can be used to change (or incerase, more specifically) the length of a tag of variable length types, like BINARY or NCHAR.

:::note
Changing tag value can be applied to only sub tables. All other tag operations, like add tag, remove tag, however, can be applied to only STable. If a new tag is added for a STable, the tag will be added with NULL value for all its sub tables.

:::
