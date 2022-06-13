---
sidebar_label: STable
title: Super Table
---

:::note

Keyword `STable`, abbreviated for super table, is supported since version 2.0.15.

:::

## Create STable

```
CREATE STable [IF NOT EXISTS] stb_name (timestamp_field_name TIMESTAMP, field1_name data_type1 [, field2_name data_type2 ...]) TAGS (tag1_name tag_type1, tag2_name tag_type2 [, tag3_name tag_type3]);
```

The SQL statement of creating a STable is similar to that of creating a table, but a special column set named `TAGS` must be specified with the names and types of the tags.

:::info

1. A tag can be of type timestamp, since version 2.1.3.0, but its value must be fixed and arithmetic operations cannot be performed on it. Prior to version 2.1.3.0, tag types specified in TAGS could not be of type timestamp.
2. The tag names specified in TAGS should NOT be the same as other columns.
3. The tag names specified in TAGS should NOT be the same as any reserved keywords.(Please refer to [keywords](/taos-sql/keywords/)
4. The maximum number of tags specified in TAGS is 128, there must be at least one tag, and the total length of all tag columns should NOT exceed 16KB.

:::

## Drop STable

```
DROP STable [IF EXISTS] stb_name;
```

All the subtables created using the deleted STable will be deleted automatically.

## Show All STables

```
SHOW STableS [LIKE tb_name_wildcard];
```

This command can be used to display the information of all STables in the current database, including name, creation time, number of columns, number of tags, and number of tables created using this STable.

## Show The Create Statement of A STable

```
SHOW CREATE STable stb_name;
```

This command is useful in migrating data from one TDengine cluster to another because it can be used to create the exact same STable in the target database.

## Get STable Definition

```
DESCRIBE stb_name;
```

## Change Columns Of STable

### Add A Column

```
ALTER STable stb_name ADD COLUMN field_name data_type;
```

### Remove A Column

```
ALTER STable stb_name DROP COLUMN field_name;
```

### Change Column Length

```
ALTER STable stb_name MODIFY COLUMN field_name data_type(length);
```

This command can be used to change (or more specifically, increase) the length of a column of variable length types, like BINARY or NCHAR.

## Change Tags of A STable

### Add A Tag

```
ALTER STable stb_name ADD TAG new_tag_name tag_type;
```

This command is used to add a new tag for a STable and specify the tag type.

### Remove A Tag

```
ALTER STable stb_name DROP TAG tag_name;
```

The tag will be removed automatically from all the subtables, created using the super table as template, once a tag is removed from a super table.

### Change A Tag

```
ALTER STable stb_name CHANGE TAG old_tag_name new_tag_name;
```

The tag name will be changed automatically for all the subtables, created using the super table as template, once a tag name is changed for a super table.

### Change Tag Length

```
ALTER STable stb_name MODIFY TAG tag_name data_type(length);
```

This command can be used to change (or more specifically, increase) the length of a tag of variable length types, like BINARY or NCHAR.

:::note
Changing tag values can be applied to only subtables. All other tag operations, like add tag, remove tag, however, can be applied to only STable. If a new tag is added for a STable, the tag will be added with NULL value for all its subtables.

:::
