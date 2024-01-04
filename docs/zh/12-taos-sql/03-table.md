---
title: 表
sidebar_label: 表
description: 对表的各种管理操作
---

## 创建表

`CREATE TABLE` 语句用于创建普通表和以超级表为模板创建子表。

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
    col_name column_type

table_options:
    table_option ...

table_option: {
    COMMENT 'string_value'
  | SMA(col_name [, col_name] ...)
  | TTL value
}

```

**使用说明**

1. 表的第一个字段必须是 TIMESTAMP，并且系统自动将其设为主键；
2. 表名最大长度为 192；
3. 表的每行长度不能超过 48KB（从 3.0.5.0 版本开始为 64KB）;（注意：每个 BINARY/NCHAR/GEOMETRY 类型的列还会额外占用 2 个字节的存储位置）
4. 子表名只能由字母、数字和下划线组成，且不能以数字开头，不区分大小写
5. 使用数据类型 BINARY/NCHAR/GEOMETRY，需指定其最长的字节数，如 BINARY(20)，表示 20 字节；
6. 为了兼容支持更多形式的表名，TDengine 引入新的转义符 "\`"，可以让表名与关键词不冲突，同时不受限于上述表名称合法性约束检查。但是同样具有长度限制要求。使用转义字符以后，不再对转义字符中的内容进行大小写统一。
   例如：\`aBc\` 和 \`abc\` 是不同的表名，但是 abc 和 aBc 是相同的表名。

**参数说明**

1. COMMENT：表注释。可用于超级表、子表和普通表。
2. SMA：Small Materialized Aggregates，提供基于数据块的自定义预计算功能。预计算类型包括 MAX、MIN 和 SUM。可用于超级表/普通表。
3. TTL：Time to Live，是用户用来指定表的生命周期的参数。如果创建表时指定了这个参数，当该表的存在时间超过 TTL 指定的时间后，TDengine 自动删除该表。这个 TTL 的时间只是一个大概时间，系统不保证到了时间一定会将其删除，而只保证存在这样一个机制且最终一定会删除。TTL 单位是天，默认为 0，表示不限制，到期时间为表创建时间加上 TTL 时间。TTL 与数据库 KEEP 参数没有关联，如果 KEEP 比 TTL 小，在表被删除之前数据也可能已经被删除。

## 创建子表

### 创建子表

```sql
CREATE TABLE [IF NOT EXISTS] tb_name USING stb_name TAGS (tag_value1, ...);
```

### 创建子表并指定标签的值

```sql
CREATE TABLE [IF NOT EXISTS] tb_name USING stb_name (tag_name1, ...) TAGS (tag_value1, ...);
```

以指定的超级表为模板，也可以指定一部分 TAGS 列的值来创建数据表（没被指定的 TAGS 列会设为空值）。

### 批量创建子表

```sql
CREATE TABLE [IF NOT EXISTS] tb_name1 USING stb_name TAGS (tag_value1, ...) [IF NOT EXISTS] tb_name2 USING stb_name TAGS (tag_value2, ...) ...;
```

批量建表方式要求数据表必须以超级表为模板。 在不超出 SQL 语句长度限制的前提下，单条语句中的建表数量建议控制在 1000 ～ 3000 之间，将会获得比较理想的建表速度。

## 修改普通表

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

**使用说明**
对普通表可以进行如下修改操作

1. ADD COLUMN：添加列。
2. DROP COLUMN：删除列。
3. MODIFY COLUMN：修改列定义，如果数据列的类型是可变长类型，那么可以使用此指令修改其宽度，只能改大，不能改小。
4. RENAME COLUMN：修改列名称。

### 增加列

```sql
ALTER TABLE tb_name ADD COLUMN field_name data_type;
```

### 删除列

```sql
ALTER TABLE tb_name DROP COLUMN field_name;
```

### 修改列宽

```sql
ALTER TABLE tb_name MODIFY COLUMN field_name data_type(length);
```

### 修改列名

```sql
ALTER TABLE tb_name RENAME COLUMN old_col_name new_col_name
```

## 修改子表

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

**使用说明**

1. 对子表的列和标签的修改，除了更改标签值以外，都要通过超级表才能进行。

### 修改子表标签值

```
ALTER TABLE tb_name SET TAG tag_name=new_tag_value;
```

## 删除表

可以在一条 SQL 语句中删除一个或多个普通表或子表。

```sql
DROP TABLE [IF EXISTS] [db_name.]tb_name [, [IF EXISTS] [db_name.]tb_name] ...
```

## 查看表的信息

### 显示所有表

如下 SQL 语句可以列出当前数据库中的所有表名。

```sql
SHOW TABLES [LIKE tb_name_wildchar];
```

### 显示表创建语句

```
SHOW CREATE TABLE tb_name;
```

常用于数据库迁移。对一个已经存在的数据表，返回其创建语句；在另一个集群中执行该语句，就能得到一个结构完全相同的数据表。

### 获取表结构信息

```
DESCRIBE [db_name.]tb_name;
```
