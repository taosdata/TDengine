---
title: 数据表
sidebar_label: 数据表
description: 对表的各种管理操作
---

## 创建表

`CREATE TABLE` 语句用于创建普通表和以超级表为模板创建子表。也可以通过指定 `TAGS` 子句创建超级表，或创建带自有标签的普通表。

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

**使用说明**

1. 表（列）名命名规则参见 [名称命名规则](../11-appendix/02-limit.md#名称命名规则)。
2. 表名最大长度为 192 字节，不包括数据库名前缀和分隔符。
3. 表的第一个字段必须是 `TIMESTAMP`，系统会自动将其设为主键。
4. 除时间戳主键列之外，还可以通过 `COMPOSITE KEY` 关键字指定第二列为额外的主键列，该列与时间戳列共同组成复合主键。当设置了复合主键时，两条记录的时间戳列与 `COMPOSITE KEY` 列都相同，才会被认为是重复记录，数据库只保留最新的一条；否则视为两条记录，全部保留。被指定为主键列的第二列必须为整型或字符串类型，如 `INT`、`BIGINT`、`INT UNSIGNED`、`BIGINT UNSIGNED`、`VARCHAR`、`BINARY`。
5. 表的每行长度不能超过 64 KB。每个 `BINARY`、`VARCHAR`、`NCHAR`、`GEOMETRY`、`VARBINARY` 类型的列还会额外占用 2 个字节的存储位置。
6. 使用 `BINARY`、`VARCHAR`、`VARBINARY`、`GEOMETRY` 类型时，需要指定最长字节数，如 `VARCHAR(20)` 表示最多存储 20 个单字节字符；使用 `NCHAR` 时，需要指定字符长度，如 `NCHAR(10)` 表示最多存储 10 个 `NCHAR` 字符。
7. 关于 `ENCODE` 和 `COMPRESS` 的使用，参见 [按列压缩](../03-data-write/03-compress.md)。
8. `TAGS` 子句的语义取决于标签是否带值：全部标签不带值时创建超级表；全部标签带 `= const_value` 显式值时创建带自有标签的普通表（`= NULL` 是合法的显式值）；部分带值会报错。普通表的自有标签只支持字面量值，不支持 `FROM` 引用；标签类型不支持 `DECIMAL`，`JSON` 类型标签只允许在建表时声明且必须是唯一的标签。普通表标签的查询与维护方式见下方“创建带标签的普通表”和“修改普通表”章节。

**参数说明**

1. `COMMENT`：表注释。可用于超级表、子表和普通表。最大长度为 1024 个字节。
2. `SMA`：Small Materialized Aggregates，提供基于数据块的预计算以加速聚合查询。预计算类型包括 `MAX`、`MIN` 和 `SUM`。默认情况下，系统会为大多数列创建块级 `SMA`，部分类型如 `BINARY`、`NCHAR` 等默认不创建；当建表时显式指定 `SMA(col_name, ...)` 时，仅对指定列创建块级 `SMA`。可用于超级表和普通表。
3. `TTL`：Time to Live，用于指定表的生命周期。指定 `TTL` 后，当该表的存在时间超过 `TTL` 指定的时间后，TDengine 会自动删除该表。这个删除时间只是大致时间，系统不保证到期后立即删除，但会保证最终删除。`TTL` 单位是天，取值范围为 `[0, 2147483647]`，默认为 `0`，表示不限制，到期时间为表创建时间加上 `TTL` 时间。`TTL` 与数据库 `KEEP` 参数没有关联，如果 `KEEP` 比 `TTL` 小，在表被删除之前数据也可能已经被删除。

### 创建普通表

```sql
CREATE TABLE [IF NOT EXISTS] tb_name (create_definition [, create_definition] ...);
```

### 创建带标签的普通表

```sql
CREATE TABLE [IF NOT EXISTS] tb_name (create_definition [, create_definition] ...)
    TAGS (tag_name1 tag_type1 = const_value1 [, ...]);
```

`TAGS` 子句中全部标签都带 `= const_value` 显式值时，创建的是带自有标签的普通表，例如：

```sql
CREATE TABLE ntb (ts TIMESTAMP, v INT) TAGS (loc INT = 5, dept VARCHAR(16) = 'rd');
```

普通表的自有标签是表级常量：投影时对每行返回该常量，用于 `WHERE` 过滤时按常量语义求值；`DESC` 输出中标签行带 `TAG` 标记。`SHOW CREATE TABLE` 输出单条 `CREATE TABLE ... TAGS(...)` 语句，自有标签内联输出 `= value`（`NULL` 值输出 `= NULL`），可直接回放重建。

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

批量建表方式要求数据表必须以超级表为模板。在不超出 SQL 语句长度限制的前提下，单条语句中的建表数量建议控制在 1000 ～ 3000 之间，将会获得比较理想的建表速度。

### 使用 CSV 批量创建子表

```sql
CREATE TABLE [IF NOT EXISTS] USING [db_name.]stb_name (tbname [, tag_name] ...) FILE 'csv_file_path';
```

**参数说明**

1. `FILE` 语法表示数据来自 CSV 文件（英文逗号分隔、英文单引号括住每个值），CSV 文件无需表头。CSV 文件中应仅包含 `tbname` 与标签值。如需插入数据，参见 [数据写入](../03-data-write/01-insert.md)。
2. 为指定的 `stb_name` 创建子表，该超级表必须已经存在。
3. 字段列表顺序与 CSV 文件各列内容顺序一致。列表中不允许出现重复项，且必须包含 `tbname`，可包含零个或多个超级表中已定义的标签列。未包含在列表中的标签值将被设置为 `NULL`。

## 修改表

### 修改普通表

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

**使用说明**

对普通表可以进行如下修改操作：

1. `ADD COLUMN`：添加列。
2. `DROP COLUMN`：删除列。
3. `MODIFY COLUMN`：修改列定义。如果数据列的类型是可变长类型，可以使用此指令修改其宽度，只能改大，不能改小。
4. `MODIFY COLUMN` 后也可指定 `ENCODE`、`COMPRESS`、`LEVEL` 等列压缩选项，参见 [按列压缩](../03-data-write/03-compress.md)。
5. `RENAME COLUMN`：修改列名称。
6. 普通表的主键列不能被修改，也不能通过 `ADD COLUMN`/`DROP COLUMN` 来添加或删除主键列。
7. `ADD TAG`：添加自有标签，初值为 `NULL`，可再经 `SET TAG` 赋值。
8. `SET TAG`：设置自有标签的值。
9. `DROP TAG`：删除标签。
10. 普通表不支持标签引用：`ADD TAG ... FROM ...` 和 `SET TAG ... = db_name.table_name.tag_name` 均报错；`JSON` 类型标签不能通过 `ADD TAG` 追加。

**参数说明**

1. `COMMENT`：表注释。可用于超级表、子表和普通表。最大长度为 1024 个字节。
2. `TTL`：表的生命周期（单位：天）。取值为 `[0, 2147483647]`，`0` 表示不限制。超过该天数后，系统会最终自动删除该表（不保证到期立即删除）。到期时间按表创建时间起算；与数据库 `KEEP` 无关。

#### 增加列

```sql
ALTER TABLE tb_name ADD COLUMN field_name data_type;
```

#### 删除列

```sql
ALTER TABLE tb_name DROP COLUMN field_name;
```

#### 修改列宽

```sql
ALTER TABLE tb_name MODIFY COLUMN field_name data_type(length);
```

#### 修改列名

```sql
ALTER TABLE tb_name RENAME COLUMN old_col_name new_col_name;
```

#### 增加标签

```sql
ALTER TABLE tb_name ADD TAG tag_name tag_type;
```

#### 设置标签值

```sql
ALTER TABLE tb_name SET TAG tag_name = new_tag_value;
```

#### 删除标签

```sql
ALTER TABLE tb_name DROP TAG tag_name;
```

#### 修改表生命周期

```sql
ALTER TABLE tb_name TTL value;
```

#### 修改表注释

```sql
ALTER TABLE tb_name COMMENT 'string_value';
```

### 修改子表

```sql
ALTER TABLE [db_name.]tb_name alter_table_clause;

alter_table_clause: {
    alter_table_options
  | SET TAG tag_name = new_tag_value, tag_name2 = new_tag2_value ...
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

**参数说明**

1. `COMMENT`：表注释。可用于超级表、子表和普通表。最大长度为 1024 个字节。
2. `TTL`：表的生命周期（单位：天）。取值为 `[0, 2147483647]`，`0` 表示不限制。超过该天数后，系统会最终自动删除该表（不保证到期立即删除）。到期时间按表创建时间起算；与数据库 `KEEP` 无关。

#### 修改标签值

```sql
ALTER TABLE tb_name SET TAG tag_name1 = new_tag_value1, tag_name2 = new_tag_value2 ...;
```

#### 批量修改标签值

```sql
ALTER TABLE tb_name1 SET TAG tag_name1 = new_tag_value1, tag_name2 = new_tag_value2 tb_name2 SET TAG tag_name3 = new_tag_value3 ...;
```

#### 修改生命周期

```sql
ALTER TABLE tb_name TTL value;
```

#### 修改注释

```sql
ALTER TABLE tb_name COMMENT 'string_value';
```

## 删除表

可以在一条 SQL 语句中删除一个或多个普通表或子表。

```sql
DROP TABLE [IF EXISTS] [db_name.]tb_name [, [IF EXISTS] [db_name.]tb_name] ...;
```

**注意**：删除表并不会立即释放该表所占用的磁盘空间，而是把该表的数据标记为已删除，在查询时这些数据将不会再出现，但释放磁盘空间会延迟到系统自动（建库参数 `KEEP` 生效）或用户手动进行数据重整时（企业版功能 `COMPACT`）。

## 查看表的信息

### 显示所有表

如下 SQL 语句可以列出当前数据库中的所有普通表和子表信息。`NORMAL` 指定只显示普通表信息，`CHILD` 指定只显示子表信息。

```sql
SHOW [NORMAL | CHILD] [db_name.]TABLES [LIKE 'pattern'];
```

### 显示表创建语句

```sql
SHOW CREATE TABLE [db_name.]tb_name;
```

常用于数据库迁移。对一个已经存在的数据表，返回其创建语句；在另一个集群中执行该语句，即可得到一个结构完全相同的数据表。

### 获取表结构信息

```sql
DESCRIBE [db_name.]tb_name;
```
