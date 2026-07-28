---
sidebar_label: 超级表
title: 超级表
description: 对超级表的各种管理操作
---

## 创建超级表

```sql
CREATE STABLE [IF NOT EXISTS] [db_name.]stb_name
    (create_definition [, create_definition] ...)
    TAGS (create_definition [, create_definition] ...)
    [BASE ON [db_name.]parent_stb_name [, [db_name.]parent_stb_name] ...]
    [table_options]

create_definition:
    col_name column_definition

column_definition:
    type_name [COMPOSITE KEY] [ENCODE 'encode_type'] [COMPRESS 'compress_type'] [LEVEL 'level_type']

table_options:
    table_option ...

table_option: {
    COMMENT 'string_value'
  | SMA(col_name [, col_name] ...)
  | KEEP value
  | VIRTUAL {0 | 1}
}
```

**使用说明**

1. 超级表名和列名命名规则参见 [名称命名规则](../11-appendix/02-limit.md#名称命名规则)。
2. 非虚拟超级表中列和标签的总数最大为 `4096`，最少需要 3 个字段：一个 `TIMESTAMP` 主键列、一个数据列和一个标签列。虚拟超级表中的数据列最大为 `32767`；标签数量仍最多为 `128` 个。
3. 第一个字段必须是 `TIMESTAMP` 类型，系统自动将其设为主键。
4. 除时间戳主键列之外，还可以通过 `COMPOSITE KEY` 指定第二列为额外主键列。设置复合主键后，只有两条记录的时间戳列和 `COMPOSITE KEY` 列都相同，才会被认为是重复记录，数据库只保留最新的一条；否则视为两条记录并全部保留。`COMPOSITE KEY` 列必须是整型（`INT`、`BIGINT`、`UINT`、`UBIGINT`）或字符串类型（`VARCHAR`、`BINARY`）。
5. `TAGS` 用于定义超级表的标签列，需遵循以下约定：
   - 标签列名不能与数据列名重复，且不能是未转义的保留关键字。
   - 标签最多允许 `128` 个，至少需要 1 个，一个表中标签值的总长度不超过 16 KB。
   - `TAGS` 中的 `TIMESTAMP` 列写入数据时需要提供确定值，暂不支持 `NOW + 10s` 这类表达式。
6. `ENCODE`、`COMPRESS` 和 `LEVEL` 的使用方式参见 [按列压缩](../03-data-write/03-compress.md)。
7. `COMMENT`、`SMA` 等通用表选项说明参见 [数据表](./02-table.md#创建表)。
8. `KEEP` 仅对超级表生效，用于设置该超级表的数据保留时长。`KEEP` 的取值格式参见 [数据库](./01-database.md#keep)，但超级表的 `KEEP` 与数据库 `KEEP` 有以下差异：
   - 超级表 `KEEP` 不会立即影响查询结果，只有执行数据重整后，过期数据才会被清理并对查询不可见。
   - 超级表 `KEEP` 需小于数据库 `KEEP`。
   - 数据重整前需要先执行 `FLUSH`，否则可能不生效。
   - 数据重整后再通过 `ALTER STABLE` 修改 `KEEP` 并重新执行数据重整时，部分数据是否能被清理取决于对应文件在上次数据重整之后是否有新的数据写入。
9. `VIRTUAL` 仅对超级表生效。`VIRTUAL 1` 表示创建虚拟超级表，`VIRTUAL 0` 表示创建普通超级表，默认值为 `0`。创建虚拟超级表时，`column_definition` 中只支持 `type_name`，不支持定义额外主键列和压缩选项。虚拟超级表支持通过 `BASE ON` 继承其他虚拟超级表的列和标签，详见 [虚拟超级表继承](./04-virtualtable.md#虚拟超级表继承)。

## 查看超级表

### 显示超级表

```sql
SHOW [db_name.]STABLES [LIKE 'pattern'];
```

显示指定数据库或当前数据库下的所有超级表。可以使用 `LIKE` 对超级表名进行模糊匹配。

### 显示超级表创建语句

```sql
SHOW CREATE STABLE [db_name.]stb_name;
```

常用于数据库迁移。对一个已经存在的超级表，返回其创建语句；在另一个集群中执行该语句，就能得到一个结构完全相同的超级表。

### 获取超级表的结构信息

```sql
DESCRIBE [db_name.]stb_name;
```

### 获取超级表中所有子表的标签信息

```sql
SHOW TABLE TAGS [tag_name [, tag_name] ...] FROM table_name [FROM db_name];
SHOW TABLE TAGS [tag_name [, tag_name] ...] FROM [db_name.]table_name;
```

```sql
taos> SHOW TABLE TAGS FROM st1;
             tbname             |     id      |         loc          |
======================================================================
 st1s1                          |           1 | beijing              |
 st1s2                          |           2 | shanghai             |
 st1s3                          |           3 | guangzhou            |
Query OK, 3 rows in database (0.004455s)
```

返回结果集的第一列为子表名，后续列为标签列。

如果已经知道标签列名称，也可以使用 `SELECT DISTINCT` 获取指定标签列的值。

```sql
taos> SELECT DISTINCT TBNAME, id FROM st1;
             tbname             |     id      |
===============================================
 st1s1                          |           1 |
 st1s2                          |           2 |
 st1s3                          |           3 |
Query OK, 3 rows in database (0.002891s)
```

`SELECT` 语句中的 `DISTINCT` 和 `TBNAME` 都是必不可少的，TDengine 会根据它们对语句进行优化，使之在没有数据或数据非常多的情况下都可以正确并快速地返回标签值。

### 获取某个子表的标签信息

```sql
taos> SHOW TAGS FROM st1s1;
   table_name    |     db_name     |   stable_name   |    tag_name     |    tag_type     |    tag_value    |
============================================================================================================
 st1s1           | test            | st1             | id              | INT             | 1               |
 st1s1           | test            | st1             | loc             | VARCHAR(20)     | beijing         |
Query OK, 2 rows in database (0.003684s)
```

同样，也可以用 `SELECT` 语句查询指定标签列的值。

```sql
taos> SELECT DISTINCT TBNAME, id, loc FROM st1s1;
     tbname      |     id      |       loc       |
==================================================
 st1s1           |           1 | beijing         |
Query OK, 1 rows in database (0.001884s)
```

## 删除超级表

```sql
DROP STABLE [IF EXISTS] [db_name.]stb_name;
```

删除超级表会自动删除通过该超级表创建的子表以及子表中的所有数据。

**注意**：删除超级表并不会立即释放该表所占用的磁盘空间，而是把该表的数据标记为已删除，在查询时这些数据将不会再出现，但释放磁盘空间会延迟到系统自动或用户手动进行数据重整时。

## 修改超级表

```sql
ALTER STABLE [db_name.]stb_name alter_table_clause

alter_table_clause: {
    alter_table_options
  | ADD COLUMN col_name column_type
  | DROP COLUMN col_name
  | MODIFY COLUMN col_name column_type
  | ADD TAG tag_name tag_type
  | DROP TAG tag_name
  | MODIFY TAG tag_name tag_type
  | RENAME TAG old_tag_name new_tag_name
  | ADD BASE ON [db_name.]parent_stb_name [, [db_name.]parent_stb_name] ...
  | DROP BASE ON [db_name.]parent_stb_name [, [db_name.]parent_stb_name] ...
}

alter_table_options:
    alter_table_option ...

alter_table_option: {
    COMMENT 'string_value'
  | KEEP value
}

```

**使用说明**

修改超级表的结构会对其下的所有子表生效。无法针对某个特定子表修改表结构。标签结构的修改需要对超级表下发，TDengine 会自动作用于此超级表的所有子表。

- `ADD COLUMN`：添加数据列。
- `DROP COLUMN`：删除数据列。
- `MODIFY COLUMN`：修改数据列宽度。数据列类型必须是可变长类型，如 `BINARY`、`VARCHAR` 或 `NCHAR`；列宽只能改大，不能改小。
- `ADD TAG`：添加标签。
- `DROP TAG`：删除标签。从超级表删除某个标签后，该超级表下的所有子表也会自动删除该标签。
- `MODIFY TAG`：修改标签列宽度。标签类型必须是可变长类型，如 `BINARY`、`VARCHAR` 或 `NCHAR`；列宽只能改大，不能改小。
- `RENAME TAG`：修改标签名称。从超级表修改某个标签名后，该超级表下的所有子表也会自动更新该标签名。
- `ADD BASE ON` / `DROP BASE ON`：修改虚拟超级表的继承关系，详见 [虚拟超级表继承](./04-virtualtable.md#修改继承关系)。
- 与普通表一样，超级表的主键列不允许被修改，也不允许通过 `ADD COLUMN` 或 `DROP COLUMN` 添加、删除主键列。

### 增加列

```sql
ALTER STABLE stb_name ADD COLUMN col_name column_type;
```

### 删除列

```sql
ALTER STABLE stb_name DROP COLUMN col_name;
```

### 修改列宽

```sql
ALTER STABLE stb_name MODIFY COLUMN col_name data_type(length);
```

如果数据列的类型是可变长类型（如 `BINARY`、`VARCHAR` 或 `NCHAR`），可以使用此语句修改其宽度。列宽只能改大，不能改小。

### 添加标签

```sql
ALTER STABLE stb_name ADD TAG tag_name tag_type;
```

为超级表增加一个新的标签，并指定新标签的类型。标签总数不能超过 `128` 个，一个表中标签值的总长度不超过 16 KB。

### 删除标签

```sql
ALTER STABLE stb_name DROP TAG tag_name;
```

删除超级表的一个标签，从超级表删除某个标签后，该超级表下的所有子表也会自动删除该标签。

### 修改标签名

```sql
ALTER STABLE stb_name RENAME TAG old_tag_name new_tag_name;
```

修改超级表的标签名，从超级表修改某个标签名后，该超级表下的所有子表也会自动更新该标签名。

### 修改标签列宽度

```sql
ALTER STABLE stb_name MODIFY TAG tag_name data_type(length);
```

如果标签类型是可变长类型（如 `BINARY`、`VARCHAR` 或 `NCHAR`），可以使用此语句修改其宽度。列宽只能改大，不能改小。

### 超级表查询

使用 `SELECT` 语句可以完成在超级表上的投影及聚合查询，在 `WHERE` 子句中可以对标签和数据列进行筛选。

如果在超级表查询语句中不加 `ORDER BY`，返回顺序是先返回一个子表的所有数据，然后再返回下一个子表的所有数据，因此返回的数据是无序的。增加 `ORDER BY` 后，会按 `ORDER BY` 指定的顺序返回。

:::note
除了更新标签值的操作针对子表进行外，其他标签操作（添加标签、删除标签等）均只能作用于超级表，不能对单个子表操作。对超级表添加标签后，基于该超级表创建的所有子表都会自动增加该标签，新增标签的默认值为 `NULL`。

:::
