---
title: 表管理
---

## 创建数据表

```
CREATE TABLE [IF NOT EXISTS] tb_name (timestamp_field_name TIMESTAMP, field1_name data_type1 [, field2_name data_type2 ...]);
```

:::info 说明

1. 表的第一个字段必须是 TIMESTAMP，并且系统自动将其设为主键；
2. 表名最大长度为 192；
3. 表的每行长度不能超过 16k 个字符;（注意：每个 BINARY/NCHAR 类型的列还会额外占用 2 个字节的存储位置）
4. 子表名只能由字母、数字和下划线组成，且不能以数字开头，不区分大小写
5. 使用数据类型 binary 或 nchar，需指定其最长的字节数，如 binary(20)，表示 20 字节；
6. 为了兼容支持更多形式的表名，TDengine 引入新的转义符 "\`"，可以让表名与关键词不冲突，同时不受限于上述表名称合法性约束检查。但是同样具有长度限制要求。使用转义字符以后，不再对转义字符中的内容进行大小写统一。
   例如：\`aBc\` 和 \`abc\` 是不同的表名，但是 abc 和 aBc 是相同的表名。
   需要注意的是转义字符中的内容必须是可打印字符。
   上述的操作逻辑和约束要求与 MySQL 数据的操作一致。
   从 2.3.0.0 版本开始支持这种方式。

:::

### 以超级表为模板创建数据表

```
CREATE TABLE [IF NOT EXISTS] tb_name USING stb_name TAGS (tag_value1, ...);
```

以指定的超级表为模板，指定 TAGS 的值来创建数据表。

### 以超级表为模板创建数据表，并指定具体的 TAGS 列

```
CREATE TABLE [IF NOT EXISTS] tb_name USING stb_name (tag_name1, ...) TAGS (tag_value1, ...);
```

以指定的超级表为模板，指定一部分 TAGS 列的值来创建数据表（没被指定的 TAGS 列会设为空值）。
说明：从 2.0.17.0 版本开始支持这种方式。在之前的版本中，不允许指定 TAGS 列，而必须显式给出所有 TAGS 列的取值。

### 批量创建数据表

```
CREATE TABLE [IF NOT EXISTS] tb_name1 USING stb_name TAGS (tag_value1, ...) [IF NOT EXISTS] tb_name2 USING stb_name TAGS (tag_value2, ...) ...;
```

以更快的速度批量创建大量数据表（服务器端 2.0.14 及以上版本）。

:::info

1.批量建表方式要求数据表必须以超级表为模板。 2.在不超出 SQL 语句长度限制的前提下，单条语句中的建表数量建议控制在 1000 ～ 3000 之间，将会获得比较理想的建表速度。

:::

## 删除数据表

```
DROP TABLE [IF EXISTS] tb_name;
```

## 显示当前数据库下的所有数据表信息

```
SHOW TABLES [LIKE tb_name_wildchar];
```

显示当前数据库下的所有数据表信息。

## 显示一个数据表的创建语句

```
SHOW CREATE TABLE tb_name;
```

常用于数据库迁移。对一个已经存在的数据表，返回其创建语句；在另一个集群中执行该语句，就能得到一个结构完全相同的数据表。

## 获取表的结构信息

```
DESCRIBE tb_name;
```

## 修改表定义

### 表增加列

```
ALTER TABLE tb_name ADD COLUMN field_name data_type;
```

:::info

1. 列的最大个数为 1024，最小个数为 2；（从 2.1.7.0 版本开始，改为最多允许 4096 列）
2. 列名最大长度为 64。

:::

### 表删除列

```
ALTER TABLE tb_name DROP COLUMN field_name;
```

如果表是通过超级表创建，更改表结构的操作只能对超级表进行。同时针对超级表的结构更改对所有通过该结构创建的表生效。对于不是通过超级表创建的表，可以直接修改表结构。

### 表修改列宽

```
ALTER TABLE tb_name MODIFY COLUMN field_name data_type(length);
```

如果数据列的类型是可变长格式（BINARY 或 NCHAR），那么可以使用此指令修改其宽度（只能改大，不能改小）。（2.1.3.0 版本新增）
如果表是通过超级表创建，更改表结构的操作只能对超级表进行。同时针对超级表的结构更改对所有通过该结构创建的表生效。对于不是通过超级表创建的表，可以直接修改表结构。

### 修改子表标签值

```
ALTER TABLE tb_name SET TAG tag_name=new_tag_value;
```

如果表是通过超级表创建，可以使用此指令修改其标签值
