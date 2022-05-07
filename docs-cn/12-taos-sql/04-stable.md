---
sidebar_label: 超级表管理
title: 超级表 STable 管理
---

:::note

在 2.0.15.0 及以后的版本中开始支持 STABLE 保留字。也即，在本节后文的指令说明中，CREATE、DROP、ALTER 三个指令在 2.0.15.0 之前的版本中 STABLE 保留字需写作 TABLE。

:::

## 创建超级表

```
CREATE STABLE [IF NOT EXISTS] stb_name (timestamp_field_name TIMESTAMP, field1_name data_type1 [, field2_name data_type2 ...]) TAGS (tag1_name tag_type1, tag2_name tag_type2 [, tag3_name tag_type3]);
```

创建 STable，与创建表的 SQL 语法相似，但需要指定 TAGS 字段的名称和类型。

:::info

1. TAGS 列的数据类型不能是 timestamp 类型；（从 2.1.3.0 版本开始，TAGS 列中支持使用 timestamp 类型，但需注意在 TAGS 中的 timestamp 列写入数据时需要提供给定值，而暂不支持四则运算，例如 `NOW + 10s` 这类表达式）
2. TAGS 列名不能与其他列名相同；
3. TAGS 列名不能为预留关键字（参见：[参数限制与保留关键字](/taos-sql/keywords/) 章节）；
4. TAGS 最多允许 128 个，至少 1 个，总长度不超过 16 KB。

:::

## 删除超级表

```
DROP STABLE [IF EXISTS] stb_name;
```

删除 STable 会自动删除通过 STable 创建的子表。

## 显示当前数据库下的所有超级表信息

```
SHOW STABLES [LIKE tb_name_wildcard];
```

查看数据库内全部 STable，及其相关信息，包括 STable 的名称、创建时间、列数量、标签（TAG）数量、通过该 STable 建表的数量。

## 显示一个超级表的创建语句

```
SHOW CREATE STABLE stb_name;
```

常用于数据库迁移。对一个已经存在的超级表，返回其创建语句；在另一个集群中执行该语句，就能得到一个结构完全相同的超级表。

## 获取超级表的结构信息

```
DESCRIBE stb_name;
```

## 修改超级表普通列

### 超级表增加列

```
ALTER STABLE stb_name ADD COLUMN field_name data_type;
```

### 超级表删除列

```
ALTER STABLE stb_name DROP COLUMN field_name;
```

### 超级表修改列宽

```
ALTER STABLE stb_name MODIFY COLUMN field_name data_type(length);
```

如果数据列的类型是可变长格式（BINARY 或 NCHAR），那么可以使用此指令修改其宽度（只能改大，不能改小）。（2.1.3.0 版本新增）

## 修改超级表标签列

### 添加标签

```
ALTER STABLE stb_name ADD TAG new_tag_name tag_type;
```

为 STable 增加一个新的标签，并指定新标签的类型。标签总数不能超过 128 个，总长度不超过 16k 个字符。

### 删除标签

```
ALTER STABLE stb_name DROP TAG tag_name;
```

删除超级表的一个标签，从超级表删除某个标签后，该超级表下的所有子表也会自动删除该标签。

### 修改标签名

```
ALTER STABLE stb_name CHANGE TAG old_tag_name new_tag_name;
```

修改超级表的标签名，从超级表修改某个标签名后，该超级表下的所有子表也会自动更新该标签名。

### 修改标签列宽度

```
ALTER STABLE stb_name MODIFY TAG tag_name data_type(length);
```

如果标签的类型是可变长格式（BINARY 或 NCHAR），那么可以使用此指令修改其宽度（只能改大，不能改小）。（2.1.3.0 版本新增）

### 超级表查询
使用 SELECT 语句可以完成在超级表上的投影及聚合两类查询，在 WHERE 语句中可以对标签及列进行筛选及过滤。

如果在超级表查询语句中不加 ORDER BY, 返回顺序是先返回一个子表的所有数据，然后再返回下个子表的所有数据，所以返回的数据是无序的。如果增加了 ORDER BY 语句，会严格按 ORDER BY 语句指定的顺序返回的。



:::note
除了更新标签的值的操作是针对子表进行，其他所有的标签操作（添加标签、删除标签等）均只能作用于 STable，不能对单个子表操作。对 STable 添加标签以后，依托于该 STable 建立的所有表将自动增加了一个标签，所有新增标签的默认值都是 NULL。

:::
