---
title: TDengine SQL
sidebar_label: TDengine SQL
description: TDengine SQL 语法、数据类型、写入、查询、函数和常用限制说明
---

本文档说明 TDengine SQL 支持的语法规则、数据类型、数据定义、数据写入、数据查询、函数和常用限制等内容。阅读本文档需要具备基本的 SQL 语言基础。如果你需要从 2.x 迁移到 3.x，请参见 [3.0 版本语法变更](11-appendix/04-changes.md) 了解废弃语法和替代写法。

TDengine SQL 是用户对 TDengine 进行数据写入和查询的主要工具。TDengine SQL 以标准 SQL 为基础，并针对时序数据和业务特点扩展了许多语法和功能。TDengine SQL 语句的最大长度默认是 4 MB，可通过客户端参数 `maxSQLLength` 配置，取值范围为 1 MB 到 64 MB。TDengine SQL 不支持关键字的缩写，例如 `DELETE` 不能缩写为 `DEL`。

本章节 SQL 语法遵循如下约定：

- 用大写字母表示关键字，但 SQL 本身并不区分关键字和标识符的大小写
- 用小写字母表示需要用户输入的内容
- \[ \] 表示内容为可选项，但不能输入 [] 本身
- | 表示多选一，选择其中一个即可，但不能输入 | 本身
- … 表示前面的项可重复多个

为更好地说明 SQL 语法的规则及其特点，本文假设存在一个数据集。以智能电表（meters）为例，假设每个智能电表采集电流、电压、相位三个量。其建模如下：

```sql
taos> DESCRIBE meters;
  Field    | Type      | Length | Note |
=========================================
  ts       | TIMESTAMP |      8 |      |
  current  | FLOAT     |      4 |      |
  voltage  | INT       |      4 |      |
  phase    | FLOAT     |      4 |      |
  location | BINARY    |     64 | TAG  |
  groupid  | INT       |      4 | TAG  |
```

数据集包含 4 个智能电表的数据，按照 TDengine 的建模规则，对应 4 个子表，其名称分别是 `d1001`、`d1002`、`d1003`、`d1004`。本章示例统一使用 `groupid` 作为分组标签名。
