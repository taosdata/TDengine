---
title: TAOS SQL
description: "TAOS SQL 支持的语法规则、主要查询功能、支持的 SQL 查询函数，以及常用技巧等内容"
---

本文档说明 TAOS SQL 支持的语法规则、主要查询功能、支持的 SQL 查询函数，以及常用技巧等内容。阅读本文档需要读者具有基本的 SQL 语言的基础。

TAOS SQL 是用户对 TDengine 进行数据写入和查询的主要工具。TAOS SQL 为了便于用户快速上手，在一定程度上提供与标准 SQL 类似的风格和模式。严格意义上，TAOS SQL 并不是也不试图提供标准的 SQL 语法。此外，由于 TDengine 针对的时序性结构化数据不提供删除功能，因此在 TAO SQL 中不提供数据删除的相关功能。

TAOS SQL 不支持关键字的缩写，例如 DESCRIBE 不能缩写为 DESC。

本章节 SQL 语法遵循如下约定：

- <\> 里的内容是用户需要输入的，但不要输入 <\> 本身
- \[ \] 表示内容为可选项，但不能输入 [] 本身
- | 表示多选一，选择其中一个即可，但不能输入 | 本身
- … 表示前面的项可重复多个

为更好地说明 SQL 语法的规则及其特点，本文假设存在一个数据集。以智能电表(meters)为例，假设每个智能电表采集电流、电压、相位三个量。其建模如下：

```
taos> DESCRIBE meters;
             Field              |        Type        |   Length    |    Note    |
=================================================================================
 ts                             | TIMESTAMP          |           8 |            |
 current                        | FLOAT              |           4 |            |
 voltage                        | INT                |           4 |            |
 phase                          | FLOAT              |           4 |            |
 location                       | BINARY             |          64 | TAG        |
 groupid                        | INT                |           4 | TAG        |
```

数据集包含 4 个智能电表的数据，按照 TDengine 的建模规则，对应 4 个子表，其名称分别是 d1001, d1002, d1003, d1004。

```mdx-code-block
import DocCardList from '@theme/DocCardList';
import {useCurrentSidebarCategory} from '@docusaurus/theme-common';

<DocCardList items={useCurrentSidebarCategory().items}/>
```