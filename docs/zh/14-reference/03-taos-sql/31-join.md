---
sidebar_label: 关联查询
title: 关联查询
description: 关联查询详细描述
---

## Join 概念

### 驱动表

驱动关联查询进行的表，在 Left Join 系列中左表为驱动表，在 Right Join 系列中右表为驱动表。

### 连接条件

连接条件是指进行表关联所指定的条件，TDengine 支持的所有关联查询都需要指定连接条件，连接条件通常（Inner Join 和 Window Join 例外）只出现在 `ON` 之后。根据语义，Inner Join 中出现在 `WHERE` 之后的条件也可以视作连接条件，而 Window Join 是通过 `WINDOW_OFFSET` 来指定连接条件。

  除 ASOF Join 外，TDengine 支持的所有 Join 类型都必须显式指定连接条件，ASOF Join 因为默认定义有隐式的连接条件，所以（在默认条件可以满足需求的情况下）可以不必显式指定连接条件。

除 ASOF/Window Join 外，连接条件中除了包含主连接条件外，还可以包含任意多条其他连接条件，主连接条件与其他连接条件间必须是 `AND` 关系，而其他连接条件之间则没有这个限制。其他连接条件中可以包含主键列、Tag 、普通列、常量及其标量函数或运算的任意逻辑运算组合。

以智能电表为例，下面这几条 SQL 都包含合法的连接条件：

```sql
SELECT a.* FROM meters a LEFT JOIN meters b ON a.ts = b.ts AND a.ts > '2023-10-18 10:00:00.000';
SELECT a.* FROM meters a LEFT JOIN meters b ON a.ts = b.ts AND (a.ts > '2023-10-18 10:00:00.000' OR a.ts < '2023-10-17 10:00:00.000');
SELECT a.* FROM meters a LEFT JOIN meters b ON timetruncate(a.ts, 1s) = timetruncate(b.ts, 1s) AND (a.ts + 1s > '2023-10-18 10:00:00.000' OR a.groupId > 0);
SELECT a.* FROM meters a LEFT ASOF JOIN meters b ON timetruncate(a.ts, 1s) < timetruncate(b.ts, 1s) AND a.groupId = b.groupId;
```

### 主连接条件

作为一款时序数据库，TDengine 所有的关联查询都围绕主键时戳列进行，因此要求除 ASOF/Window Join 外的所有关联查询都必须含有主键列的等值连接条件，而按照顺序首次出现在连接条件中的主键列等值连接条件将会被作为主连接条件。ASOF Join 的主连接条件可以包含非等值的连接条件，而 Window Join 的主连接条件则是通过 `WINDOW_OFFSET` 来指定。

除 Window Join 外，TDengine 支持在主连接条件中进行 `timetruncate` 函数操作，例如 `ON timetruncate(a.ts, 1s) = timetruncate(b.ts, 1s)`，除此之外，暂不支持其他函数及标量运算。

### 分组条件

时序数据库特色的 ASOF/Window Join 支持对关联查询的输入数据进行分组，然后每个分组内进行关联操作。分组只对关联查询的输入进行，输出结果将不包含分组信息。ASOF/Window Join 中出现在 `ON` 之后的等值条件（ASOF 的主连接条件除外）将被作为分组条件。

### 主键时间线

TDengine 作为时序数据库要求每个表（子表）中必须有主键时间戳列，它将作为该表的主键时间线进行很多跟时间相关的运算，而子查询的结果或者 Join 运算的结果中也需要明确哪一列将被视作主键时间线参与后续的时间相关的运算。在子查询中，查询结果中存在的有序的第一个出现的主键列（或其运算）或等同主键列的伪列（`_wstart`/`_wend`）将被视作该输出表的主键时间线。Join 输出结果中主键时间线的选择遵从以下规则：
- Left/Right Join 系列中驱动表（子查询）的主键列将被作为后续查询的主键时间线；此外，在 Window Join 窗口内，因为左右表同时有序所以在窗口内可以把任意一个表的主键列做作主键时间线，优先选择本表的主键列作为主键时间线。
- Inner Join 可以把任意一个表的主键列做作主键时间线，当存在类似分组条件（Tag 列的等值条件且与主连接条件 `AND` 关系）时将无法产生主键时间线。
- Full Join 因为无法产生任何一个有效的主键时间序列，因此没有主键时间线，这也就意味着 Full Join 中无法进行时间线相关的运算。


## 语法说明

在接下来的章节中会通过共用的方式同时介绍 Left/Right Join 系列，因此后续的包括 Outer、Semi、Anti-Semi、ASOF、Window 系列介绍中都采用了类似 "left/right" 的写法来同时进行 Left/Right Join 的介绍。这里简要介绍这种写法的含义，写在 "/" 前面的表示应用于 Left Join，而写在 "/" 后面的表示应用于 Right Join。

举例说明：

"左/右表" 表示对 Left Join 来说，它指的是"左表"，对 Right Join 来说，它指的是“右表”；

同理，

"右/左表" 表示对 Left Join 来说，它指的是"右表"，对 Right Join 来说，它指的是“左表”；


## Join 功能

### Inner Join

#### 定义
内连接 - 只有左右表中同时符合连接条件的数据才会被返回，可以视为两个表符合连接条件的数据的交集。

#### 语法
```sql
SELECT ... FROM table_name1 [INNER] JOIN table_name2 [ON ...] [WHERE ...] [...]
或
SELECT ... FROM table_name1, table_name2 WHERE ... [...]
```
#### 结果集
符合连接条件的左右表行数据的笛卡尔积集合。

#### 适用范围
支持超级表、普通表、子表、子查询间 Inner Join。

#### 说明
- 对于第一种语法，`INNER` 关键字可选， `ON` 和/或 `WHERE` 中可以指定主连接条件和其他连接条件，`WHERE` 中还可以指定过滤条件，`ON`/`WHERE` 两者至少指定一个。
- 对于第二种语法，可以在 `WHERE` 中指定主连接条件、其他连接条件、过滤条件。
- 对超级表进行 Inner Join 时，与主连接条件 `AND` 关系的 Tag 列等值条件将作为类似分组条件使用，因此输出结果不能保持有序。

#### 示例

表 d1001 和表 d1002 中同时出现电压大于 220V 的时刻及各自的电压值：
```sql
SELECT a.ts, a.voltage, b.voltage FROM d1001 a JOIN d1002 b ON a.ts = b.ts and a.voltage > 220 and b.voltage > 220
```


### Left/Right Outer Join

#### 定义
左/右（外）连接 - 既包含左右表同时符合连接条件的数据集合，也包括左/右表中不符合连接条件的数据集合。

#### 语法
```sql
SELECT ... FROM table_name1 LEFT|RIGHT [OUTER] JOIN table_name2 ON ... [WHERE ...] [...]
```

#### 结果集
Inner Join 的结果集 + 左/右表中不符合连接条件的行和右/左表的空数据（`NULL`）组成的行数据集合。

#### 适用范围
支持超级表、普通表、子表、子查询间 Left/Right Join。

#### 说明
- OUTER 关键字可选。

#### 示例

表 d1001 所有时刻的电压值以及和表 d1002 中同时出现电压大于 220V 的时刻及各自的电压值：
```sql
SELECT a.ts, a.voltage, b.voltage FROM d1001 a LEFT JOIN d1002 b ON a.ts = b.ts and a.voltage > 220 and b.voltage > 220
```

### Left/Right Semi Join

#### 定义
左/右半连接 - 通常表达的是 `IN``/EXISTS` 的含义，即对左/右表任意一条数据来说，只有当右/左表中存在任一符合连接条件的数据时才返回左/右表行数据。

#### 语法
```sql
SELECT ... FROM table_name1 LEFT|RIGHT SEMI JOIN table_name2 ON ... [WHERE ...] [...]
```

#### 结果集
左/右表中符合连接条件的行和右/左表任一符合连接条件的行组成的行数据集合。

#### 适用范围
支持超级表、普通表、子表、子查询间 Left/Right Semi Join。

#### 示例

表 d1001 中出现电压大于 220V 且存在其他电表同一时刻电压也大于 220V 的时间：
```sql
SELECT a.ts FROM d1001 a LEFT SEMI JOIN meters b ON a.ts = b.ts and a.voltage > 220 and b.voltage > 220 and b.tbname != 'd1001'
```

### Left/Right Anti-Semi Join

#### 定义
左/右反连接 - 同左/右半连接的逻辑正好相反，通常表达的是 `NOT IN`/`NOT EXISTS` 的含义，即对左/右表任意一条数据来说，只有当右/左表中不存在任何符合连接条件的数据时才返回左/右表行数据。

#### 语法
```sql
SELECT ... FROM table_name1 LEFT|RIGHT ANTI JOIN table_name2 ON ... [WHERE ...] [...]
```

#### 结果集
左/右表中不符合连接条件的行和右/左表的空数据（`NULL`）组成的行数据集合。

#### 适用范围
支持超级表、普通表、子表、子查询间 Left/Right Anti-Semi Join。

#### 示例

表 d1001 中出现电压大于 220V 且不存在其他电表同一时刻电压也大于 220V 的时间：
```sql
SELECT a.ts FROM d1001 a LEFT ANTI JOIN meters b ON a.ts = b.ts and b.voltage > 220 and b.tbname != 'd1001' WHERE a.voltage > 220
```

### left/Right ASOF Join

#### 定义
左/右不完全匹配连接 - 不同于其他传统 Join 的完全匹配模式，ASOF Join 允许以指定的匹配模式进行不完全匹配，即按照主键时间戳最接近的方式进行匹配。

#### 语法
```sql
SELECT ... FROM table_name1 LEFT|RIGHT ASOF JOIN table_name2 [ON ...] [JLIMIT jlimit_num] [WHERE ...] [...]
```

##### 结果集
左/右表中每一行数据与右/左表中符合连接条件的按主键列排序后时间戳最接近的最多 `jlimit_num` 条数据或空数据（`NULL`）的笛卡尔积集合。

##### 适用范围
支持超级表、普通表、子表间 Left/Right ASOF Join。

#### 说明
- 只支持表间 ASOF Join，不支持子查询间 ASOF Join。
- ON 子句中支持指定主键列或主键列的 timetruncate 函数运算（不支持其他标量运算及函数）后的单个匹配规则（主连接条件），支持的运算符及其含义如下：


  |    **运算符**   |       **Left ASOF 时含义**       |
  | :-------------: | ------------------------ |
  | &gt;    | 匹配右表中主键时间戳小于左表主键时间戳且时间戳最接近的数据行      |
  | &gt;=    | 匹配右表中主键时间戳小于等于左表主键时间戳且时间戳最接近的数据行  |
  | =    | 匹配右表中主键时间戳等于左表主键时间戳的行  |
  | &lt;    | 匹配右表中主键时间戳大于左表主键时间戳且时间戳最接近的数据行  |
  | &lt;=    | 匹配右表中主键时间戳大于等于左表主键时间戳且时间戳最接近的数据行  |

  对于 Right ASOF 来说，上述运算符含义正好相反。

- 如果不含 `ON` 子句或 `ON` 子句中未指定主键列的匹配规则，则默认主键匹配规则运算符是 “>=”， 即（对 Left ASOF Join 来说）右表中主键时戳小于等于左表主键时戳的行数据。不支持多个主连接条件。
- `ON` 子句中还可以指定除主键列外的 Tag、普通列（不支持标量函数及运算）之间的等值条件用于分组计算，除此之外不支持其他类型的条件。
- 所有 ON 条件间只支持 `AND` 运算。
- `JLIMIT` 用于指定单行匹配结果的最大行数，可选，未指定时默认值为1，即左/右表每行数据最多从右/左表中获得一行匹配结果。`JLIMIT` 取值范围为 [0, 1024]。符合匹配条件的 `jlimit_num` 条数据不要求时间戳相同，当右/左表中不存在满足条件的 `jlimit_num` 条数据时，返回的结果行数可能小于 `jlimit_num`；当右/左表中存在符合条件的多于 `jlimit_num` 条数据时，如果时间戳相同将随机返回 `jlimit_num` 条数据。

#### 示例

表 d1001 电压值大于 220V 且表 d1002 中同一时刻或稍早前最后时刻出现电压大于 220V 的时间及各自的电压值：
```sql
SELECT a.ts, a.voltage, b.ts, b.voltage FROM d1001 a LEFT ASOF JOIN d1002 b ON a.ts >= b.ts where a.voltage > 220 and b.voltage > 220 
```

### Left/Right Window Join

#### 定义
左/右窗口连接 - 根据左/右表中每一行的主键时间戳和窗口边界构造窗口并据此进行窗口连接，支持窗口内进行投影、标量和聚合操作。

#### 语法
```sql
SELECT ... FROM table_name1 LEFT|RIGHT WINDOW JOIN table_name2 [ON ...] WINDOW_OFFSET(start_offset, end_offset) [JLIMIT jlimit_num] [WHERE ...] [...]
```

#### 结果集
左/右表中每一行数据与右/左表中基于左/右表主键时戳列和 `WINDOW_OFFSET` 划分的窗口内的至多 `jlimit_num` 条数据或空数据（`NULL`）的笛卡尔积集合 或 
左/右表中每一行数据与右/左表中基于左/右表主键时戳列和 `WINDOW_OFFSET` 划分的窗口内的至多 `jlimit_num` 条数据的聚合结果或空数据（`NULL`）组成的行数据集合。

#### 适用范围
支持超级表、普通表、子表间 Left/Right Window Join。

#### 说明
- 只支持表间 Window Join，不支持子查询间 Window Join；
- `ON` 子句可选，只支持指定除主键列外的 Tag、普通列（不支持标量函数及运算）之间的等值条件用于分组计算，所有条件间只支持 `AND` 运算；
- `WINDOW_OFFSET` 用于指定窗口的左右边界相对于左/右表主键时间戳的偏移量，支持自带时间单位的形式，例如：`WINDOW_OFFSET(-1a， 1a)`，对于 Left Window Join 来说，表示每个窗口为 [左表主键时间戳 - 1毫秒，左表主键时间戳 + 1毫秒] ，左右边界均为闭区间。数字后面的时间单位可以是 `b`（纳秒）、`u`（微秒）、`a`（毫秒）、`s`（秒）、`m`（分）、`h`（小时）、`d`（天）、`w`（周），不支持自然月（`n`）、自然年（`y`），支持的最小时间单位为数据库精度，左右表所在数据库精度需保持一致。
- `JLIMIT` 用于指定单个窗口内的最大匹配行数，可选，未指定时默认获取每个窗口内的所有匹配行。`JLIMIT` 取值范围为 [0, 1024]，当右表中不存在满足条件的 `jlimit_num` 条数据时，返回的结果行数可能小于 `jlimit_num`；当右表中存在超过 `jlimit_num` 条满足条件的数据时，优先返回窗口内主键时间戳最小的 `jlimit_num` 条数据。
- SQL 语句中不能含其他 `GROUP BY`/`PARTITION BY`/窗口查询；
- 支持在 `WHERE` 子句中进行标量过滤，支持在 `HAVING` 子句中针对每个窗口进行聚合函数过滤（不支持标量过滤），不支持 `SLIMIT`，不支持各种窗口伪列；

#### 示例

表 d1001 电压值大于 220V 时前后1秒的区间内表 d1002 的电压值：
```sql
SELECT a.ts, a.voltage, b.voltage FROM d1001 a LEFT WINDOW JOIN d1002 b WINDOW_OFFSET（-1s, 1s) where a.voltage > 220
```

表 d1001 电压值大于 220V 且前后1秒的区间内表 d1002 的电压平均值也大于 220V 的时间及电压值：
```sql
SELECT a.ts, a.voltage, avg(b.voltage) FROM d1001 a LEFT WINDOW JOIN d1002 b WINDOW_OFFSET（-1s, 1s) where a.voltage > 220 HAVING(avg(b.voltage) > 220)
```

### Full Outer Join

#### 定义
全（外）连接 - 既包含左右表同时符合连接条件的数据集合，也包括左右表中不符合连接条件的数据集合。

#### 语法
SELECT ... FROM table_name1 FULL [OUTER] JOIN table_name2 ON ... [WHERE ...] [...]

#### 结果集
Inner Join 的结果集 + 左表中不符合连接条件的行加上右表的空数据组成的行数据集合 + 右表中不符合连接条件的行加上左表的空数据(`NULL`)组成的行数据集合。

#### 适用范围
支持超级表、普通表、子表、子查询间 Full Outer Join。

#### 说明
- OUTER 关键字可选。

#### 示例

表 d1001 和表 d1002 中记录的所有时刻及电压值：
```sql
SELECT a.ts, a.voltage, b.ts, b.voltage FROM d1001 a FULL JOIN d1002 b on a.ts = b.ts
```

## 约束和限制

### 输入时间线限制
- 目前所有 Join 都要求输入数据含有效的主键时间线，所有表查询都可以满足，子查询需要注意输出数据是否含有效的主键时间线。

### 连接条件限制
- 除 ASOF 和 Window Join 之外，其他 Join 的连接条件中必须含主键列的主连接条件； 且
- 主连接条件与其他连接条件间只支持 `AND` 运算；
- 作为主连接条件的主键列只支持 `timetruncate` 函数运算（不支持其他函数和标量运算），作为其他连接条件时无限制；

### 分组条件限制
- 只支持除主键列外的 Tag、普通列的等值条件；
- 不支持标量运算；
- 支持多个分组条件，条件间只支持 `AND` 运算；

### 查询结果顺序限制
- 普通表、子表、子查询且无分组条件无排序的场景下，查询结果会按照驱动表的主键列顺序输出；
- 超级表查询、Full Join或有分组条件无排序的场景下，查询结果没有固定的输出顺序；
因此，在有排序需求且输出无固定顺序的场景下，需要进行排序操作。部分依赖时间线的函数可能会因为没有有效的时间线输出而无法执行。

### 嵌套 Join 与多表 Join 限制
- 目前除 Inner Join 支持嵌套与多表 Join 外，其他类型的 JoiN 暂不支持嵌套与多表 Join。
