---
sidebar_label: 关联查询
title: 关联查询
description: 关联查询（JOIN）概念、类型、语法与限制
---

## JOIN 概念

### 驱动表

驱动关联查询的表。在 Left Join 系列中，左表为驱动表；在 Right Join 系列中，右表为驱动表。

### 连接条件

连接条件指进行表关联时所指定的条件。TDengine 支持的所有关联查询都需要指定连接条件；连接条件通常（`INNER JOIN` 和 `WINDOW JOIN` 例外）只出现在 `ON` 之后。按语义，`INNER JOIN` 中出现在 `WHERE` 之后的条件也可视作连接条件；`WINDOW JOIN` 通过 `WINDOW_OFFSET` 指定连接条件。

除 `ASOF JOIN` 外，TDengine 支持的所有 Join 类型都必须显式指定连接条件。`ASOF JOIN` 默认带有隐式连接条件，在默认条件可满足需求时可不显式指定。

除 `ASOF JOIN` / `WINDOW JOIN` 外，连接条件除主连接条件外，还可包含任意多条其他连接条件。主连接条件与其他连接条件之间必须是 `AND` 关系，其他连接条件之间则无此限制。其他连接条件可包含主键列、标签、普通列、常量，以及它们的标量函数或运算的任意逻辑组合。

以智能电表为例，以下 SQL 均包含合法的连接条件：

```sql
SELECT a.* FROM meters a LEFT JOIN meters b ON a.ts = b.ts AND a.ts > '2023-10-18 10:00:00.000';
SELECT a.* FROM meters a LEFT JOIN meters b ON a.ts = b.ts AND (a.ts > '2023-10-18 10:00:00.000' OR a.ts < '2023-10-17 10:00:00.000');
SELECT a.* FROM meters a LEFT JOIN meters b ON TIMETRUNCATE(a.ts, 1s) = TIMETRUNCATE(b.ts, 1s) AND (a.ts + 1s > '2023-10-18 10:00:00.000' OR a.groupId > 0);
SELECT a.* FROM meters a LEFT ASOF JOIN meters b ON TIMETRUNCATE(a.ts, 1s) < TIMETRUNCATE(b.ts, 1s) AND a.groupId = b.groupId;
```

### 主连接条件

作为时序数据库，TDengine 的关联查询都围绕主键时间戳列进行。因此除 `ASOF JOIN` / `WINDOW JOIN` 外，所有关联查询都必须包含主键列的等值连接条件；连接条件中按顺序首次出现的主键列等值条件将作为主连接条件。`ASOF JOIN` 的主连接条件可以是非等值条件；`WINDOW JOIN` 的主连接条件通过 `WINDOW_OFFSET` 指定。

自 `v3.3.6.0` 起，子查询中的常量（含返回时间戳的常量函数如 `TODAY()`、`NOW()`，以及常量时间戳及其加减运算）可作为等价主键列出现在主连接条件中。例如：

```sql
SELECT * FROM d1001 a JOIN (SELECT TODAY() AS ts1, * FROM d1002 WHERE ts = '2025-03-19 10:00:00.000') b ON TIMETRUNCATE(a.ts, 1d) = b.ts1;
```

上面的语句可将表 `d1001` 当天的全部记录与表 `d1002` 中某一时刻的某条记录做关联。需要注意：SQL 中的时间字符串常量默认不会被当作时间戳，例如 `'2025-03-19 10:00:00.000'` 只会被当作字符串。若需按常量时间戳处理，可用类型前缀 `TIMESTAMP` 指定，例如：

```sql
SELECT * FROM d1001 a JOIN (SELECT TIMESTAMP '2025-03-19 10:00:00.000' AS ts1, * FROM d1002 WHERE ts = '2025-03-19 10:00:00.000') b ON TIMETRUNCATE(a.ts, 1d) = b.ts1;
```

除 `WINDOW JOIN` 外，主连接条件中支持对主键列使用 `TIMETRUNCATE`，例如 `ON TIMETRUNCATE(a.ts, 1s) = TIMETRUNCATE(b.ts, 1s)`；暂不支持其他函数及标量运算。

### 分组条件

时序场景中的 `ASOF JOIN` / `WINDOW JOIN` 支持对输入数据分组，再在每个分组内做关联。分组只作用于关联查询的输入，输出结果不包含分组信息。`ASOF JOIN` / `WINDOW JOIN` 中出现在 `ON` 之后的等值条件（`ASOF JOIN` 的主连接条件除外）将作为分组条件。

### 主键时间线

TDengine 要求每张表（子表）必须有主键时间戳列，作为该表的主键时间线，参与大量与时间相关的运算。子查询结果或 Join 结果中也需要明确哪一列作为主键时间线，以参与后续时间相关运算。

在子查询中，结果里有序且首次出现的主键列（或其运算），或等同主键列的伪列（`_wstart` / `_wend`），将作为该输出表的主键时间线。自 `v3.3.6.0` 起，子查询结果中的常量时间戳列也可作为输出表的主键时间线。Join 输出结果中主键时间线的选择规则如下：

- Left/Right Join 系列：驱动表（子查询）的主键列作为后续查询的主键时间线。此外，在 `WINDOW JOIN` 窗口内，左右表同时有序，窗口内可将任一方主键列作为主键时间线，并优先选择本表主键列。
- `INNER JOIN`：可将任一方主键列作为主键时间线；若存在类似分组条件（标签列等值条件，且与主连接条件为 `AND` 关系），则无法产生主键时间线。
- `FULL JOIN`：无法产生有效的主键时间序列，因此没有主键时间线，也就无法进行依赖时间线的运算。

## 语法说明

后续章节会同时介绍 Left/Right Join 系列，因此 Outer、Semi、Anti-Semi、ASOF、Window 等说明中采用类似“左/右”的写法。写在 `/` 前面的适用于 Left Join，写在 `/` 后面的适用于 Right Join。

例如：

- “左/右表”：对 Left Join 指左表，对 Right Join 指右表。
- “右/左表”：对 Left Join 指右表，对 Right Join 指左表。

## JOIN 功能

### INNER JOIN

#### 定义

内连接：只有左右表中同时符合连接条件的数据才会返回，可视为两表符合连接条件数据的交集。

#### 语法

```sql
SELECT ... FROM table_name1 [INNER] JOIN table_name2 [ON ...] [WHERE ...] [...]
或
SELECT ... FROM table_name1, table_name2 WHERE ... [...]
```

#### 结果集

符合连接条件的左右表行数据的笛卡尔积集合。

#### 适用范围

支持超级表、普通表、子表、子查询之间的 `INNER JOIN`。

#### 说明

- 第一种语法中，`INNER` 关键字可选；可在 `ON` 和/或 `WHERE` 中指定主连接条件和其他连接条件，`WHERE` 中还可指定过滤条件；`ON` / `WHERE` 至少指定其一。
- 第二种语法中，可在 `WHERE` 中指定主连接条件、其他连接条件和过滤条件。
- 对超级表做 `INNER JOIN` 时，与主连接条件为 `AND` 关系的标签列等值条件会作为类似分组条件使用，因此输出结果不能保持有序。

#### 示例

表 `d1001` 与表 `d1002` 中同时出现电压大于 220V 的时刻及各自的电压值：

```sql
SELECT a.ts, a.voltage, b.voltage FROM d1001 a JOIN d1002 b ON a.ts = b.ts AND a.voltage > 220 AND b.voltage > 220;
```

### Left/Right Outer Join

#### 定义

左/右（外）连接：既包含左右表同时符合连接条件的数据，也包含左/右表中不符合连接条件的数据。

#### 语法

```sql
SELECT ... FROM table_name1 LEFT|RIGHT [OUTER] JOIN table_name2 ON ... [WHERE ...] [...]
```

#### 结果集

`INNER JOIN` 的结果集，加上左/右表中不符合连接条件的行与右/左表空值（`NULL`）组成的行集合。

#### 适用范围

支持超级表、普通表、子表、子查询之间的 Left/Right Outer Join。

#### 说明

- `OUTER` 关键字可选。

#### 示例

表 `d1001` 全部时刻的电压值，以及与表 `d1002` 中同时出现电压大于 220V 的时刻及各自的电压值：

```sql
SELECT a.ts, a.voltage, b.voltage FROM d1001 a LEFT JOIN d1002 b ON a.ts = b.ts AND a.voltage > 220 AND b.voltage > 220;
```

### Left/Right Semi Join

#### 定义

左/右半连接：通常表达 `IN` / `EXISTS` 的语义。对左/右表任意一行，仅当右/左表中存在任一符合连接条件的数据时，才返回该左/右表行。

#### 语法

```sql
SELECT ... FROM table_name1 LEFT|RIGHT SEMI JOIN table_name2 ON ... [WHERE ...] [...]
```

#### 结果集

左/右表中符合连接条件的行，与右/左表任一符合连接条件的行组成的行集合。

#### 适用范围

支持超级表、普通表、子表、子查询之间的 Left/Right Semi Join。

#### 示例

表 `d1001` 中电压大于 220V，且存在其他电表同一时刻电压也大于 220V 的时间：

```sql
SELECT a.ts FROM d1001 a LEFT SEMI JOIN meters b ON a.ts = b.ts AND a.voltage > 220 AND b.voltage > 220 AND b.tbname != 'd1001';
```

### Left/Right Anti-Semi Join

#### 定义

左/右反连接：与左/右半连接逻辑相反，通常表达 `NOT IN` / `NOT EXISTS` 的语义。对左/右表任意一行，仅当右/左表中不存在任何符合连接条件的数据时，才返回该左/右表行。

#### 语法

```sql
SELECT ... FROM table_name1 LEFT|RIGHT ANTI JOIN table_name2 ON ... [WHERE ...] [...]
```

#### 结果集

左/右表中不符合连接条件的行，与右/左表空值（`NULL`）组成的行集合。

#### 适用范围

支持超级表、普通表、子表、子查询之间的 Left/Right Anti-Semi Join。

#### 示例

表 `d1001` 中电压大于 220V，且不存在其他电表同一时刻电压也大于 220V 的时间：

```sql
SELECT a.ts FROM d1001 a LEFT ANTI JOIN meters b ON a.ts = b.ts AND b.voltage > 220 AND b.tbname != 'd1001' WHERE a.voltage > 220;
```

### Left/Right ASOF Join

#### 定义

左/右不完全匹配连接：不同于传统 Join 的完全匹配，`ASOF JOIN` 允许按指定匹配模式做不完全匹配，即按主键时间戳最接近的方式匹配。

#### 语法

```sql
SELECT ... FROM table_name1 LEFT|RIGHT ASOF JOIN table_name2 [ON ...] [JLIMIT jlimit_num] [WHERE ...] [...]
```

#### 结果集

左/右表中每一行，与右/左表中符合连接条件、按主键列排序后时间戳最接近的至多 `jlimit_num` 条数据（或不存在时的 `NULL`）的笛卡尔积集合。

#### 适用范围

支持超级表、普通表、子表之间的 Left/Right ASOF Join。

#### 说明

- 只支持表之间的 `ASOF JOIN`，不支持子查询之间的 `ASOF JOIN`。
- `ON` 子句中支持指定主键列，或主键列经 `TIMETRUNCATE` 运算后的单个匹配规则（主连接条件）；不支持其他标量运算及函数。支持的运算符及其含义如下：

  | 运算符 | Left ASOF 时含义 |
  | ---- | --- |
  | `>`  | 匹配右表中主键时间戳小于左表主键时间戳，且时间戳最接近的数据行 |
  | `>=` | 匹配右表中主键时间戳小于等于左表主键时间戳，且时间戳最接近的数据行 |
  | `=`  | 匹配右表中主键时间戳等于左表主键时间戳的行 |
  | `<`  | 匹配右表中主键时间戳大于左表主键时间戳，且时间戳最接近的数据行 |
  | `<=` | 匹配右表中主键时间戳大于等于左表主键时间戳，且时间戳最接近的数据行 |

  对 Right ASOF 来说，上述运算符含义正好相反。

- 若不含 `ON` 子句，或 `ON` 子句中未指定主键列匹配规则，则默认主键匹配运算符为 `>=`，即（对 Left ASOF Join）匹配右表中主键时间戳小于等于左表主键时间戳的行。不支持多个主连接条件。
- `ON` 子句还可指定除主键列外的标签、普通列（不支持标量函数及运算）之间的等值条件，用于分组计算；除此之外不支持其他类型条件。
- 所有 `ON` 条件之间只支持 `AND`。
- `JLIMIT` 用于指定单行匹配结果的最大行数，可选；未指定时默认值为 `1`，即左/右表每行最多从右/左表获得一行匹配结果。`JLIMIT` 取值范围为 `[0, 1024]`。符合条件的至多 `jlimit_num` 条数据不要求时间戳相同；若右/左表中不足 `jlimit_num` 条，返回行数可能更少；若多于 `jlimit_num` 条且时间戳相同，则随机返回 `jlimit_num` 条。

#### 示例

表 `d1001` 电压大于 220V，且表 `d1002` 在同一时刻或稍早前最后时刻也出现电压大于 220V 的时间及各自电压值：

```sql
SELECT a.ts, a.voltage, b.ts, b.voltage FROM d1001 a LEFT ASOF JOIN d1002 b ON a.ts >= b.ts WHERE a.voltage > 220 AND b.voltage > 220;
```

### Left/Right Window Join

#### 定义

左/右窗口连接：根据左/右表每一行的主键时间戳和窗口边界构造窗口并据此连接，支持在窗口内做投影、标量和聚合。

#### 语法

```sql
SELECT ... FROM table_name1 LEFT|RIGHT WINDOW JOIN table_name2 [ON ...] WINDOW_OFFSET(start_offset, end_offset) [JLIMIT jlimit_num] [WHERE ...] [...]
```

#### 结果集

以下两种之一：

- 左/右表每一行，与右/左表中按左/右表主键时间戳和 `WINDOW_OFFSET` 划分的窗口内至多 `jlimit_num` 条数据（或不存在时的 `NULL`）的笛卡尔积集合。
- 左/右表每一行，与上述窗口内至多 `jlimit_num` 条数据的聚合结果（或不存在时的 `NULL`）组成的行集合。

#### 适用范围

支持超级表、普通表、子表之间的 Left/Right Window Join。

#### 说明

- 只支持表之间的 `WINDOW JOIN`，不支持子查询之间的 `WINDOW JOIN`。
- `ON` 子句可选，只支持指定除主键列外的标签、普通列（不支持标量函数及运算）之间的等值条件用于分组；所有条件之间只支持 `AND`。
- `WINDOW_OFFSET` 用于指定窗口左右边界相对左/右表主键时间戳的偏移量，支持自带时间单位的写法，例如 `WINDOW_OFFSET(-1a, 1a)`。对 Left Window Join，表示每个窗口为 `[左表主键时间戳 - 1 毫秒, 左表主键时间戳 + 1 毫秒]`，左右边界均为闭区间。数字后的时间单位可为 `b`（纳秒）、`u`（微秒）、`a`（毫秒）、`s`（秒）、`m`（分）、`h`（小时）、`d`（天）、`w`（周）；不支持自然月（`n`）、自然年（`y`）。支持的最小时间单位为数据库精度，左右表所在数据库精度需一致。
- `JLIMIT` 用于指定单个窗口内的最大匹配行数，可选；未指定时默认获取每个窗口内的全部匹配行。`JLIMIT` 取值范围为 `[0, 1024]`。若右表不足 `jlimit_num` 条，返回行数可能更少；若超过 `jlimit_num` 条，优先返回窗口内主键时间戳最小的 `jlimit_num` 条。
- SQL 中不能再含其他 `GROUP BY` / `PARTITION BY` / 窗口查询。
- 支持在 `WHERE` 中做标量过滤；支持在 `HAVING` 中针对每个窗口做聚合函数过滤（不支持标量过滤）；不支持 `SLIMIT`；不支持各种窗口伪列。

#### 示例

表 `d1001` 电压大于 220V 时，前后 1 秒区间内表 `d1002` 的电压值：

```sql
SELECT a.ts, a.voltage, b.voltage FROM d1001 a LEFT WINDOW JOIN d1002 b WINDOW_OFFSET(-1s, 1s) WHERE a.voltage > 220;
```

表 `d1001` 电压大于 220V，且前后 1 秒区间内表 `d1002` 的电压平均值也大于 220V 的时间及电压值：

```sql
SELECT a.ts, a.voltage, AVG(b.voltage) FROM d1001 a LEFT WINDOW JOIN d1002 b WINDOW_OFFSET(-1s, 1s) WHERE a.voltage > 220 HAVING (AVG(b.voltage) > 220);
```

### Full Outer Join

#### 定义

全（外）连接：既包含左右表同时符合连接条件的数据，也包含左右表中不符合连接条件的数据。

#### 语法

```sql
SELECT ... FROM table_name1 FULL [OUTER] JOIN table_name2 ON ... [WHERE ...] [...]
```

#### 结果集

`INNER JOIN` 的结果集，加上：

- 左表中不符合连接条件的行与右表空值（`NULL`）组成的行集合；
- 右表中不符合连接条件的行与左表空值（`NULL`）组成的行集合。

#### 适用范围

支持超级表、普通表、子表、子查询之间的 Full Outer Join。

#### 说明

- `OUTER` 关键字可选。

#### 示例

表 `d1001` 与表 `d1002` 中记录的全部时刻及电压值：

```sql
SELECT a.ts, a.voltage, b.ts, b.voltage FROM d1001 a FULL JOIN d1002 b ON a.ts = b.ts;
```

## 约束和限制

### 输入时间线限制

- 目前所有 Join 都要求输入数据含有效的主键时间线。表查询通常可满足；子查询需注意输出是否含有效主键时间线。

### 连接条件限制

- 除 `ASOF JOIN` 和 `WINDOW JOIN` 外，其他 Join 的连接条件中必须含主键列的主连接条件。
- 主连接条件与其他连接条件之间只支持 `AND`。
- 作为主连接条件的主键列只支持 `TIMETRUNCATE`（不支持其他函数和标量运算）；作为其他连接条件时无此限制。

### 分组条件限制

- 只支持除主键列外的标签、普通列的等值条件。
- 不支持标量运算。
- 支持多个分组条件，条件之间只支持 `AND`。

### 查询结果顺序限制

- 普通表、子表、子查询，且无分组条件、无排序时，查询结果按驱动表主键列顺序输出。
- 超级表查询、`FULL JOIN`，或有分组条件、无排序时，查询结果没有固定输出顺序。

因此，在有排序需求且输出无固定顺序的场景下，需要显式排序。部分依赖时间线的函数可能因没有有效时间线输出而无法执行。

### 嵌套 JOIN 与多表 JOIN 限制

- 目前除 `INNER JOIN` 支持嵌套与多表 Join 外，其他类型的 Join 暂不支持嵌套与多表 Join。
