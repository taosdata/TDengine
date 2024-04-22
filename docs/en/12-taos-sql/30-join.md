---
sidebar_label: JOIN
title: JOIN
description: JOIN Description
---

## Join Concept

### Driving table

The table used for driving JOIN queries which is the left table in the Left Join series and the right table in the Right Join series.

### Join Conditions

Join conditions refer to the conditions specified for JOIN operation. All JOIN queries supported by TDengine require specifying join conditions. Join conditions usually only appear in `ON` (except for Inner Join and Window Join). For Inner Join, conditions that appear in `WHERE` can also be regarded as join conditions. For Window Join join conditions are specified in `WINDOW_OFFSET`.

 Except for ASOF Join, all join types supported by TDengine must explicitly specify join conditions. Since ASOF Join has implicit join conditions defined by default, it is not necessary to explicitly specify the join conditions (if the default conditions meet the requirements).

Except for ASOF/Window Join, the join condition can include not only the primary join condition(refer below), but also any number of other join conditions. The primary join condition must have an `AND` relationship with the other join conditions, while there is no such restriction between the other join conditions. The other join conditions can include any logical operation combination of primary key columns, TAG, normal columns, constants, and their scalar functions or operations.


Taking smart meters as an example, the following SQL statements all contain valid join conditions:

```sql
SELECT a.* FROM meters a LEFT JOIN meters b ON a.ts = b.ts AND a.ts > '2023-10-18 10:00:00.000';
SELECT a.* FROM meters a LEFT JOIN meters b ON a.ts = b.ts AND (a.ts > '2023-10-18 10:00:00.000' OR a.ts < '2023-10-17 10:00:00.000');
SELECT a.* FROM meters a LEFT JOIN meters b ON timetruncate(a.ts, 1s) = timetruncate(b.ts, 1s) AND (a.ts + 1s > '2023-10-18 10:00:00.000' OR a.groupId > 0);
SELECT a.* FROM meters a LEFT ASOF JOIN meters b ON timetruncate(a.ts, 1s) < timetruncate(b.ts, 1s) AND a.groupId = b.groupId;
```

### Primary Join Condition

As a time series database, all join queries of TDengine revolve around the primary timestamp column, so all join queries except ASOF/Window Join are required to contain equivalent join condition of the primary key column. The equivalent join condition of the primary key column that first appear in the join conditions in order will be used as the primary join condition. The primary join condition of ASOF Join can contain non-equivalent join condition, for Window Join the primary join condition is specified by `WINDOW_OFFSET`.

Except for Window Join, TDengine supports performing timetruncate function operation in the primary join condition, e.g. `ON timetruncate (a.ts, 1s) = timetruncate (b.ts, 1s)`. Other functions and scalar operations are not currently supported.

### Grouping Conditions

ASOF/Window Join supports grouping the input data of join queries, and then performing join operations within each group. Grouping only applies to the input of join queries, and the output result will not include grouping information. Equivalent conditions that appear in `ON` in ASOF/Window Join (excluding the primary join condition of ASOF) will be used as grouping conditions.


### Primary Key Timeline

TDengine, as a time series database, requires that each table must have a primary key timestamp column, which will perform many time-related operations as the primary key timeline of the table. It is also necessary to clarify which column will be regarded as the primary key timeline for subsequent time-related operations in the results of subqueries or Join operations. In subqueries, the ordered first occurrence of the primary key column (or its operation) or the pseudo-column equivalent to the primary key column (_wstart/_wend) in the query results will be regarded as the primary key timeline of the output table. The selection of the primary key timeline in the Join output results follows the following rules:

- The primary key column of the driving table (subquery) in the Left/Right Join series will be used as the primary key timeline for subsequent queries. In addition, in the Window Join window, because the left and right tables are ordered at the same time, the primary key column of any table can be used as the primary key timeline in the window, and the primary key column of current table is preferentially selected as the primary key timeline.

- The primary key column of any table in Inner Join can be treated as the primary key timeline. When there are similar grouping conditions (equivalent conditions of TAG columns and `AND` relationship with the primary join condition), there will be no available primary key timeline.

- Full Join will not result in any primary key timeline because it cannot generate any valid primary key time series, so no timeline-related operations cannot be performed in Full Join.


## Join Function

### Inner Join

#### Meaning
Only data from both left and right tables that meet the join conditions will be returned, which can be regarded as the intersection of data from two tables that meet the join conditions.

#### Grammar
```sql
SELECT ... FROM table_name1 [INNER] JOIN table_name2 [ON ...] [WHERE ...] [...]
Or
SELECT ... FROM table_name1, table_name2 WHERE ... [...]
```
#### Result set
Cartesian product set of left and right table row data that meets the join conditions.

#### Scope
Inner Join are supported between super tables, normal tables, child tables, and subqueries.

#### Notes
- For the first type syntax, the `INNER` keyword is optional. The primary join condition and other join conditions can be specified in `ON` and/or `WHERE`, and filters can also be specified in `WHERE`. At least one of `ON/WHERE` must be specified.
- For the second type syntax, all primary join condition, other join conditions, and filters can be specified in `WHERE`.
- When performing Inner Join on the super table, the Tag column equivalent conditions with the `AND` relationship of the primary join condition will be used as a similar grouping condition, so the output result cannot remain ordered.

#### Examples

The timestamp when the voltage is greater than 220V occurs simultaneously in Table d1001 and Table d1002 and their respective voltage values:
```sql
SELECT a.ts, a.voltage, b.voltage FROM d1001 a JOIN d1002 b ON a.ts = b.ts and a.voltage > 220 and b.voltage > 220
```


### Left/Right Outer Join

#### 含义
左/右（外）连接 - 既包含左右表同时符合连接条件的数据集合，也包括左/右表中不符合连接条件的数据集合。

#### 语法
```sql
SELECT ... FROM table_name1 LEFT|RIGHT [OUTER] JOIN table_name2 ON ... [WHERE ...] [...]
```

#### 结果集
Inner Join 的结果集 + 左/右表中不符合连接条件的行和右/左表的空数据（NULL）组成的行数据集合。

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

#### 含义
左/右半连接 - 通常表达的是 IN/EXISTS 的含义，即对左/右表任意一条数据来说，只有当右/左表中存在任一符合连接条件的数据时才返回左/右表行数据。

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

#### 含义
左/右反连接 - 同左/右半连接的逻辑正好相反，通常表达的是 NOT IN/NOT EXISTS 的含义，即对左/右表任意一条数据来说，只有当右/左表中不存在任何符合连接条件的数据时才返回左/右表行数据。

#### 语法
```sql
SELECT ... FROM table_name1 LEFT|RIGHT ANTI JOIN table_name2 ON ... [WHERE ...] [...]
```

#### 结果集
左表中不符合连接条件的行和右表的空数据（NULL）组成的行数据集合。

#### 适用范围
支持超级表、普通表、子表、子查询间 Left Anti-Semi Join。

#### 示例

表 d1001 中出现电压大于 220V 且不存在其他电表同一时刻电压也大于 220V 的时间：
```sql
SELECT a.ts FROM d1001 a LEFT ANTI JOIN meters b ON a.ts = b.ts and b.voltage > 220 and b.tbname != 'd1001' WHERE a.voltage > 220
```

### left/Right ASOF Join

#### 含义
左/右不完全匹配连接 - 不同于其他传统 Join 的完全匹配模式，ASOF Join 允许以指定的匹配模式进行不完全匹配，即按照主键时间戳最接近的方式进行匹配。

#### 语法
```sql
SELECT ... FROM table_name1 LEFT|RIGHT ASOF JOIN table_name2 [ON ...] [JLIMIT jlimit_num] [WHERE ...] [...]
```

##### 结果集
左/右表中每一行数据与右/左表中符合连接条件的按主键列排序后时间戳最接近的最多 jlimit_num 条数据或空数据（NULL）的笛卡尔积集合。

##### 适用范围
支持超级表、普通表、子表间 Left/Right ASOF Join。

#### 说明
- 只支持表间 ASOF Join，不支持子查询间 ASOF Join。
- ON 子句中支持指定主键列或主键列的 timetruncate 函数运算（不支持其他标量运算及函数）后的单个匹配规则（主连接条件），支持的运算符及其含义如下：


  |    **运算符**   |       **Left ASOF 时含义**       |
  | :-------------: | ------------------------ |
  | >    | 匹配右表中主键时间戳小于左表主键时间戳且时间戳最接近的数据行      |
  | >=    | 匹配右表中主键时间戳小于等于左表主键时间戳且时间戳最接近的数据行  |
  | =    | 匹配右表中主键时间戳等于左表主键时间戳的行  |
  | <    | 匹配右表中主键时间戳大于左表主键时间戳且时间戳最接近的数据行  |
  | <=    | 匹配右表中主键时间戳大于等于左表主键时间戳且时间戳最接近的数据行  |

  对于 Right ASOF 来说，上述运算符含义正好相反。

- 如果不含 ON 子句或 ON 子句中未指定主键列的匹配规则，则默认主键匹配规则运算符是 “>=”， 即（对 Left ASOF Join 来说）右表中主键时戳小于等于左表主键时戳的行数据。不支持多个主连接条件。
- ON 子句中还可以指定除主键列外的 TAG、普通列（不支持标量函数及运算）之间的等值条件用于分组计算，除此之外不支持其他类型的条件。
- 所有 ON 条件间只支持 AND 运算。
- JLIMIT 用于指定单行匹配结果的最大行数，可选，未指定时默认值为1，即左/右表每行数据最多从右/左表中获得一行匹配结果。JLIMIT 取值范围为 [0, 1024]。符合匹配条件的 jlimit_num 条数据不要求时间戳相同，当右/左表中不存在满足条件的 jlimit_num 条数据时，返回的结果行数可能小于 jlimit_num；当右/左表中存在符合条件的多于 jlimit_num 条数据时，如果时间戳相同将随机返回 jlimit_num 条数据。

#### 示例

表 d1001 电压值大于 220V 且表 d1002 中同一时刻或稍早前最后时刻出现电压大于 220V 的时间及各自的电压值：
```sql
SELECT a.ts, a.voltage, a.ts, b.voltage FROM d1001 a LEFT ASOF JOIN d1002 b ON a.ts >= b.ts where a.voltage > 220 and b.voltage > 220 
```

### Left/Right Window Join

#### 含义
左/右窗口连接 - 根据左/右表中每一行的主键时间戳和窗口边界构造窗口并据此进行窗口连接，支持窗口内进行投影、标量和聚合操作。

#### 语法
```sql
SELECT ... FROM table_name1 LEFT|RIGHT WINDOW JOIN table_name2 [ON ...] WINDOW_OFFSET(start_offset, end_offset) [JLIMIT jlimit_num] [WHERE ...] [...]
```

#### 结果集
左/右表中每一行数据与右/左表中基于左/右表主键时戳列和 WINDOW_OFFSET 划分的窗口内的至多 jlimit_num 条数据或空数据（NULL）的笛卡尔积集合 或 
左/右表中每一行数据与右/左表中基于左/右表主键时戳列和 WINDOW_OFFSET 划分的窗口内的至多 jlimit_num 条数据的聚合结果或空数据（NULL）组成的行数据集合。

#### 适用范围
支持超级表、普通表、子表间 Left/Right Window Join。

#### 说明
- 只支持表间 Window Join，不支持子查询间 Window Join；
- ON 子句可选，只支持指定除主键列外的 TAG、普通列（不支持标量函数及运算）之间的等值条件用于分组计算，所有条件间只支持 AND 运算；
- WINDOW_OFFSET 用于指定窗口的左右边界相对于左/右表主键时间戳的偏移量，支持自带时间单位的形式，例如：WINDOW_OFFSET(-1a， 1a)，对于 Left Window Join 来说，表示每个窗口为 [左表主键时间戳 - 1毫秒，左表主键时间戳 + 1毫秒] ，左右边界均为闭区间。数字后面的时间单位可以是 b（纳秒）、u（微秒）、a（毫秒）、s（秒）、m（分）、h（小时）、d（天）、w（周），不支持自然月（n）、自然年（y），支持的最小时间单位为数据库精度，左右表所在数据库精度需保持一致。
- JLIMIT 用于指定单个窗口内的最大匹配行数，可选，未指定时默认获取每个窗口内的所有匹配行。JLIMIT 取值范围为 [0, 1024]，当右表中不存在满足条件的 jlimit_num 条数据时，返回的结果行数可能小于 jlimit_num；当右表中存在超过 jlimit_num 条满足条件的数据时，优先返回窗口内主键时间戳最小的 jlimit_num 条数据。
- SQL 语句中不能含其他 GROUP BY/PARTITION BY/窗口查询；
- 支持在 WHERE 子句中进行标量过滤，支持在 HAVING 子句中针对每个窗口进行聚合函数过滤（不支持标量过滤），不支持 SLIMIT，不支持各种窗口伪列；

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

#### 含义
全（外）连接 - 既包含左右表同时符合连接条件的数据集合，也包括左右表中不符合连接条件的数据集合。

#### 语法
SELECT ... FROM table_name1 FULL [OUTER] JOIN table_name2 ON ... [WHERE ...] [...]

#### 结果集
Inner Join 的结果集 + 左表中不符合连接条件的行加上右表的空数据组成的行数据集合 + 右表中不符合连接条件的行加上左表的空数据组成的行数据集合。

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
- 主连接条件与其他连接条件间只支持 AND 运算；
- 作为主连接条件的主键列只支持 timetruncate 函数运算（不支持其他函数和标量运算），作为其他连接条件时无限制；

### 分组条件限制
- 只支持除主键列外的 TAG、普通列的等值条件；
- 不支持标量运算；
- 支持多个分组条件，条件间只支持 AND 运算；

### 查询结果顺序限制
- 普通表、子表、子查询且无分组条件无排序的场景下，查询结果会按照驱动表的主键列顺序输出；
- 超级表查询、Full Join或有分组条件无排序的场景下，查询结果没有固定的输出顺序；
因此，在有排序需求且输出无固定顺序的场景下，需要进行排序操作。部分依赖时间线的函数可能会因为没有有效的时间线输出而无法执行。

### 嵌套 Join 与多表 Join 限制
- 目前除 Inner Join 支持嵌套与多表 Join 外，其他类型的 JoiN 暂不支持嵌套与多表 Join。