# Join 功能

## 1. 背景

目前 TDengine 只实现了 Inner Join 功能，为了更好的支持客户需求，本次 Join 功能增强将支持除 Cross Join 等时序数据库中基本没有需求场景的 Join 类型外的所有 Join 类型，包括传统库中的 Left Join、Right Join、Full Join、Semi Join、Anti-Semi Join 以及时序库中特色的 ASOF Join、Window Join。
所有拟支持的 Join 组合如下表所示：

|  | None | Semi | Anti-Semi | ASOF | Window |
| --- | --- | --- | --- | --- | --- |
| Inner | 支持 | / | / | / | / |
| Left | 支持 | 支持 | 支持 | 支持 | 支持 |
| Right | 支持 | 支持 | 支持 | 支持 | 支持 |
| Full | 支持 | / | / | / | / |

*注：上表中之所以存在一些不支持的 Join 组合，主要原因是这些 Join 类型（Semi、Anti-Semi、ASOF、Window）是有明确的单向性的，其查询结果也跟这种单向性紧密相关，因此 Inner Join 或 Full Join 与他们的组合将产生矛盾性。以 Semi Join 为例，通常表达的是操作符 IN 的语义，查询结果也通常只有源表的数据，当同时存在反向查询时，这种查询结果将失去意义。因此对于这些组合来说 TDengine 不提供显式支持，如果需要可以通过 SQL 语句的组合来实现，例如通过 UNION 语句获得更精确的双向语义。*
除了上述 Join 支持范围大幅增强外，另一个需求是支持主键列的 timetruncate 函数，具体来说就是支持主键列进行 timetruncate 后进行关联查询。通过这个功能增强，可以将一系列的相关时间戳转换为同一个时间戳，进而在关联查询时可以实现按时间区间 Join 的目的。
本次涉及的所有功能开发和增强在开源版中实现。

## 2. 变更历史

| 日期 | 版本 | 负责人 | 主要修改内容 |
| --- | --- | --- | --- |
| 2024/03/05 | 0.1 | 潘魏 | 初稿 |
|  |  |  |  |
|  |  |  |  |
|  |  |  |  |

## 3. 定义

- 驱动表：驱动关联查询进行的表，在 Left Join 系列中左表为驱动表，在 Right Join 系列中右表为驱动表。
- 主键列：本文中提到的主键列都指的是 TDengine 中第一列，即主键时间戳列，与复合主键新增的主键列无关。
- 等值条件：通过等号（=）运算符指定的运算条件；
- 连接条件：连接条件是指进行表关联所指定的条件，TDengine 支持的所有关联查询都需要指定连接条件，连接条件通常（Inner Join 和 Window Join 例外）只出现在 ON 之后。根据语义，Inner Join 中出现在 WHERE 之后的条件也可以视作连接条件，而 Window Join 是通过 WINDOW_OFFSET 来指定连接条件。
- 过滤条件：过滤条件是指对表关联的结果进行过滤的条件，通过 WHERE 子句进行指定。
- 主连接条件：作为一款时序数据库，TDengine 所有的关联查询都围绕主键时戳列进行，因此要求除 ASOF/Window Join 外的所有关联查询都必须含有主键列的等值连接条件，而按照顺序首次出现在连接条件中的主键列等值连接条件将会被作为主连接条件。ASOF Join 的主连接条件可以包含非等值的连接条件，而 Window Join 的主连接条件则是通过 WINDOW_OFFSET 来指定。
- 分组条件：时序数据库特色的 ASOF/Window Join （其他 Join 不支持）支持对关联查询的输入数据进行分组，然后每个分组内进行关联操作。分组只对关联查询的输入进行，输出结果将不包含分组信息。ASOF/Window Join 中出现在 ON 之后的等值条件（ASOF 的主连接条件除外）将被作为分组条件。

## 4. 行为说明

本次功能增强不涉及配置、部署方式、API 等的变更，主要是通过扩展和增强 SQL 语句的方式来实现新的功能。

### 4.1 连接条件

除 ASOF Join 外，TDengine 支持的所有 Join 类型都必须显式指定连接条件，ASOF Join 因为默认定义有隐式的连接条件，所以（在默认条件可以满足需求的情况下）可以不必显式指定连接条件。
除 ASOF/Window Join 外，连接条件中除了包含主连接条件外，还可以包含任意多条其他连接条件，主连接条件与其他连接条件间必须是 AND 关系，而其他连接条件之间则没有这个限制。其他连接条件中可以包含主键列、TAG 、普通列、常量及其标量函数或运算的任意逻辑运算组合。
以智能电表为例，下面这几条 SQL 都包含合法的连接条件：
```sql {wrap}
SELECT a.* FROM meters a LEFT JOIN meters b ON a.ts = b.ts AND a.ts > '2023-10-18 10:00:00.000';
SELECT a.* FROM meters a LEFT JOIN meters b ON a.ts = b.ts AND (a.ts > '2023-10-18 10:00:00.000' OR a.ts < '2023-10-17 10:00:00.000');
SELECT a.* FROM meters a LEFT JOIN meters b ON timetruncate(a.ts, 1s) = timetruncate(b.ts, 1s) AND (a.ts + 1s > '2023-10-18 10:00:00.000' OR a.groupId > 0);
SELECT a.* FROM meters a LEFT ASOF JOIN meters b ON timetruncate(a.ts, 1s) < timetruncate(b.ts, 1s) AND a.groupId = b.groupId;
```

### 4.2 主键时间线

TDengine 作为时序数据库要求每个表（子表）中必须有主键时间戳列，它将作为该表的主键时间线进行很多跟时间相关的运算，而子查询的结果或者 Join 运算的结果中也需要明确哪一列将被视作主键时间线参与后续的时间相关的运算。在子查询中，查询结果中存在的有序的第一个出现的主键列（或其运算）或等同主键列的伪列（_wstart/_wend）将被视作该输出表的主键时间线；Join 输出结果中主键时间线的选择遵从以下规则：
- Left/Right Join 系列中驱动表（子查询）的主键列将被作为后续查询的主键时间线；此外，在 Window Join 窗口内，因为左右表同时有序所以在窗口内可以把任意一个表的主键列做作主键时间线，优先选择本表的主键列作为主键时间线；
- Inner Join 可以把任意一个表的主键列做作主键时间线，当存在类似分组条件（TAG 列的等值条件且与主连接条件 AND 关系）时将无法产生主键时间线；
- Full Join 因为无法产生任何一个有效的主键时间序列，因此没有主键时间线，这也就意味着 Full Join 中无法进行时间线相关的运算。

#### 4.2.1 示例

以下文 4.3 节所示示例数据为例，部分 SQL 及运行结果如下：

| SQL 语句 | 查询结果 | 说明 |
| --- | --- | --- |
| SELECT diff(a.col1) FROM sta a JOIN sta b ON a.ts = b.ts AND a.tg1 = b.tg1 | 错误 | 因为缺少主键时间线报错 |
|  |  |  |
|  |  |  |


### 4.3 示例数据

这里假设存在超级表 sta 含有两张子表 tba1 和 tba2，各自的行列数据如下：

|  |
|  |
| ts | col1 | tg1(tag) | ts | col1 | tg1(tag) |
| 2023-11-17 16:29:00 | 1 | 1 | 2023-11-17 16:29:00 | 2 | 2 |
| 2023-11-17 16:29:02 | 3 | 1 | 2023-11-17 16:29:01 | 3 | 2 |
| 2023-11-17 16:29:03 | 4 | 1 | 2023-11-17 16:29:03 | 5 | 2 |
| 2023-11-17 16:29:04 | 5 | 1 | 2023-11-17 16:29:05 | 7 | 2 |

后续所有 Join 查询示例以此节示例数据作为输入数据。

### 4.4 Join 功能

#### 4.4.1 Inner Join

##### 4.4.1.1 含义

内连接 - 只有左右表中同时符合连接条件的数据才会被返回，可以视为两个表符合连接条件的数据的交集。

##### 4.4.1.2 语法

```sql
SELECT ... FROM table_name1 [INNER] JOIN table_name2 [ON ...] [WHERE ...] [...]
或
SELECT ... FROM table_name1, table_name2 WHERE ... [...]
```

##### 4.4.1.3 结果集

符合连接条件的左右表行数据的笛卡尔积集合。

##### 4.4.1.4 适用范围

支持超级表、普通表、子表、子查询间 Inner Join。

##### 4.4.1.5 说明

- 对于第一种语法，INNER 关键字可选， ON 和/或 WHERE 中可以指定主连接条件和其他连接条件，WHERE 中还可以指定过滤条件，ON/WHERE 两者至少指定一个；
- 对于第二种语法，可以在 WHERE 中指定主连接条件、其他连接条件、过滤条件；
- 对超级表进行 Inner Join 时，与主连接条件 AND 关系的 Tag 列等值条件将作为类似分组条件使用，因此输出结果不能保持有序。

##### 4.4.1.6 示例

| SQL 语句 | 查询结果 |
| --- | --- |
| SELECT a.col1, b.col1 FROM tba1 a JOIN tba2 b ON a.ts = b.ts | <grid cols="2"> <column width="50"> 1 4 </column> <column width="50"> 2 5 </column> </grid> |
| SELECT a.col1, b.col1 FROM sta a JOIN sta b ON a.ts = b.ts AND a.ts < '2023-11-17 16:29:02' | <grid cols="2"> <column width="50"> 1 1 2 2 3 </column> <column width="50"> 1 2 1 2 3 </column> </grid> |


#### 4.4.2 Left Outer Join

##### 4.4.2.1 含义

左（外）连接 - 既包含左右表同时符合连接条件的数据集合，也包括左表中不符合连接条件的数据集合。

##### 4.4.2.2 语法

```sql
SELECT ... FROM table_name1 LEFT [OUTER] JOIN table_name2 ON ... [WHERE ...] [...]
```

##### 4.4.2.3 结果集

Inner Join 的结果集 + 左表中不符合连接条件的行和右表的空数据（NULL）组成的行数据集合。

##### 4.4.2.4 适用范围

支持超级表、普通表、子表、子查询间 Left Join。

##### 4.4.2.5 说明

- OUTER 关键字可选；

##### 4.4.2.6 示例

| SQL 语句 | 查询结果 |
| --- | --- |
| SELECT a.col1, b.col1 FROM sta a LEFT JOIN sta b ON a.ts = b.ts AND a.ts < '2023-11-17 16:29:02' AND b.ts < '2023-11-17 16:29:01' | <grid cols="2"> <column width="50"> 1 1 2 2 3 3 4 5 5 7 </column> <column width="50"> 1 2 1 2 NULL NULL NULL NULL NULL NULL </column> </grid> |
| SELECT a.col1, b.col1 FROM sta a LEFT JOIN sta b ON a.ts = b.ts WHERE a.ts < '2023-11-17 16:29:02' AND b.ts < '2023-11-17 16:29:01' ORDER BY a.col1, b.col1; | <grid cols="2"> <column width="50"> 1 1 2 2 </column> <column width="50"> 1 2 1 2 </column> </grid> |

#### 4.4.3 Left Semi Join

##### 4.4.3.1 含义

左半连接 - 通常表达的是 IN/EXISTS 的含义，即对左表任意一条数据来说，只有当右表中存在任一符合连接条件的数据时才返回左表行数据。

##### 4.4.3.2 语法

```sql
SELECT ... FROM table_name1 LEFT SEMI JOIN table_name2 ON ... [WHERE ...] [...]
```

##### 4.4.3.3 结果集

左表中符合连接条件的行和右表任一符合连接条件的行组成的行数据集合。

##### 4.4.3.4 适用范围

支持超级表、普通表、子表、子查询间 Left Semi Join。

##### 4.4.3.5 示例

| SQL 语句 | 查询结果 |
| --- | --- |
| SELECT a.ts, b.ts FROM sta a LEFT SEMI JOIN sta b ON a.ts = b.ts AND a.ts < '2023-11-17 16:29:02' | <grid cols="2"> <column width="50"> 2023-11-17 16:29:00 2023-11-17 16:29:00 2023-11-17 16:29:01 </column> <column width="50"> 2023-11-17 16:29:00 2023-11-17 16:29:00 2023-11-17 16:29:01 </column> </grid> |


#### 4.4.4 Left Anti-Semi Join

##### 4.4.4.1 含义

左反连接 - 同左半连接的逻辑正好相反，通常表达的是 NOT IN/NOT EXISTS 的含义，即对左表任意一条数据来说，只有当右表中不存在任何符合连接条件的数据时才返回左表行数据。

##### 4.4.4.2 语法

```sql
SELECT ... FROM table_name1 LEFT ANTI JOIN table_name2 ON ... [WHERE ...] [...]
```

##### 4.4.4.3 结果集

左表中不符合连接条件的行和右表的空数据（NULL）组成的行数据集合。

##### 4.4.4.4 适用范围

支持超级表、普通表、子表、子查询间 Left Anti-Semi Join。

##### 4.4.4.5 示例

| SQL 语句 | 查询结果 |
| --- | --- |
| SELECT a.ts, b.ts FROM tba1 a LEFT ANTI JOIN tba2 b ON a.ts = b.ts | <grid cols="2"> <column width="50"> 2023-11-17 16:29:02 2023-11-17 16:29:04 </column> <column width="50"> NULL NULL </column> </grid> |

#### 4.4.5 Left ASOF Join

##### 4.4.5.1 含义

左不完全匹配连接 - 不同于其他传统 Join 的完全匹配模式，ASOF Join 允许以指定的匹配模式进行不完全匹配，即按照主键时间戳最接近的方式进行匹配。

##### 4.4.5.2 语法

```sql {wrap}
SELECT ... FROM table_name1 LEFT ASOF JOIN table_name2 [ON ...] [JLIMIT jlimit_num] [WHERE ...] [...]
```

##### 4.4.5.3 结果集

左表中每一行数据与右表中符合连接条件的按主键列排序后时间戳最接近的最多 jlimit_num 条数据或空数据（NULL）的笛卡尔积集合。

##### 4.4.5.4 适用范围

支持超级表、普通表、子表间 Left ASOF Join。

##### 4.4.5.5 说明

- 只支持表间 ASOF Join，不支持子查询间 ASOF Join；
- ON 子句中支持指定主键列或主键列的 timetruncate 函数运算（不支持其他标量运算及函数）后的单个匹配规则（主连接条件），支持的运算符及其含义如下：

  | 运算符 | 含义 |
| --- | --- |
| > | 匹配右表中主键时间戳小于左表主键时间戳且时间戳最接近的数据行 |
| >= | 匹配右表中主键时间戳小于等于左表主键时间戳且时间戳最接近的数据行 |
| = | 匹配右表中主键时间戳等于左表主键时间戳的行 |
| < | 匹配右表中主键时间戳大于左表主键时间戳且时间戳最接近的数据行 |
| <= | 匹配右表中主键时间戳大于等于左表主键时间戳且时间戳最接近的数据行 |

- 如果不含 ON 子句或 ON 子句中未指定主键列的匹配规则，则默认主键匹配规则运算符是 “>=”， 即右表中主键时戳小于等于左表主键时戳的行数据。不支持多个主连接条件；
- ON 子句中还可以指定除主键列外的 TAG、普通列（不支持标量函数及运算）之间的等值条件用于分组计算，除此之外不支持其他类型的条件；
- 所有 ON 条件间只支持 AND 运算；
- JLIMIT 用于指定单行匹配结果的最大行数，可选，未指定时默认值为1，即左表每行数据最多从右表中获得一行匹配结果。JLIMIT 取值范围为 [0, 1024]。符合匹配条件的 jlimit_num 条数据不要求时间戳相同，当右表中不存在满足条件的 jlimit_num 条数据时，返回的结果行数可能小于 jlimit_num；当右表中存在符合条件的多于 jlimit_num 条数据时，如果时间戳相同将随机返回 jlimit_num 条数据。

##### 4.4.5.6 示例

| SQL 语句 | 查询结果 |
| --- | --- |
| SELECT a.ts, b.ts FROM tba1 a LEFT ASOF JOIN tba2 b ON a.ts = b.ts | <grid cols="2"> <column width="50"> 2023-11-17 16:29:00 2023-11-17 16:29:02 2023-11-17 16:29:03 2023-11-17 16:29:04 </column> <column width="50"> 2023-11-17 16:29:00 NULL 2023-11-17 16:29:03 NULL </column> </grid> |
| SELECT a.ts, b.ts FROM tba1 a LEFT ASOF JOIN tba2 b ON a.ts <= b.ts | <grid cols="2"> <column width="50"> 2023-11-17 16:29:00 2023-11-17 16:29:02 2023-11-17 16:29:03 2023-11-17 16:29:04 </column> <column width="50"> 2023-11-17 16:29:00 2023-11-17 16:29:03 2023-11-17 16:29:03 2023-11-17 16:29:05 </column> </grid> |
| SELECT a.ts, b.ts FROM tba1 a LEFT ASOF JOIN tba2 b ON a.ts > b.ts JLIMIT 2 | <grid cols="2"> <column width="50"> 2023-11-17 16:29:00 2023-11-17 16:29:02 2023-11-17 16:29:02 2023-11-17 16:29:03 2023-11-17 16:29:03 2023-11-17 16:29:04 2023-11-17 16:29:04 </column> <column width="50"> NULL 2023-11-17 16:29:00 2023-11-17 16:29:01 2023-11-17 16:29:00 2023-11-17 16:29:01 2023-11-17 16:29:01 2023-11-17 16:29:03 </column> </grid> |
| SELECT a.ts, b.ts FROM tba1 a LEFT ASOF JOIN tba2 b | <grid cols="2"> <column width="50"> 2023-11-17 16:29:00 2023-11-17 16:29:02 2023-11-17 16:29:03 2023-11-17 16:29:04 </column> <column width="50"> 2023-11-17 16:29:00 2023-11-17 16:29:01 2023-11-17 16:29:03 2023-11-17 16:29:03 </column> </grid> |
| SELECT a.ts, b.ts FROM tba1 a LEFT ASOF JOIN tba2 b JLIMIT 2 | <grid cols="2"> <column width="50"> 2023-11-17 16:29:00 2023-11-17 16:29:02 2023-11-17 16:29:02 2023-11-17 16:29:03 2023-11-17 16:29:03 2023-11-17 16:29:04 2023-11-17 16:29:04 </column> <column width="50"> 2023-11-17 16:29:00 2023-11-17 16:29:00 2023-11-17 16:29:01 2023-11-17 16:29:01 2023-11-17 16:29:03 2023-11-17 16:29:01 2023-11-17 16:29:03 </column> </grid> |

#### 4.4.6 Left Window Join

##### 4.4.6.1 含义

左窗口连接 - 根据左表中每一行的主键时间戳和窗口边界构造窗口并据此进行窗口连接，支持窗口内进行投影、标量和聚合操作。

##### 4.4.6.2 语法

```sql {wrap}
SELECT ... FROM table_name1 LEFT WINDOW JOIN table_name2 [ON ...] WINDOW_OFFSET(start_offset, end_offset) [JLIMIT jlimit_num] [WHERE ...] [...]
```

##### 4.4.6.3 结果集

左表中每一行数据与右表中基于左表主键时戳列和 WINDOW_OFFSET 划分的窗口内的至多 jlimit_num 条数据或空数据（NULL）的笛卡尔积集合 或 
左表中每一行数据与右表中基于左表主键时戳列和 WINDOW_OFFSET 划分的窗口内的至多 jlimit_num 条数据的聚合结果或空数据（NULL）组成的行数据集合。

##### 4.4.6.4 适用范围

支持超级表、普通表、子表间 Left Window Join。

##### 4.4.6.5 说明

- 只支持表间 Window Join，不支持子查询间 Window Join；
- ON 子句可选，只支持指定除主键列外的 TAG、普通列（不支持标量函数及运算）之间的等值条件用于分组计算，所有条件间只支持 AND 运算；
- WINDOW_OFFSET 用于指定窗口的左右边界相对于左表主键时间戳的偏移量，支持自带时间单位的形式，例如：WINDOW_OFFSET(-1a， 1a)，表示每个窗口是 [左表主键时间戳 - 1毫秒，左表主键时间戳 + 1毫秒] ，左右边界均为闭区间。数字后面的时间单位可以是 b（纳秒）、u（微秒）、a（毫秒）、s（秒）、m（分）、h（小时）、d（天）、w（周），不支持自然月（n）、自然年（y），支持的最小时间单位为数据库精度，左右表所在数据库精度需保持一致。
- JLIMIT 用于指定单个窗口内的最大匹配行数，可选，未指定时默认获取每个窗口内的所有匹配行。JLIMIT 取值范围为 [0, 1024]，当右表中不存在满足条件的 jlimit_num 条数据时，返回的结果行数可能小于 jlimit_num；当右表中存在超过 jlimit_num 条满足条件的数据时，优先返回窗口内主键时间戳最小的 jlimit_num 条数据。
- SQL 语句中不能含其他 GROUP BY/PARTITION BY/窗口查询；
- 支持在 WHERE 子句中进行标量过滤，支持在 HAVING 子句中针对每个窗口进行聚合函数过滤（不支持标量过滤），不支持 SLIMIT，不支持各种窗口伪列；

##### 4.4.6.6 示例

| SQL 语句 | 查询结果 |
| --- | --- |
| SELECT a.ts, b.ts FROM tba1 a LEFT WINDOW JOIN tba2 b WINDOW_OFFSET(-1s, 1s) | <grid cols="2"> <column width="50"> 2023-11-17 16:29:00 2023-11-17 16:29:00 2023-11-17 16:29:02 2023-11-17 16:29:02 2023-11-17 16:29:03 2023-11-17 16:29:04 2023-11-17 16:29:04 </column> <column width="50"> 2023-11-17 16:29:00 2023-11-17 16:29:01 2023-11-17 16:29:01 2023-11-17 16:29:03 2023-11-17 16:29:03 2023-11-17 16:29:03 2023-11-17 16:29:05 </column> </grid> |
| SELECT a.ts, b.ts FROM tba1 a LEFT WINDOW JOIN tba2 b WINDOW_OFFSET(-1s, 1s) JLIMIT 1 | <grid cols="2"> <column width="50"> 2023-11-17 16:29:00 2023-11-17 16:29:02 2023-11-17 16:29:03 2023-11-17 16:29:04 </column> <column width="50"> 2023-11-17 16:29:00 2023-11-17 16:29:01 2023-11-17 16:29:03 2023-11-17 16:29:03 </column> </grid> |
| SELECT a.ts, count(b.*) FROM tba1 a LEFT WINDOW JOIN tba2 b WINDOW_OFFSET(-1s, 1s) | <grid cols="2"> <column width="50"> 2023-11-17 16:29:00 2023-11-17 16:29:02 2023-11-17 16:29:03 2023-11-17 16:29:04 </column> <column width="50"> 2 2 1 2 </column> </grid> |
| SELECT a.ts, count(b.*) FROM tba1 a LEFT WINDOW JOIN tba2 b ON a.col1 = b.col1 WINDOW_OFFSET(-1s, 1s) | <grid cols="2"> <column width="50"> 2023-11-17 16:29:02 2023-11-17 16:29:04 2023-11-17 16:29:00 2023-11-17 16:29:03 </column> <column width="50"> 1 1 0（NULL） 0（NULL） </column> </grid> |
| SELECT a.ts, count(b.*) FROM tba1 a LEFT WINDOW JOIN tba2 b WINDOW_OFFSET(-1s, 1s) HAVING(count(b.*) > 1) | <grid cols="2"> <column width="50"> 2023-11-17 16:29:00 2023-11-17 16:29:02 2023-11-17 16:29:04 </column> <column width="50"> 2 2 2 </column> </grid> |

#### 4.4.7 Right Outer Join

##### 4.4.7.1 含义

右（外）连接 - 既包含左右表同时符合连接条件的数据集合，也包括右表中不符合连接条件的数据集合。

##### 4.4.7.2 语法

```sql
SELECT ... FROM table_name1 RIGHT [OUTER] JOIN table_name2 ON ... [WHERE ...] [...]
```

##### 4.4.7.3 结果集

Inner Join 的结果集 + 右表中不符合连接条件的行和左表的空数据（NULL）组成的行数据集合。

##### 4.4.7.4 适用范围

支持超级表、普通表、子表、子查询间 Right Join。

##### 4.4.7.5 说明

- OUTER 关键字可选；

##### 4.4.7.6 示例

| SQL 语句 | 查询结果 |
| --- | --- |
| SELECT a.col1, b.col1 FROM sta a RIGHT JOIN sta b ON a.ts = b.ts and b.ts < '2023-11-17 16:29:02' AND a.ts < '2023-11-17 16:29:01' | <grid cols="2"> <column width="50"> 1 2 1 2 NULL NULL NULL NULL NULL NULL </column> <column width="50"> 1 1 2 2 3 3 4 5 5 7 </column> </grid> |

#### 4.4.8 Right Semi Join

##### 4.4.8.1 含义

右半连接 - 通常表达的是 IN/EXISTS 的含义，即只有当左表中存在任一符合连接条件的数据时才返回右表行数据。

##### 4.4.8.2 语法

```sql
SELECT ... FROM table_name1 RIGHT SEMI JOIN table_name2 ON ... [WHERE ...] [...]
```

##### 4.4.8.3 结果集

右表中符合连接条件的行和左表任一符合连接条件的行组成的行数据集合。

##### 4.4.8.4 适用范围

支持超级表、普通表、子表、子查询间 Right Semi Join。

##### 4.4.8.5 示例

| SQL 语句 | 查询结果 |
| --- | --- |
| SELECT a.ts, b.ts FROM sta a RIGHT SEMI JOIN sta b ON a.ts = b.ts AND b.ts < '2023-11-17 16:29:02' | <grid cols="2"> <column width="50"> 2023-11-17 16:29:00 2023-11-17 16:29:00 2023-11-17 16:29:01 </column> <column width="50"> 2023-11-17 16:29:00 2023-11-17 16:29:00 2023-11-17 16:29:01 </column> </grid> |

#### 4.4.9 Right Anti-Semi Join

##### 4.4.9.1 含义

右反连接 - 同右半连接的逻辑正好相反，通常表达的是 NOT IN/NOT EXISTS 的含义，即只有当左表中不存在任何符合连接条件的数据时才返回右表行数据。

##### 4.4.9.2 语法

```sql
SELECT ... FROM table_name1 RIGHT ANTI JOIN table_name2 ON ... [WHERE ...] [...]
```

##### 4.4.9.3 结果集

右表中不符合连接条件的行和左表的空数据（NULL）组成的行数据集合。

##### 4.4.9.4 适用范围

支持超级表、普通表、子表、子查询间 Right Anti-Semi Join。

##### 4.4.9.5 示例

| SQL 语句 | 查询结果 |
| --- | --- |
| SELECT a.ts, b.ts FROM tba1 a RIGHT ANTI JOIN tba2 b ON a.ts = b.ts | <grid cols="2"> <column width="50"> NULL NULL </column> <column width="50"> 2023-11-17 16:29:01 2023-11-17 16:29:05 </column> </grid> |

#### 

#### 4.4.10 Right ASOF Join

##### 4.4.10.1 含义

右不完全匹配连接 - 不同于其他传统 Join 的完全匹配模式，ASOF Join 允许以指定的匹配模式进行不完全匹配，即按照主键时间戳最接近的方式进行匹配。

##### 4.4.10.2 语法

```sql {wrap}
SELECT ... FROM table_name1 RIGHT ASOF JOIN table_name2 [ON ...] [JLIMIT jlimit_num] [WHERE ...] [...]
```

##### 4.4.10.3 结果集

右表中每一行数据与左表中符合连接条件的按主键列排序后时间最接近的最多 jlimit_num 条数据或空数据（NULL）的笛卡尔积集合。

##### 4.4.10.4 适用范围

支持超级表、普通表、子表间 Right ASOF Join。

##### 4.4.10.5 说明

- 只支持表间 ASOF Join，不支持子查询间 ASOF Join；
- ON 子句中支持指定主键列或主键列的 timetruncate 函数运算（不支持其他标量运算及函数）后的单个匹配规则（主连接条件），支持的运算符及其含义如下：

  | 运算符 | 含义 |
| --- | --- |
| > | 匹配左表中主键时间戳大于右表主键时间戳且时间戳最接近的数据行 |
| >= | 匹配左表中主键时间戳大于等于右表主键时间戳且时间戳最接近的数据行 |
| = | 匹配左表中主键时间戳等于右表主键时间戳的行 |
| < | 匹配左表中主键时间戳小于右表主键时间戳且时间戳最接近的数据行 |
| <= | 匹配左表中主键时间戳小于等于右表主键时间戳且时间戳最接近的数据行 |

- 如果不含 ON 子句或 ON 子句中未指定主键列的匹配规则，则默认主键匹配规则运算符是 “<=”， 即匹配左表中主键时间戳小于等于右表主键时间戳的行数据。不支持多个主连接条件。
- ON 子句中还可以指定除主键列外的 TAG、普通列（不支持标量函数及运算）之间的等值条件用于分组计算，除此之外不支持其他类型的条件；
- 所有 ON 条件间只支持 AND 运算。
- JLIMIT 用于指定单行匹配结果的最大行数，可选，未指定时默认值为1，即右表每行数据最多从左表中获得一行匹配结果。JLIMIT 取值范围为 [0, 1024]。符合匹配条件的 jlimit_num 条数据不要求时间戳相同，当左表中不存在满足条件的 jlimit_num 条数据时，返回的结果行数可能小于 jlimit_num。

##### 4.4.10.6 示例

| SQL 语句 | 查询结果 |
| --- | --- |
| SELECT a.ts, b.ts FROM tba1 a RIGHT ASOF JOIN tba2 b ON a.ts = b.ts | <grid cols="2"> <column width="50"> 2023-11-17 16:29:00 NULL 2023-11-17 16:29:03 NULL </column> <column width="50"> 2023-11-17 16:29:00 2023-11-17 16:29:01 2023-11-17 16:29:03 2023-11-17 16:29:05 </column> </grid> |
| SELECT a.ts, b.ts FROM tba1 a RIGHT ASOF JOIN tba2 b ON a.ts <= b.ts | <grid cols="2"> <column width="50"> 2023-11-17 16:29:00 2023-11-17 16:29:00 2023-11-17 16:29:03 2023-11-17 16:29:04 </column> <column width="50"> 2023-11-17 16:29:00 2023-11-17 16:29:01 2023-11-17 16:29:03 2023-11-17 16:29:05 </column> </grid> |
| SELECT a.ts, b.ts FROM tba1 a RIGHT ASOF JOIN tba2 b ON a.ts > b.ts JLIMIT 2 | <grid cols="2"> <column width="50"> 2023-11-17 16:29:02 2023-11-17 16:29:03 2023-11-17 16:29:02 2023-11-17 16:29:03 2023-11-17 16:29:04 NULL </column> <column width="50"> 2023-11-17 16:29:00 2023-11-17 16:29:00 2023-11-17 16:29:01 2023-11-17 16:29:01 2023-11-17 16:29:03 2023-11-17 16:29:05 </column> </grid> |


#### 4.4.11 Right Window Join

##### 4.4.11.1 含义

右窗口连接 - 根据右表每一行的主键时间戳和窗口边界构造窗口并据此进行窗口连接，支持窗口内进行投影、标量和聚合操作。

##### 4.4.11.2 语法

```sql {wrap}
SELECT ... FROM table_name1 RIGHT WINDOW JOIN table_name2 [ON ...] WINDOW_OFFSET(start_offset, end_offset) [JLIMIT jlimit_num] [WHERE ...] [...]
```

##### 4.4.11.3 结果集

右表中每一行数据与左表中基于右表主键时戳列和 WINDOW_OFFSET 划分的窗口内的至多 jlimit_num 条数据或空数据（NULL）的笛卡尔积集合 或 
右表中每一行数据与左表中基于右表主键时戳列和 WINDOW_OFFSET 划分的窗口内的至多 jlimit_num 条数据的聚合结果或空数据（NULL）组成的行数据集合。

##### 4.4.11.4 适用范围

支持超级表、普通表、子表间 Right Window Join。

##### 4.4.11.5 说明

- 只支持表间 Window Join，不支持子查询间 Window Join；
- ON 子句可选，只支持指定除主键列外的 TAG、普通列（不支持标量函数及运算）之间的等值条件用于分组计算，所有条件间只支持 AND 运算；
- WINDOW_OFFSET 用于指定窗口的左右边界相对于右表主键时间戳的偏移量，支持自带时间单位的形式，例如：WINDOW_OFFSET(-1a， 1a)，则表示每个窗口是 [右表主键时间戳 - 1毫秒，右表主键时间戳 + 1毫秒] ，左右边界均为闭区间。数字后面的时间单位可以是 b（纳秒）、u（微秒）、a（毫秒）、s（秒）、m（分）、h（小时）、d（天）、w（周），不支持自然月、自然年，最小支持的时间单位为数据库精度，左右表所在数据库精度需保持一致。
- JLIMIT 用于指定单个窗口内的最大匹配行数，可选，未指定时默认获取每个窗口内的所有匹配行。JLIMIT 取值范围为 [0, 1024]，当左表中不存在满足条件的 jlimit_num 条数据时，返回的结果行数可能小于 jlimit_num；当左表中存在超过 jlimit_num 条满足条件的数据时，优先返回窗口内主键时间戳最小的 jlimit_num 条数据。
- SQL 语句中不能含其他 GROUP BY/PARTITION BY/窗口查询；
- 支持在 WHERE 子句中进行标量过滤，支持在 HAVING 子句中针对窗口进行聚合函数过滤（不支持标量过滤），不支持 SLIMIT；

##### 4.4.11.6 示例

| SQL 语句 | 查询结果 |
| --- | --- |
| SELECT a.ts, b.ts FROM tba1 a RIGHT WINDOW JOIN tba2 b WINDOW_OFFSET(-1s, 1s) | <grid cols="2"> <column width="50"> 2023-11-17 16:29:00 2023-11-17 16:29:00 2023-11-17 16:29:02 2023-11-17 16:29:02 2023-11-17 16:29:03 2023-11-17 16:29:04 2023-11-17 16:29:04 </column> <column width="50"> 2023-11-17 16:29:00 2023-11-17 16:29:01 2023-11-17 16:29:01 2023-11-17 16:29:03 2023-11-17 16:29:03 2023-11-17 16:29:03 2023-11-17 16:29:05 </column> </grid> |
| SELECT a.ts, b.ts FROM tba1 a RIGHT WINDOW JOIN tba2 b WINDOW_OFFSET(-1s, 1s) JLIMIT 1 | <grid cols="2"> <column width="50"> 2023-11-17 16:29:00 2023-11-17 16:29:00 2023-11-17 16:29:02 2023-11-17 16:29:04 </column> <column width="50"> 2023-11-17 16:29:00 2023-11-17 16:29:01 2023-11-17 16:29:03 2023-11-17 16:29:05 </column> </grid> |
| SELECT b.ts, count(a.*) FROM tba1 a RIGHT WINDOW JOIN tba2 b WINDOW_OFFSET(-1s, 1s) | <grid cols="2"> <column width="50"> 2023-11-17 16:29:00 2023-11-17 16:29:01 2023-11-17 16:29:03 2023-11-17 16:29:05 </column> <column width="50"> 1 2 3 1 </column> </grid> |
| SELECT b.ts, count(a.*) FROM tba1 a RIGHT WINDOW JOIN tba2 b ON a.col1 = b.col1 WINDOW_OFFSET(-1s, 1s) | <grid cols="2"> <column width="50"> 2023-11-17 16:29:01 2023-11-17 16:29:03 2023-11-17 16:29:00 2023-11-17 16:29:05 </column> <column width="50"> 1 1 0（NULL） 0（NULL） </column> </grid> |


#### 4.4.12 Full Outer Join

##### 4.4.12.1 含义

全（外）连接 - 既包含左右表同时符合连接条件的数据集合，也包括左右表中不符合连接条件的数据集合。

##### 4.4.12.2 语法

```sql
SELECT ... FROM table_name1 FULL [OUTER] JOIN table_name2 ON ... [WHERE ...] [...]
```

##### 4.4.12.3 结果集

Inner Join 的结果集 + 左表中不符合连接条件的行加上右表的空数据组成的行数据集合 + 右表中不符合连接条件的行加上左表的空数据组成的行数据集合。

##### 4.4.12.4 适用范围

支持超级表、普通表、子表、子查询间 Full Outer Join。

##### 4.4.12.5 说明

- OUTER 关键字可选；

##### 4.4.12.6 示例

| SQL 语句 | 查询结果 |
| --- | --- |
| SELECT a.ts, b.ts FROM tba1 a FULL JOIN tba2 b ON a.ts = b.ts AND a.ts < '2023-11-17 16:29:03' AND b.ts < '2023-11-17 16:29:03' | <grid cols="2"> <column width="50"> 2023-11-17 16:29:00 2023-11-17 16:29:02 NULL 2023-11-17 16:29:03 2023-11-17 16:29:04 NULL NULL </column> <column width="50"> 2023-11-17 16:29:00 NULL 2023-11-17 16:29:01 NULL NULL 2023-11-17 16:29:03 2023-11-17 16:29:05 </column> </grid> |

## 5. 性能

- 本次 Join 功能开发大多数为新功能，因此对既有功能无性能影响，新功能更偏重于功能覆盖，性能为辅，以后期性能测试结果来判断是否需要进一步优化性能；
- Inner Join 的实现在本次会进行完全的重构，因此 Inner Join 的性能预期有提升。
- 其他既有功能基本不受影响。

## 6. 兼容性

无。

## 7. 运维

无。

## 8. 使用场景

基础功能，所有需要使用关联查询的场景都可以使用，目前短期内有需求的是金融客户的 ASOF Join。

## 9. 约束和限制

### 9.1 输入时间线限制

- 目前所有 Join 都要求输入数据含有效的主键时间线，所有表查询都可以满足，子查询需要注意输出数据是否含有效的主键时间线。

### 9.2 连接条件限制

- 除 ASOF 和 Window Join 之外，其他 Join 的连接条件中必须含主键列的主连接条件； 且
- 主连接条件与其他连接条件间只支持 AND 运算；
- 作为主连接条件的主键列只支持 timetruncate 函数运算（不支持其他函数和标量运算），作为其他连接条件时无限制；

### 9.3 分组条件限制

- 只支持除主键列外的 TAG、普通列的等值条件；
- 不支持标量运算；
- 支持多个分组条件，条件间只支持 AND 运算；

### 9.4 查询结果顺序限制

- 普通表、子表、子查询且无分组条件无排序的场景下，查询结果会按照驱动表的主键列顺序输出；
- 超级表查询、Full Join或有分组条件无排序的场景下，查询结果没有固定的输出顺序；
因此，在有排序需求且输出无固定顺序的场景下，需要进行排序操作。部分依赖时间线的函数可能会因为没有有效的时间线输出而无法执行。

### 9.5 嵌套 Join 与多表 Join 限制

- 目前除 Inner Join 支持嵌套与多表 Join 外，其他类型的 JoiN 暂不支持嵌套与多表 Join。

## 10. 常见错误和排查

- 大多数错误参考错误提示即可；
- 测试过程中可能会有一些结果预期错误，需要重点理解连接条件与过滤条件的区别，其他参考FS；

## 11. 参考文档

[需求说明：JOIN](https://taosdata.feishu.cn/wiki/DE02wRO1siQGtykVawhc491vnfb)
[JOIN 操作分析](https://taosdata.feishu.cn/docx/V8HCdaf25ofWYFxnDMZc1g1dnGg)
