---
sidebar_label: 数据查询
title: 数据查询
---

## 查询语法

```
SELECT select_expr [, select_expr ...]
    FROM {tb_name_list}
    [WHERE where_condition]
    [SESSION(ts_col, tol_val)]
    [STATE_WINDOW(col)]
    [INTERVAL(interval_val [, interval_offset]) [SLIDING sliding_val]]
    [FILL(fill_mod_and_val)]
    [GROUP BY col_list]
    [ORDER BY col_list { DESC | ASC }]
    [SLIMIT limit_val [SOFFSET offset_val]]
    [LIMIT limit_val [OFFSET offset_val]]
    [>> export_file];
```

## 通配符

通配符 \* 可以用于代指全部列。对于普通表，结果中只有普通列。

```
taos> SELECT * FROM d1001;
           ts            |       current        |   voltage   |        phase         |
======================================================================================
 2018-10-03 14:38:05.000 |             10.30000 |         219 |              0.31000 |
 2018-10-03 14:38:15.000 |             12.60000 |         218 |              0.33000 |
 2018-10-03 14:38:16.800 |             12.30000 |         221 |              0.31000 |
Query OK, 3 row(s) in set (0.001165s)
```

在针对超级表，通配符包含 _标签列_ 。

```
taos> SELECT * FROM meters;
           ts            |       current        |   voltage   |        phase         |            location            |   groupid   |
=====================================================================================================================================
 2018-10-03 14:38:05.500 |             11.80000 |         221 |              0.28000 | Beijing.Haidian                |           2 |
 2018-10-03 14:38:16.600 |             13.40000 |         223 |              0.29000 | Beijing.Haidian                |           2 |
 2018-10-03 14:38:05.000 |             10.80000 |         223 |              0.29000 | Beijing.Haidian                |           3 |
 2018-10-03 14:38:06.500 |             11.50000 |         221 |              0.35000 | Beijing.Haidian                |           3 |
 2018-10-03 14:38:04.000 |             10.20000 |         220 |              0.23000 | Beijing.Chaoyang               |           3 |
 2018-10-03 14:38:16.650 |             10.30000 |         218 |              0.25000 | Beijing.Chaoyang               |           3 |
 2018-10-03 14:38:05.000 |             10.30000 |         219 |              0.31000 | Beijing.Chaoyang               |           2 |
 2018-10-03 14:38:15.000 |             12.60000 |         218 |              0.33000 | Beijing.Chaoyang               |           2 |
 2018-10-03 14:38:16.800 |             12.30000 |         221 |              0.31000 | Beijing.Chaoyang               |           2 |
Query OK, 9 row(s) in set (0.002022s)
```

通配符支持表名前缀，以下两个 SQL 语句均为返回全部的列：

```
SELECT * FROM d1001;
SELECT d1001.* FROM d1001;
```

在 JOIN 查询中，带前缀的\*和不带前缀\*返回的结果有差别， \*返回全部表的所有列数据（不包含标签），带前缀的通配符，则只返回该表的列数据。

```
taos> SELECT * FROM d1001, d1003 WHERE d1001.ts=d1003.ts;
           ts            | current |   voltage   |    phase     |           ts            | current |   voltage   |    phase     |
==================================================================================================================================
 2018-10-03 14:38:05.000 | 10.30000|         219 |      0.31000 | 2018-10-03 14:38:05.000 | 10.80000|         223 |      0.29000 |
Query OK, 1 row(s) in set (0.017385s)
```

```
taos> SELECT d1001.* FROM d1001,d1003 WHERE d1001.ts = d1003.ts;
           ts            |       current        |   voltage   |        phase         |
======================================================================================
 2018-10-03 14:38:05.000 |             10.30000 |         219 |              0.31000 |
Query OK, 1 row(s) in set (0.020443s)
```

在使用 SQL 函数来进行查询的过程中，部分 SQL 函数支持通配符操作。其中的区别在于：
`count(*)`函数只返回一列。`first`、`last`、`last_row`函数则是返回全部列。

```
taos> SELECT COUNT(*) FROM d1001;
       count(*)        |
========================
                     3 |
Query OK, 1 row(s) in set (0.001035s)
```

```
taos> SELECT FIRST(*) FROM d1001;
        first(ts)        |    first(current)    | first(voltage) |     first(phase)     |
=========================================================================================
 2018-10-03 14:38:05.000 |             10.30000 |            219 |              0.31000 |
Query OK, 1 row(s) in set (0.000849s)
```

## 标签列

从 2.0.14 版本开始，支持在普通表的查询中指定 _标签列_，且标签列的值会与普通列的数据一起返回。

```
taos> SELECT location, groupid, current FROM d1001 LIMIT 2;
            location            |   groupid   |       current        |
======================================================================
 Beijing.Chaoyang               |           2 |             10.30000 |
 Beijing.Chaoyang               |           2 |             12.60000 |
Query OK, 2 row(s) in set (0.003112s)
```

注意：普通表的通配符 \* 中并不包含 _标签列_。

## 获取标签列或普通列的去重取值

从 2.0.15.0 版本开始，支持在超级表查询标签列时，指定 DISTINCT 关键字，这样将返回指定标签列的所有不重复取值。注意，在 2.1.6.0 版本之前，DISTINCT 只支持处理单个标签列，而从 2.1.6.0 版本开始，DISTINCT 可以对多个标签列进行处理，输出这些标签列取值不重复的组合。

```sql
SELECT DISTINCT tag_name [, tag_name ...] FROM stb_name;
```

从 2.1.7.0 版本开始，DISTINCT 也支持对数据子表或普通表进行处理，也即支持获取单个普通列的不重复取值，或多个普通列取值的不重复组合。

```sql
SELECT DISTINCT col_name [, col_name ...] FROM tb_name;
```

:::info

1. cfg 文件中的配置参数 maxNumOfDistinctRes 将对 DISTINCT 能够输出的数据行数进行限制。其最小值是 100000，最大值是 100000000，默认值是 10000000。如果实际计算结果超出了这个限制，那么会仅输出这个数量范围内的部分。
2. 由于浮点数天然的精度机制原因，在特定情况下，对 FLOAT 和 DOUBLE 列使用 DISTINCT 并不能保证输出值的完全唯一性。
3. 在当前版本下，DISTINCT 不能在嵌套查询的子查询中使用，也不能与聚合函数、GROUP BY、或 JOIN 在同一条语句中混用。

:::

## 结果集列名

`SELECT`子句中，如果不指定返回结果集合的列名，结果集列名称默认使用`SELECT`子句中的表达式名称作为列名称。此外，用户可使用`AS`来重命名返回结果集合中列的名称。例如：

```
taos> SELECT ts, ts AS primary_key_ts FROM d1001;
           ts            |     primary_key_ts      |
====================================================
 2018-10-03 14:38:05.000 | 2018-10-03 14:38:05.000 |
 2018-10-03 14:38:15.000 | 2018-10-03 14:38:15.000 |
 2018-10-03 14:38:16.800 | 2018-10-03 14:38:16.800 |
Query OK, 3 row(s) in set (0.001191s)
```

但是针对`first(*)`、`last(*)`、`last_row(*)`不支持针对单列的重命名。

## 隐式结果列

`Select_exprs`可以是表所属列的列名，也可以是基于列的函数表达式或计算式，数量的上限 256 个。当用户使用了`interval`或`group by tags`的子句以后，在最后返回结果中会强制返回时间戳列（第一列）和 group by 子句中的标签列。后续的版本中可以支持关闭 group by 子句中隐式列的输出，列输出完全由 select 子句控制。

## 表（超级表）列表

FROM 关键字后面可以是若干个表（超级表）列表，也可以是子查询的结果。
如果没有指定用户的当前数据库，可以在表名称之前使用数据库的名称来指定表所属的数据库。例如：`power.d1001` 方式来跨库使用表。

```
SELECT * FROM power.d1001;
------------------------------
USE power;
SELECT * FROM d1001;
```

## 特殊功能

部分特殊的查询功能可以不使用 FROM 子句执行。获取当前所在的数据库 database()：

```
taos> SELECT DATABASE();
           database()           |
=================================
 power                          |
Query OK, 1 row(s) in set (0.000079s)
```

如果登录的时候没有指定默认数据库，且没有使用`USE`命令切换数据，则返回 NULL。

```
taos> SELECT DATABASE();
           database()           |
=================================
 NULL                           |
Query OK, 1 row(s) in set (0.000184s)
```

获取服务器和客户端版本号：

```
taos> SELECT CLIENT_VERSION();
 client_version() |
===================
 2.0.0.0          |
Query OK, 1 row(s) in set (0.000070s)

taos> SELECT SERVER_VERSION();
 server_version() |
===================
 2.0.0.0          |
Query OK, 1 row(s) in set (0.000077s)
```

服务器状态检测语句。如果服务器正常，返回一个数字（例如 1）。如果服务器异常，返回 error code。该 SQL 语法能兼容连接池对于 TDengine 状态的检查及第三方工具对于数据库服务器状态的检查。并可以避免出现使用了错误的心跳检测 SQL 语句导致的连接池连接丢失的问题。

```
taos> SELECT SERVER_STATUS();
 server_status() |
==================
               1 |
Query OK, 1 row(s) in set (0.000074s)

taos> SELECT SERVER_STATUS() AS status;
   status    |
==============
           1 |
Query OK, 1 row(s) in set (0.000081s)
```

## \_block_dist 函数

**功能说明**: 用于获得指定的（超级）表的数据块分布信息

```txt title="语法"
SELECT _block_dist() FROM { tb_name | stb_name }
```

**返回结果类型**：字符串。

**适用数据类型**：不能输入任何参数。

**嵌套子查询支持**：不支持子查询或嵌套查询。

**返回结果**:

- 返回 FROM 子句中输入的表或超级表的数据块分布情况。不支持查询条件。
- 返回的结果是该表或超级表的数据块所包含的行数的数据分布直方图。

```txt title="返回结果"
summary:
5th=[392], 10th=[392], 20th=[392], 30th=[392], 40th=[792], 50th=[792] 60th=[792], 70th=[792], 80th=[792], 90th=[792], 95th=[792], 99th=[792] Min=[392(Rows)] Max=[800(Rows)] Avg=[666(Rows)] Stddev=[2.17] Rows=[2000], Blocks=[3], Size=[5.440(Kb)] Comp=[0.23] RowsInMem=[0] SeekHeaderTime=[1(us)]
```

**上述信息的说明如下**:

- 查询的（超级）表所包含的存储在文件中的数据块（data block）中所包含的数据行的数量分布直方图信息：5%， 10%， 20%， 30%， 40%， 50%， 60%， 70%， 80%， 90%， 95%， 99% 的数值；
- 所有数据块中，包含行数最少的数据块所包含的行数量， 其中的 Min 指标 392 行。
- 所有数据块中，包含行数最多的数据块所包含的行数量， 其中的 Max 指标 800 行。
- 所有数据块行数的算数平均值 666 行（其中的 Avg 项）。
- 所有数据块中行数分布的均方差为 2.17 ( stddev ）。
- 数据块包含的行的总数为 2000 行（Rows）。
- 数据块总数是 3 个数据块 （Blocks）。
- 数据块占用磁盘空间大小 5.44 Kb （size）。
- 压缩后的数据块的大小除以原始数据的所获得的压缩比例： 23%（Comp），及压缩后的数据规模是原始数据规模的 23%。
- 内存中存在的数据行数是 0，表示内存中没有数据缓存。
- 获取数据块信息的过程中读取头文件的时间开销 1 微秒（SeekHeaderTime）。

**支持版本**：指定计算算法的功能从 2.1.0.x 版本开始，2.1.0.0 之前的版本不支持指定使用算法的功能。

## TAOS SQL 中特殊关键词

- `TBNAME`： 在超级表查询中可视为一个特殊的标签，代表查询涉及的子表名
- `_c0`: 表示表（超级表）的第一列

## 小技巧

获取一个超级表所有的子表名及相关的标签信息：

```
SELECT TBNAME, location FROM meters;
```

统计超级表下辖子表数量：

```
SELECT COUNT(TBNAME) FROM meters;
```

以上两个查询均只支持在 WHERE 条件子句中添加针对标签（TAGS）的过滤条件。例如：

```
taos> SELECT TBNAME, location FROM meters;
             tbname             |            location            |
==================================================================
 d1004                          | Beijing.Haidian                |
 d1003                          | Beijing.Haidian                |
 d1002                          | Beijing.Chaoyang               |
 d1001                          | Beijing.Chaoyang               |
Query OK, 4 row(s) in set (0.000881s)

taos> SELECT COUNT(tbname) FROM meters WHERE groupId > 2;
     count(tbname)     |
========================
                     2 |
Query OK, 1 row(s) in set (0.001091s)
```

- 可以使用 \* 返回所有列，或指定列名。可以对数字列进行四则运算，可以给输出的列取列名。
  - 暂不支持含列名的四则运算表达式用于条件过滤算子（例如，不支持 `where a*2>6;`，但可以写 `where a>6/2;`）。
  - 暂不支持含列名的四则运算表达式作为 SQL 函数的应用对象（例如，不支持 `select min(2*a) from t;`，但可以写 `select 2*min(a) from t;`）。
- WHERE 语句可以使用各种逻辑判断来过滤数字值，或使用通配符来过滤字符串。
- 输出结果缺省按首列时间戳升序排序，但可以指定按降序排序( \_c0 指首列时间戳)。使用 ORDER BY 对其他字段进行排序,排序结果顺序不确定。
- 参数 LIMIT 控制输出条数，OFFSET 指定从第几条开始输出。LIMIT/OFFSET 对结果集的执行顺序在 ORDER BY 之后。且 `LIMIT 5 OFFSET 2` 可以简写为 `LIMIT 2, 5`。
  - 在有 GROUP BY 子句的情况下，LIMIT 参数控制的是每个分组中至多允许输出的条数。
- 参数 SLIMIT 控制由 GROUP BY 指令划分的分组中，至多允许输出几个分组的数据。且 `SLIMIT 5 SOFFSET 2` 可以简写为 `SLIMIT 2, 5`。
- 通过 “>>” 输出结果可以导出到指定文件。

## 条件过滤操作

| **Operation** | **Note**                 | **Applicable Data Types**                 |
| ------------- | ------------------------ | ----------------------------------------- |
| >             | larger than              | all types except bool                     |
| <             | smaller than             | all types except bool                     |
| >=            | larger than or equal to  | all types except bool                     |
| <=            | smaller than or equal to | all types except bool                     |
| =             | equal to                 | all types                                 |
| <\>           | not equal to             | all types                                 |
| is [not] null | is null or is not null   | all types                                 |
| between and   | within a certain range   | all types except bool                     |
| in            | match any value in a set | all types except first column `timestamp` |
| like          | match a wildcard string  | **`binary`** **`nchar`**                  |
| match/nmatch  | filter regex             | **`binary`** **`nchar`**                  |

**使用说明**:

- <\> 算子也可以写为 != ，请注意，这个算子不能用于数据表第一列的 timestamp 字段。
- like 算子使用通配符字符串进行匹配检查。
   - 在通配符字符串中：'%'（百分号）匹配 0 到任意个字符；'\_'（下划线）匹配单个任意 ASCII 字符。
   - 如果希望匹配字符串中原本就带有的 \_（下划线）字符，那么可以在通配符字符串中写作 `\_`，也即加一个反斜线来进行转义。（从 2.2.0.0 版本开始支持）
   - 通配符字符串最长不能超过 20 字节。（从 2.1.6.1 版本开始，通配符字符串的长度放宽到了 100 字节，并可以通过 taos.cfg 中的 maxWildCardsLength 参数来配置这一长度限制。但不建议使用太长的通配符字符串，将有可能严重影响 LIKE 操作的执行性能。）
- 同时进行多个字段的范围过滤，需要使用关键词 AND 来连接不同的查询条件，暂不支持 OR 连接的不同列之间的查询过滤条件。
   - 从 2.3.0.0 版本开始，已支持完整的同一列和/或不同列间的 AND/OR 运算。
- 针对单一字段的过滤，如果是时间过滤条件，则一条语句中只支持设定一个；但针对其他的（普通）列或标签列，则可以使用 `OR` 关键字进行组合条件的查询过滤。例如： `((value > 20 AND value < 30) OR (value < 12))`。
   - 从 2.3.0.0 版本开始，允许使用多个时间过滤条件，但首列时间戳的过滤运算结果只能包含一个区间。
- 从 2.0.17.0 版本开始，条件过滤开始支持 BETWEEN AND 语法，例如 `WHERE col2 BETWEEN 1.5 AND 3.25` 表示查询条件为“1.5 ≤ col2 ≤ 3.25”。
- 从 2.1.4.0 版本开始，条件过滤开始支持 IN 算子，例如 `WHERE city IN ('Beijing', 'Shanghai')`。说明：BOOL 类型写作 `{true, false}` 或 `{0, 1}` 均可，但不能写作 0、1 之外的整数；FLOAT 和 DOUBLE 类型会受到浮点数精度影响，集合内的值在精度范围内认为和数据行的值完全相等才能匹配成功；TIMESTAMP 类型支持非主键的列。
- 从 2.3.0.0 版本开始，条件过滤开始支持正则表达式，关键字 match/nmatch，不区分大小写。

## 正则表达式过滤

### 语法

```txt
WHERE (column|tbname) **match/MATCH/nmatch/NMATCH** _regex_
```

### 正则表达式规范

确保使用的正则表达式符合 POSIX 的规范，具体规范内容可参见[Regular Expressions](https://pubs.opengroup.org/onlinepubs/9699919799/basedefs/V1_chap09.html)

### 使用限制

只能针对表名（即 tbname 筛选）、binary/nchar 类型标签值进行正则表达式过滤，不支持普通列的过滤。

正则匹配字符串长度不能超过 128 字节。可以通过参数 _maxRegexStringLen_ 设置和调整最大允许的正则匹配字符串，该参数是客户端配置参数，需要重启才能生效。

## JOIN 子句

从 2.2.0.0 版本开始，TDengine 对内连接（INNER JOIN）中的自然连接（Natural join）操作实现了完整的支持。也即支持“普通表与普通表之间”、“超级表与超级表之间”、“子查询与子查询之间”进行自然连接。自然连接与内连接的主要区别是，自然连接要求参与连接的字段在不同的表/超级表中必须是同名字段。也即，TDengine 在连接关系的表达中，要求必须使用同名数据列/标签列的相等关系。

在普通表与普通表之间的 JOIN 操作中，只能使用主键时间戳之间的相等关系。例如：

```sql
SELECT *
FROM temp_tb_1 t1, pressure_tb_1 t2
WHERE t1.ts = t2.ts
```

在超级表与超级表之间的 JOIN 操作中，除了主键时间戳一致的条件外，还要求引入能实现一一对应的标签列的相等关系。例如：

```sql
SELECT *
FROM temp_stable t1, temp_stable t2
WHERE t1.ts = t2.ts AND t1.deviceid = t2.deviceid AND t1.status=0;
```

类似地，也可以对多个子查询的查询结果进行 JOIN 操作。

:::note

JOIN语句存在如下限制要求：

- 参与一条语句中 JOIN 操作的表/超级表最多可以有 10 个。
- 在包含 JOIN 操作的查询语句中不支持 FILL。
- 暂不支持参与 JOIN 操作的表之间聚合后的四则运算。
- 不支持只对其中一部分表做 GROUP BY。
- JOIN 查询的不同表的过滤条件之间不能为 OR。
- JOIN 查询要求连接条件不能是普通列，只能针对标签和主时间字段列（第一列）。

:::

## 嵌套查询

“嵌套查询”又称为“子查询”，也即在一条 SQL 语句中，“内层查询”的计算结果可以作为“外层查询”的计算对象来使用。

从 2.2.0.0 版本开始，TDengine 的查询引擎开始支持在 FROM 子句中使用非关联子查询（“非关联”的意思是，子查询不会用到父查询中的参数）。也即在普通 SELECT 语句的 tb_name_list 位置，用一个独立的 SELECT 语句来代替（这一 SELECT 语句被包含在英文圆括号内），于是完整的嵌套查询 SQL 语句形如：

```
SELECT ... FROM (SELECT ... FROM ...) ...;
```

:::info

- 目前仅支持一层嵌套，也即不能在子查询中再嵌入子查询。
- 内层查询的返回结果将作为“虚拟表”供外层查询使用，此虚拟表可以使用 AS 语法做重命名，以便于外层查询中方便引用。
- 目前不能在“连续查询”功能中使用子查询。
- 在内层和外层查询中，都支持普通的表间/超级表间 JOIN。内层查询的计算结果也可以再参与数据子表的 JOIN 操作。
- 目前内层查询、外层查询均不支持 UNION 操作。
- 内层查询支持的功能特性与非嵌套的查询语句能力是一致的。
   - 内层查询的 ORDER BY 子句一般没有意义，建议避免这样的写法以免无谓的资源消耗。
- 与非嵌套的查询语句相比，外层查询所能支持的功能特性存在如下限制：
   - 计算函数部分：
      - 如果内层查询的结果数据未提供时间戳，那么计算过程依赖时间戳的函数在外层会无法正常工作。例如：TOP, BOTTOM, FIRST, LAST, DIFF。
      - 计算过程需要两遍扫描的函数，在外层查询中无法正常工作。例如：此类函数包括：STDDEV, PERCENTILE。
   - 外层查询中不支持 IN 算子，但在内层中可以使用。
   - 外层查询不支持 GROUP BY。

:::

## UNION ALL 子句

```txt title=语法
SELECT ...
UNION ALL SELECT ...
[UNION ALL SELECT ...]
```

TDengine 支持 UNION ALL 操作符。也就是说，如果多个 SELECT 子句返回结果集的结构完全相同（列名、列类型、列数、顺序），那么可以通过 UNION ALL 把这些结果集合并到一起。目前只支持 UNION ALL 模式，也即在结果集的合并过程中是不去重的。在同一个 sql 语句中，UNION ALL 最多支持 100 个。

### SQL 示例

对于下面的例子，表 tb1 用以下语句创建：

```
CREATE TABLE tb1 (ts TIMESTAMP, col1 INT, col2 FLOAT, col3 BINARY(50));
```

查询 tb1 刚过去的一个小时的所有记录：

```
SELECT * FROM tb1 WHERE ts >= NOW - 1h;
```

查询表 tb1 从 2018-06-01 08:00:00.000 到 2018-06-02 08:00:00.000 时间范围，并且 col3 的字符串是'nny'结尾的记录，结果按照时间戳降序：

```
SELECT * FROM tb1 WHERE ts > '2018-06-01 08:00:00.000' AND ts <= '2018-06-02 08:00:00.000' AND col3 LIKE '%nny' ORDER BY ts DESC;
```

查询 col1 与 col2 的和，并取名 complex, 时间大于 2018-06-01 08:00:00.000, col2 大于 1.2，结果输出仅仅 10 条记录，从第 5 条开始：

```
SELECT (col1 + col2) AS 'complex' FROM tb1 WHERE ts > '2018-06-01 08:00:00.000' AND col2 > 1.2 LIMIT 10 OFFSET 5;
```

查询过去 10 分钟的记录，col2 的值大于 3.14，并且将结果输出到文件 `/home/testoutput.csv`：

```
SELECT COUNT(*) FROM tb1 WHERE ts >= NOW - 10m AND col2 > 3.14 >> /home/testoutput.csv;
```
