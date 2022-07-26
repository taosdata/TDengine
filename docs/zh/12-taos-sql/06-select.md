---
sidebar_label: 数据查询
title: 数据查询
---

## 查询语法

```sql
SELECT {DATABASE() | CLIENT_VERSION() | SERVER_VERSION() | SERVER_STATUS() | NOW() | TODAY() | TIMEZONE()}

SELECT [DISTINCT] select_list
    from_clause
    [WHERE condition]
    [PARTITION BY tag_list]
    [window_clause]
    [group_by_clause]
    [order_by_clasue]
    [SLIMIT limit_val [SOFFSET offset_val]]
    [LIMIT limit_val [OFFSET offset_val]]
    [>> export_file]

select_list:
    select_expr [, select_expr] ...

select_expr: {
    *
  | query_name.*
  | [schema_name.] {table_name | view_name} .*
  | t_alias.*
  | expr [[AS] c_alias]
}

from_clause: {
    table_reference [, table_reference] ...
  | join_clause [, join_clause] ...
}

table_reference:
    table_expr t_alias

table_expr: {
    table_name
  | view_name
  | ( subquery )
}

join_clause:
    table_reference [INNER] JOIN table_reference ON condition

window_clause: {
    SESSION(ts_col, tol_val)
  | STATE_WINDOW(col)
  | INTERVAL(interval_val [, interval_offset]) [SLIDING (sliding_val)] [WATERMARK(watermark_val)] [FILL(fill_mod_and_val)]

changes_option: {
    DURATION duration_val
  | ROWS rows_val
}

group_by_clause:
    GROUP BY expr [, expr] ... HAVING condition

order_by_clasue:
    ORDER BY order_expr [, order_expr] ...

order_expr:
    {expr | position | c_alias} [DESC | ASC] [NULLS FIRST | NULLS LAST]
```

## 列表

查询语句可以指定部分或全部列作为返回结果。数据列和标签列都可以出现在列表中。

### 通配符

通配符 \* 可以用于代指全部列。对于普通表，结果中只有普通列。对于超级表和子表，还包含了 TAG 列。

```sql
SELECT * FROM d1001;
```

通配符支持表名前缀，以下两个 SQL 语句均为返回全部的列：

```sql
SELECT * FROM d1001;
SELECT d1001.* FROM d1001;
```

在 JOIN 查询中，带表名前缀的\*和不带前缀\*返回的结果有差别， \*返回全部表的所有列数据（不包含标签），而带表名前缀的通配符，则只返回该表的列数据。

```sql
SELECT * FROM d1001, d1003 WHERE d1001.ts=d1003.ts;
SELECT d1001.* FROM d1001,d1003 WHERE d1001.ts = d1003.ts;
```

上面的查询语句中，前者返回 d1001 和 d1003 的全部列，而后者仅返回 d1001 的全部列。

在使用 SQL 函数来进行查询的过程中，部分 SQL 函数支持通配符操作。其中的区别在于：
`count(*)`函数只返回一列。`first`、`last`、`last_row`函数则是返回全部列。

### 标签列

在超级表和子表的查询中可以指定 _标签列_，且标签列的值会与普通列的数据一起返回。

```sql
ELECT location, groupid, current FROM d1001 LIMIT 2;
```

### 结果去重

`DISINTCT` 关键字可以对结果集中的一列或多列进行去重，去除的列既可以是标签列也可以是数据列。

对标签列去重：

```sql
SELECT DISTINCT tag_name [, tag_name ...] FROM stb_name;
```

对数据列去重：

```sql
SELECT DISTINCT col_name [, col_name ...] FROM tb_name;
```

:::info

1. cfg 文件中的配置参数 maxNumOfDistinctRes 将对 DISTINCT 能够输出的数据行数进行限制。其最小值是 100000，最大值是 100000000，默认值是 10000000。如果实际计算结果超出了这个限制，那么会仅输出这个数量范围内的部分。
2. 由于浮点数天然的精度机制原因，在特定情况下，对 FLOAT 和 DOUBLE 列使用 DISTINCT 并不能保证输出值的完全唯一性。
3. 在当前版本下，DISTINCT 不能在嵌套查询的子查询中使用，也不能与聚合函数、GROUP BY、或 JOIN 在同一条语句中混用。

:::

### 结果集列名

`SELECT`子句中，如果不指定返回结果集合的列名，结果集列名称默认使用`SELECT`子句中的表达式名称作为列名称。此外，用户可使用`AS`来重命名返回结果集合中列的名称。例如：

```sql
taos> SELECT ts, ts AS primary_key_ts FROM d1001;
```

但是针对`first(*)`、`last(*)`、`last_row(*)`不支持针对单列的重命名。

### 隐式结果列

`Select_exprs`可以是表所属列的列名，也可以是基于列的函数表达式或计算式，数量的上限 256 个。当用户使用了`interval`或`group by tags`的子句以后，在最后返回结果中会强制返回时间戳列（第一列）和 group by 子句中的标签列。后续的版本中可以支持关闭 group by 子句中隐式列的输出，列输出完全由 select 子句控制。

### 伪列

**TBNAME**
`TBNAME` 可以视为超级表中一个特殊的标签，代表子表的表名。

获取一个超级表所有的子表名及相关的标签信息：

```mysql
SELECT TBNAME, location FROM meters;
```

统计超级表下辖子表数量：

```mysql
SELECT COUNT(*) FROM (SELECT DISTINCT TBNAME FROM meters);
```

以上两个查询均只支持在 WHERE 条件子句中添加针对标签（TAGS）的过滤条件。例如：

**\_QSTART/\_QEND**

\_qstart 和\_qend 表示用户输入的查询时间范围，即 WHERE 子句中主键时间戳条件所限定的时间范围。如果 WHERE 子句中没有有效的主键时间戳条件，则时间范围为[-2^63, 2^63-1]。

\_qstart 和\_qend 不能用于 WHERE 子句中。

**\_WSTART/\_WEND/\_WDURATION**
\_wstart 伪列、\_wend 伪列和\_wduration 伪列
\_wstart 表示窗口起始时间戳，\_wend 表示窗口结束时间戳，\_wduration 表示窗口持续时长。

这三个伪列只能用于时间窗口的窗口切分查询之中，且要在窗口切分子句之后出现。

**\_c0/\_ROWTS**

TDengine 中，所有表的第一列都必须是时间戳类型，且为其主键，\_rowts 伪列和\_c0 伪列均代表了此列的值。相比实际的主键时间戳列，使用伪列更加灵活，语义也更加标准。例如，可以和 max\min 等函数一起使用。

```sql
select _rowts, max(current) from meters;
```

## 查询对象

FROM 关键字后面可以是若干个表（超级表）列表，也可以是子查询的结果。
如果没有指定用户的当前数据库，可以在表名称之前使用数据库的名称来指定表所属的数据库。例如：`power.d1001` 方式来跨库使用表。

TDengine 支持基于时间戳主键的 INNER JOIN，规则如下：

1. 支持 FROM 表列表和显式的 JOIN 子句两种语法。
2. 对于普通表和子表，ON 条件必须有且只有时间戳主键的等值条件。
3. 对于超级表，ON 条件在时间戳主键的等值条件之外，还要求有可以一一对应的标签列等值条件，不支持 OR 条件。
4. 参与 JOIN 计算的表只能是同一种类型，即只能都是超级表，或都是子表，或都是普通表。
5. JOIN 两侧均支持子查询。
6. 参与 JOIN 的表个数上限为 10 个。
7. 不支持与 FILL 子句混合使用。

## GROUP BY

如果在语句中同时指定了 GROUP BY 子句，那么 SELECT 列表只能包含如下表达式：

1. 常量
2. 聚集函数
3. 与 GROUP BY 后表达式相同的表达式。
4. 包含前面表达式的表达式

GROUP BY 子句对每行数据按 GROUP BY 后的表达式的值进行分组，并为每个组返回一行汇总信息。

GROUP BY 子句中的表达式可以包含表或视图中的任何列，这些列不需要出现在 SELECT 列表中。

该子句对行进行分组，但不保证结果集的顺序。若要对分组进行排序，请使用 ORDER BY 子句


## PARTITON BY

PARTITION BY 子句是 TDengine 特色语法，按 part_list 对数据进行切分，在每个切分的分片中进行计算。

详见 [TDengine 特色查询](taos-sql/distinguished)

## ORDER BY

ORDER BY 子句对结果集排序。如果没有指定 ORDER BY，无法保证同一语句多次查询的结果集返回顺序一致。

ORDER BY 后可以使用位置语法，位置标识为正整数，从 1 开始，表示使用 SELECT 列表的第几个表达式进行排序。

ASC 表示升序，DESC 表示降序。

NULLS 语法用来指定 NULL 值在排序中输出的位置。NULLS LAST 是升序的默认值，NULLS FIRST 是降序的默认值。

## LIMIT

LIMIT 控制输出条数，OFFSET 指定从第几条之后开始输出。LIMIT/OFFSET 对结果集的执行顺序在 ORDER BY 之后。LIMIT 5 OFFSET 2 可以简写为 LIMIT 2, 5，都输出第 3 行到第 7 行数据。

在有 PARTITION BY 子句时，LIMIT 控制的是每个切分的分片中的输出，而不是总的结果集输出。

## SLIMIT

SLIMIT 和 PARTITION BY 子句一起使用，用来控制输出的分片的数量。SLIMIT 5 SOFFSET 2 可以简写为 SLIMIT 2, 5，都表示输出第 3 个到第 7 个分片。

需要注意，如果有 ORDER BY 子句，则输出只有一个分片。

## 特殊功能

部分特殊的查询功能可以不使用 FROM 子句执行。

### 获取当前数据库

下面的命令可以获取当前所在的数据库 database()，如果登录的时候没有指定默认数据库，且没有使用`USE`命令切换数据，则返回 NULL。

```sql
SELECT DATABASE();
```

### 获取服务器和客户端版本号

```sql
SELECT CLIENT_VERSION();
SELECT SERVER_VERSION();
```

### 获取服务器状态

服务器状态检测语句。如果服务器正常，返回一个数字（例如 1）。如果服务器异常，返回 error code。该 SQL 语法能兼容连接池对于 TDengine 状态的检查及第三方工具对于数据库服务器状态的检查。并可以避免出现使用了错误的心跳检测 SQL 语句导致的连接池连接丢失的问题。

```sql
SELECT SERVER_STATUS();
```

### 获取当前时间

```sql
SELECT NOW();
```

### 获取当前日期

```sql
SELECT TODAY();
```

### 获取当前时区

```sql
SELECT TIMEZONE();
```

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

TDengine 支持“普通表与普通表之间”、“超级表与超级表之间”、“子查询与子查询之间” 进行自然连接。自然连接与内连接的主要区别是，自然连接要求参与连接的字段在不同的表/超级表中必须是同名字段。也即，TDengine 在连接关系的表达中，要求必须使用同名数据列/标签列的相等关系。

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

JOIN 语句存在如下限制要求：

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

## SQL 示例

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
