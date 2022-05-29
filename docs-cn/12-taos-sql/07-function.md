---
sidebar_label: SQL 函数
title: SQL 函数
---

## 聚合函数

TDengine 支持针对数据的聚合查询。提供支持的聚合和选择函数如下：

### COUNT

```
SELECT COUNT([*|field_name]) FROM tb_name [WHERE clause];
```

**功能说明**：统计表/超级表中记录行数或某列的非空值个数。

**返回数据类型**：长整型 INT64。

**应用字段**：应用全部字段。

**适用于**：表、超级表。

**使用说明**:

- 可以使用星号(\*)来替代具体的字段，使用星号(\*)返回全部记录数量。
- 针对同一表的（不包含 NULL 值）字段查询结果均相同。
- 如果统计对象是具体的列，则返回该列中非 NULL 值的记录数量。

**示例**：

```
taos> SELECT COUNT(*), COUNT(voltage) FROM meters;
    count(*)        |    count(voltage)     |
================================================
                    9 |                     9 |
Query OK, 1 row(s) in set (0.004475s)

taos> SELECT COUNT(*), COUNT(voltage) FROM d1001;
    count(*)        |    count(voltage)     |
================================================
                    3 |                     3 |
Query OK, 1 row(s) in set (0.001075s)
```

### AVG

```
SELECT AVG(field_name) FROM tb_name [WHERE clause];
```

**功能说明**：统计表/超级表中某列的平均值。

**返回数据类型**：双精度浮点数 Double。

**应用字段**：不能应用在 timestamp、binary、nchar、bool 字段。

**适用于**：表、超级表。

**示例**：

```
taos> SELECT AVG(current), AVG(voltage), AVG(phase) FROM meters;
    avg(current)        |       avg(voltage)        |        avg(phase)         |
====================================================================================
            11.466666751 |             220.444444444 |               0.293333333 |
Query OK, 1 row(s) in set (0.004135s)

taos> SELECT AVG(current), AVG(voltage), AVG(phase) FROM d1001;
    avg(current)        |       avg(voltage)        |        avg(phase)         |
====================================================================================
            11.733333588 |             219.333333333 |               0.316666673 |
Query OK, 1 row(s) in set (0.000943s)
```

### TWA

```
SELECT TWA(field_name) FROM tb_name WHERE clause;
```

**功能说明**：时间加权平均函数。统计表中某列在一段时间内的时间加权平均。

**返回数据类型**：双精度浮点数 Double。

**应用字段**：不能应用在 timestamp、binary、nchar、bool 类型字段。

**适用于**：表、超级表。

**使用说明**：

- 从 2.1.3.0 版本开始，TWA 函数可以在由 GROUP BY 划分出单独时间线的情况下用于超级表（也即 GROUP BY tbname）。

### IRATE

```
SELECT IRATE(field_name) FROM tb_name WHERE clause;
```

**功能说明**：计算瞬时增长率。使用时间区间中最后两个样本数据来计算瞬时增长速率；如果这两个值呈递减关系，那么只取最后一个数用于计算，而不是使用二者差值。

**返回数据类型**：双精度浮点数 Double。

**应用字段**：不能应用在 timestamp、binary、nchar、bool 类型字段。

**适用于**：表、超级表。

**使用说明**：

- 从 2.1.3.0 版本开始此函数可用，IRATE 可以在由 GROUP BY 划分出单独时间线的情况下用于超级表（也即 GROUP BY tbname）。

### SUM

```
SELECT SUM(field_name) FROM tb_name [WHERE clause];
```

**功能说明**：统计表/超级表中某列的和。

**返回数据类型**：双精度浮点数 Double 和长整型 INT64。

**应用字段**：不能应用在 timestamp、binary、nchar、bool 类型字段。

**适用于**：表、超级表。

**示例**：

```
taos> SELECT SUM(current), SUM(voltage), SUM(phase) FROM meters;
    sum(current)        |     sum(voltage)      |        sum(phase)         |
================================================================================
            103.200000763 |                  1984 |               2.640000001 |
Query OK, 1 row(s) in set (0.001702s)

taos> SELECT SUM(current), SUM(voltage), SUM(phase) FROM d1001;
    sum(current)        |     sum(voltage)      |        sum(phase)         |
================================================================================
            35.200000763 |                   658 |               0.950000018 |
Query OK, 1 row(s) in set (0.000980s)
```

### STDDEV

```
SELECT STDDEV(field_name) FROM tb_name [WHERE clause];
```

**功能说明**：统计表中某列的均方差。

**返回数据类型**：双精度浮点数 Double。

**应用字段**：不能应用在 timestamp、binary、nchar、bool 类型字段。

**适用于**：表、超级表（从 2.0.15.1 版本开始）

**示例**：

```
taos> SELECT STDDEV(current) FROM d1001;
    stddev(current)      |
============================
            1.020892909 |
Query OK, 1 row(s) in set (0.000915s)
```

### LEASTSQUARES

```
SELECT LEASTSQUARES(field_name, start_val, step_val) FROM tb_name [WHERE clause];
```

**功能说明**：统计表中某列的值是主键（时间戳）的拟合直线方程。start_val 是自变量初始值，step_val 是自变量的步长值。

**返回数据类型**：字符串表达式（斜率, 截距）。

**应用字段**：不能应用在 timestamp、binary、nchar、bool 类型字段。

**适用于**：表。

**示例**：

```
taos> SELECT LEASTSQUARES(current, 1, 1) FROM d1001;
            leastsquares(current, 1, 1)             |
=====================================================
{slop:1.000000, intercept:9.733334}                 |
Query OK, 1 row(s) in set (0.000921s)
```

### MODE

```
SELECT MODE(field_name) FROM tb_name [WHERE clause];
```

**功能说明**：返回出现频率最高的值，若存在多个频率相同的最高值，输出空。不能匹配标签、时间戳输出。

**返回数据类型**：同应用的字段。

**应用字段**：适合于除时间主列外的任何类型字段。

**使用说明**：由于返回数据量未知，考虑到内存因素，为了函数可以正常返回结果，建议不重复的数据量在 10 万级别，否则会报错。

**支持的版本**：2.6.0.0 及以后的版本。

**示例**：

```
taos> select voltage from d002;
    voltage        |
========================
       1           |
       1           |
       2           |
       19          |
Query OK, 4 row(s) in set (0.003545s)

taos> select mode(voltage) from d002;
  mode(voltage)    |
========================
       1           |
Query OK, 1 row(s) in set (0.019393s)
```

### HYPERLOGLOG

```
SELECT HYPERLOGLOG(field_name) FROM { tb_name | stb_name } [WHERE clause];
```

**功能说明**：
  - 采用 hyperloglog 算法，返回某列的基数。该算法在数据量很大的情况下，可以明显降低内存的占用，但是求出来的基数是个估算值，标准误差（标准误差是多次实验，每次的平均数的标准差，不是与真实结果的误差）为 0.81%。
  - 在数据量较少的时候该算法不是很准确，可以使用 select count(data) from (select unique(col) as data from table) 的方法。

**返回结果类型**：整形。

**应用字段**：适合于任何类型字段。

**支持的版本**：2.6.0.0 及以后的版本。

**示例**：

```
taos> select dbig from shll;
     dbig          |
========================
       1           |
       1           |
       1           |
       NULL        |
       2           |
       19          |
       NULL        |
       9           |
Query OK, 8 row(s) in set (0.003755s)

taos> select hyperloglog(dbig) from shll;
  hyperloglog(dbig)|
========================
       4           |
Query OK, 1 row(s) in set (0.008388s)
```

## 选择函数

在使用所有的选择函数的时候，可以同时指定输出 ts 列或标签列（包括 tbname），这样就可以方便地知道被选出的值是源于哪个数据行的。

### MIN

```
SELECT MIN(field_name) FROM {tb_name | stb_name} [WHERE clause];
```

**功能说明**：统计表/超级表中某列的值最小值。

**返回数据类型**：同应用的字段。

**应用字段**：不能应用在 timestamp、binary、nchar、bool 类型字段。

**适用于**：表、超级表。

**示例**：

```
taos> SELECT MIN(current), MIN(voltage) FROM meters;
    min(current)     | min(voltage) |
======================================
            10.20000 |          218 |
Query OK, 1 row(s) in set (0.001765s)

taos> SELECT MIN(current), MIN(voltage) FROM d1001;
    min(current)     | min(voltage) |
======================================
            10.30000 |          218 |
Query OK, 1 row(s) in set (0.000950s)
```

### MAX

```
SELECT MAX(field_name) FROM { tb_name | stb_name } [WHERE clause];
```

**功能说明**：统计表/超级表中某列的值最大值。

**返回数据类型**：同应用的字段。

**应用字段**：不能应用在 timestamp、binary、nchar、bool 类型字段。

**适用于**：表、超级表。

**示例**：

```
taos> SELECT MAX(current), MAX(voltage) FROM meters;
    max(current)     | max(voltage) |
======================================
            13.40000 |          223 |
Query OK, 1 row(s) in set (0.001123s)

taos> SELECT MAX(current), MAX(voltage) FROM d1001;
    max(current)     | max(voltage) |
======================================
            12.60000 |          221 |
Query OK, 1 row(s) in set (0.000987s)
```

### FIRST

```
SELECT FIRST(field_name) FROM { tb_name | stb_name } [WHERE clause];
```

**功能说明**：统计表/超级表中某列的值最先写入的非 NULL 值。

**返回数据类型**：同应用的字段。

**应用字段**：所有字段。

**适用于**：表、超级表。

**使用说明**:

- 如果要返回各个列的首个（时间戳最小）非 NULL 值，可以使用 FIRST(\*)；
- 如果结果集中的某列全部为 NULL 值，则该列的返回结果也是 NULL；
- 如果结果集中所有列全部为 NULL 值，则不返回结果。

**示例**：

```
taos> SELECT FIRST(*) FROM meters;
        first(ts)        |    first(current)    | first(voltage) |     first(phase)     |
=========================================================================================
2018-10-03 14:38:04.000 |             10.20000 |            220 |              0.23000 |
Query OK, 1 row(s) in set (0.004767s)

taos> SELECT FIRST(current) FROM d1002;
    first(current)    |
=======================
            10.20000 |
Query OK, 1 row(s) in set (0.001023s)
```

### LAST

```
SELECT LAST(field_name) FROM { tb_name | stb_name } [WHERE clause];
```

**功能说明**：统计表/超级表中某列的值最后写入的非 NULL 值。

**返回数据类型**：同应用的字段。

**应用字段**：所有字段。

**适用于**：表、超级表。

**使用说明**:

- 如果要返回各个列的最后（时间戳最大）一个非 NULL 值，可以使用 LAST(\*)；
- 如果结果集中的某列全部为 NULL 值，则该列的返回结果也是 NULL；如果结果集中所有列全部为 NULL 值，则不返回结果。
- 在用于超级表时，时间戳完全一样且同为最大的数据行可能有多个，那么会从中随机返回一条，而并不保证多次运行所挑选的数据行必然一致。


**示例**：

```
taos> SELECT LAST(*) FROM meters;
        last(ts)         |    last(current)     | last(voltage) |     last(phase)      |
========================================================================================
2018-10-03 14:38:16.800 |             12.30000 |           221 |              0.31000 |
Query OK, 1 row(s) in set (0.001452s)

taos> SELECT LAST(current) FROM d1002;
    last(current)     |
=======================
            10.30000 |
Query OK, 1 row(s) in set (0.000843s)
```

### TOP

```
SELECT TOP(field_name, K) FROM { tb_name | stb_name } [WHERE clause];
```

**功能说明**： 统计表/超级表中某列的值最大 _k_ 个非 NULL 值。如果多条数据取值一样，全部取用又会超出 k 条限制时，系统会从相同值中随机选取符合要求的数量返回。

**返回数据类型**：同应用的字段。

**应用字段**：不能应用在 timestamp、binary、nchar、bool 类型字段。

**适用于**：表、超级表。

**使用说明**:

- *k*值取值范围 1≤*k*≤100；
- 系统同时返回该记录关联的时间戳列；
- 限制：TOP 函数不支持 FILL 子句。

**示例**：

```
taos> SELECT TOP(current, 3) FROM meters;
        ts            |   top(current, 3)    |
=================================================
2018-10-03 14:38:15.000 |             12.60000 |
2018-10-03 14:38:16.600 |             13.40000 |
2018-10-03 14:38:16.800 |             12.30000 |
Query OK, 3 row(s) in set (0.001548s)

taos> SELECT TOP(current, 2) FROM d1001;
        ts            |   top(current, 2)    |
=================================================
2018-10-03 14:38:15.000 |             12.60000 |
2018-10-03 14:38:16.800 |             12.30000 |
Query OK, 2 row(s) in set (0.000810s)
```

### BOTTOM

```
SELECT BOTTOM(field_name, K) FROM { tb_name | stb_name } [WHERE clause];
```

**功能说明**：统计表/超级表中某列的值最小 _k_ 个非 NULL 值。如果多条数据取值一样，全部取用又会超出 k 条限制时，系统会从相同值中随机选取符合要求的数量返回。

**返回数据类型**：同应用的字段。

**应用字段**：不能应用在 timestamp、binary、nchar、bool 类型字段。

**适用于**：表、超级表。

**使用说明**:

- *k*值取值范围 1≤*k*≤100；
- 系统同时返回该记录关联的时间戳列；
- 限制：BOTTOM 函数不支持 FILL 子句。

**示例**：

```
taos> SELECT BOTTOM(voltage, 2) FROM meters;
        ts            | bottom(voltage, 2) |
===============================================
2018-10-03 14:38:15.000 |                218 |
2018-10-03 14:38:16.650 |                218 |
Query OK, 2 row(s) in set (0.001332s)

taos> SELECT BOTTOM(current, 2) FROM d1001;
        ts            |  bottom(current, 2)  |
=================================================
2018-10-03 14:38:05.000 |             10.30000 |
2018-10-03 14:38:16.800 |             12.30000 |
Query OK, 2 row(s) in set (0.000793s)
```

### PERCENTILE

```
SELECT PERCENTILE(field_name, P) FROM { tb_name } [WHERE clause];
```

**功能说明**：统计表中某列的值百分比分位数。

**返回数据类型**： 双精度浮点数 Double。

**应用字段**：不能应用在 timestamp、binary、nchar、bool 类型字段。

**适用于**：表。

**使用说明**：*P*值取值范围 0≤*P*≤100，为 0 的时候等同于 MIN，为 100 的时候等同于 MAX。

**示例**：

```
taos> SELECT PERCENTILE(current, 20) FROM d1001;
percentile(current, 20)  |
============================
            11.100000191 |
Query OK, 1 row(s) in set (0.000787s)
```

### APERCENTILE

```
SELECT APERCENTILE(field_name, P[, algo_type])
FROM { tb_name | stb_name } [WHERE clause]
```

**功能说明**：统计表/超级表中指定列的值百分比分位数，与 PERCENTILE 函数相似，但是返回近似结果。

**返回数据类型**： 双精度浮点数 Double。

**应用字段**：不能应用在 timestamp、binary、nchar、bool 类型字段。

**适用于**：表、超级表。

**使用说明**

- **P**值有效取值范围 0≤P≤100，为 0 的时候等同于 MIN，为 100 的时候等同于 MAX；
- **algo_type**的有效输入：**default** 和 **t-digest**
- 用于指定计算近似分位数的算法。可不提供第三个参数的输入，此时将使用 default 的算法进行计算，即 apercentile(column_name, 50, "default") 与 apercentile(column_name, 50) 等价。
- 当使用“t-digest”参数的时候，将使用 t-digest 方式采样计算近似分位数。但该参数指定计算算法的功能从 2.2.0.x 版本开始支持，2.2.0.0 之前的版本不支持指定使用算法的功能。

**嵌套子查询支持**：适用于内层查询和外层查询。

```
taos> SELECT APERCENTILE(current, 20) FROM d1001;
apercentile(current, 20)  |
============================
            10.300000191 |
Query OK, 1 row(s) in set (0.000645s)

taos> select apercentile (count, 80, 'default') from stb1;
 apercentile (c0, 80, 'default') |
==================================
             601920857.210056424 |
Query OK, 1 row(s) in set (0.012363s)

taos> select apercentile (count, 80, 't-digest') from stb1;
 apercentile (c0, 80, 't-digest') |
===================================
              605869120.966666579 |
Query OK, 1 row(s) in set (0.011639s)
```

### LAST_ROW

```
SELECT LAST_ROW(field_name) FROM { tb_name | stb_name };
```

**功能说明**：返回表/超级表的最后一条记录。

**返回数据类型**：同应用的字段。

**应用字段**：所有字段。

**适用于**：表、超级表。

**使用说明**：

- 在用于超级表时，时间戳完全一样且同为最大的数据行可能有多个，那么会从中随机返回一条，而并不保证多次运行所挑选的数据行必然一致。
- 不能与 INTERVAL 一起使用。

**示例**：

```
 taos> SELECT LAST_ROW(current) FROM meters;
 last_row(current)   |
 =======================
             12.30000 |
 Query OK, 1 row(s) in set (0.001238s)

 taos> SELECT LAST_ROW(current) FROM d1002;
 last_row(current)   |
 =======================
             10.30000 |
 Query OK, 1 row(s) in set (0.001042s)
```

### INTERP [2.3.1 及之后的版本]

```
SELECT INTERP(field_name) FROM { tb_name | stb_name } [WHERE where_condition] [ RANGE(timestamp1,timestamp2) ] [EVERY(interval)] [FILL ({ VALUE | PREV | NULL | LINEAR | NEXT})];
```

**功能说明**：返回表/超级表的指定时间截面指定列的记录值（插值）。

**返回数据类型**：同字段类型。

**应用字段**：数值型字段。

**适用于**：表、超级表、嵌套查询。


**使用说明**

- INTERP 用于在指定时间断面获取指定列的记录值，如果该时间断面不存在符合条件的行数据，那么会根据 FILL 参数的设定进行插值。
- INTERP 的输入数据为指定列的数据，可以通过条件语句（where 子句）来对原始列数据进行过滤，如果没有指定过滤条件则输入为全部数据。
- INTERP 的输出时间范围根据 RANGE(timestamp1,timestamp2)字段来指定，需满足 timestamp1<=timestamp2。其中 timestamp1（必选值）为输出时间范围的起始值，即如果 timestamp1 时刻符合插值条件则 timestamp1 为输出的第一条记录，timestamp2（必选值）为输出时间范围的结束值，即输出的最后一条记录的 timestamp 不能大于 timestamp2。如果没有指定 RANGE，那么满足过滤条件的输入数据中第一条记录的 timestamp 即为 timestamp1，最后一条记录的 timestamp 即为 timestamp2，同样也满足 timestamp1 <= timestamp2。
- INTERP 根据 EVERY 字段来确定输出时间范围内的结果条数，即从 timestamp1 开始每隔固定长度的时间（EVERY 值）进行插值。如果没有指定 EVERY，则默认窗口大小为无穷大，即从 timestamp1 开始只有一个窗口。
- INTERP 根据 FILL 字段来决定在每个符合输出条件的时刻如何进行插值，如果没有 FILL 字段则默认不插值，即输出为原始记录值或不输出（原始记录不存在）。
- INTERP 只能在一个时间序列内进行插值，因此当作用于超级表时必须跟 group by tbname 一起使用，当作用嵌套查询外层时内层子查询不能含 GROUP BY 信息。
- INTERP 的插值结果不受 ORDER BY timestamp 的影响，ORDER BY timestamp 只影响输出结果的排序。

**SQL示例（基于文档中广泛使用的电表 schema )**：

- 单点线性插值

```
 taos> SELECT INTERP(current) FROM t1 RANGE('2017-7-14 18:40:00','2017-7-14 18:40:00') FILL(LINEAR);
```

- 在2017-07-14 18:00:00到2017-07-14 19:00:00间每隔5秒钟进行取值(不插值)

```
 taos> SELECT INTERP(current) FROM t1 RANGE('2017-7-14 18:00:00','2017-7-14 19:00:00') EVERY(5s);
```

- 在2017-07-14 18:00:00到2017-07-14 19:00:00间每隔5秒钟进行线性插值

```
  taos> SELECT INTERP(current) FROM t1 RANGE('2017-7-14 18:00:00','2017-7-14 19:00:00') EVERY(5s) FILL(LINEAR);
```

- 在所有时间范围内每隔 5 秒钟进行向后插值

```
  taos> SELECT INTERP(current) FROM t1 EVERY(5s) FILL(NEXT);
```

- 根据 2017-07-14 17:00:00 到 2017-07-14 20:00:00 间的数据进行从 2017-07-14 18:00:00 到 2017-07-14 19:00:00 间每隔 5 秒钟进行线性插值

```
  taos> SELECT INTERP(current) FROM t1 where ts >= '2017-07-14 17:00:00' and ts <= '2017-07-14 20:00:00' RANGE('2017-7-14 18:00:00'，'2017-7-14 19:00:00') EVERY(5s) FILL(LINEAR);
```

### INTERP [2.3.1 之前的版本]

```
SELECT INTERP(field_name) FROM { tb_name | stb_name } WHERE ts='timestamp' [FILL ({ VALUE | PREV | NULL | LINEAR | NEXT})];
```

**功能说明**：返回表/超级表的指定时间截面、指定字段的记录。

**返回数据类型**：同字段类型。

**应用字段**：数值型字段。

**适用于**：表、超级表。

**使用说明**:

- 从 2.0.15.0 及以后版本可用
- INTERP 必须指定时间断面，如果该时间断面不存在直接对应的数据，那么会根据 FILL 参数的设定进行插值。此外，条件语句里面可附带筛选条件，例如标签、tbname。
- INTERP 查询要求查询的时间区间必须位于数据集合（表）的所有记录的时间范围之内。如果给定的时间戳位于时间范围之外，即使有插值指令，仍然不返回结果。
- 单个 INTERP 函数查询只能够针对一个时间点进行查询，如果需要返回等时间间隔的断面数据，可以通过 INTERP 配合 EVERY 的方式来进行查询处理（而不是使用 INTERVAL），其含义是每隔固定长度的时间进行插值

**示例**：

```
 taos> SELECT INTERP(*) FROM meters WHERE ts='2017-7-14 18:40:00.004';
        interp(ts)        |   interp(current)    | interp(voltage) |    interp(phase)     |
 ==========================================================================================
  2017-07-14 18:40:00.004 |              9.84020 |             216 |              0.32222 |
 Query OK, 1 row(s) in set (0.002652s)
```

如果给定的时间戳无对应的数据，在不指定插值生成策略的情况下，不会返回结果，如果指定了插值策略，会根据插值策略返回结果。

```
 taos> SELECT INTERP(*) FROM meters WHERE tbname IN ('d636') AND ts='2017-7-14 18:40:00.005';
 Query OK, 0 row(s) in set (0.004022s)

 taos> SELECT INTERP(*) FROM meters WHERE tbname IN ('d636') AND ts='2017-7-14 18:40:00.005' FILL(PREV);
        interp(ts)        |   interp(current)    | interp(voltage) |    interp(phase)     |
 ==========================================================================================
  2017-07-14 18:40:00.005 |              9.88150 |             217 |              0.32500 |
 Query OK, 1 row(s) in set (0.003056s)
```

如下所示代码表示在时间区间 `['2017-7-14 18:40:00', '2017-7-14 18:40:00.014']` 中每隔 5 毫秒 进行一次断面计算。

```
 taos> SELECT INTERP(current) FROM d636 WHERE ts>='2017-7-14 18:40:00' AND ts<='2017-7-14 18:40:00.014' EVERY(5a);
            ts            |   interp(current)    |
 =================================================
  2017-07-14 18:40:00.000 |             10.04179 |
  2017-07-14 18:40:00.010 |             10.16123 |
 Query OK, 2 row(s) in set (0.003487s)
```

### TAIL

```
SELECT TAIL(field_name, k, offset_val) FROM {tb_name | stb_name} [WHERE clause];
```

**功能说明**：返回跳过最后 offset_value 个，然后取连续 k 个记录，不忽略 NULL 值。offset_val 可以不输入。此时返回最后的 k 个记录。当有 offset_val 输入的情况下，该函数功能等效于 `order by ts desc LIMIT k OFFSET offset_val`。

**参数范围**：k: [1,100] offset_val: [0,100]。

**返回结果数据类型**：同应用的字段。

**应用字段**：适合于除时间主列外的任何类型字段。

**支持版本**：2.6.0.0 及之后的版本。

**示例**：

```
taos> select ts,dbig from tail2;
       ts            |         dbig          |
==================================================
2021-10-15 00:31:33.000 |                     1 |
2021-10-17 00:31:31.000 |                  NULL |
2021-12-24 00:31:34.000 |                     2 |
2022-01-01 08:00:05.000 |                    19 |
2022-01-01 08:00:06.000 |                  NULL |
2022-01-01 08:00:07.000 |                     9 |
Query OK, 6 row(s) in set (0.001952s)

taos> select tail(dbig,2,2) from tail2;
ts                      |    tail(dbig,2,2)     |
==================================================
2021-12-24 00:31:34.000 |                     2 |
2022-01-01 08:00:05.000 |                    19 |
Query OK, 2 row(s) in set (0.002307s)
```

### UNIQUE

```
SELECT UNIQUE(field_name) FROM {tb_name | stb_name} [WHERE clause];
```

**功能说明**：返回该列的数值首次出现的值。该函数功能与 distinct 相似，但是可以匹配标签和时间戳信息。可以针对除时间列以外的字段进行查询，可以匹配标签和时间戳，其中的标签和时间戳是第一次出现时刻的标签和时间戳。

**返回结果数据类型**：同应用的字段。

**应用字段**：适合于除时间类型以外的字段。

**支持版本**：2.6.0.0 及之后的版本。

**使用说明**:

- 该函数可以应用在普通表和超级表上。不能和窗口操作一起使用，例如 interval/state_window/session_window 。
- 由于返回数据量未知，考虑到内存因素，为了函数可以正常返回结果，建议不重复的数据量在 10 万级别，否则会报错。

**示例**：

```
taos> select ts,voltage from unique1;
       ts            |        voltage        |
==================================================
2021-10-17 00:31:31.000 |                     1 |
2022-01-24 00:31:31.000 |                     1 |
2021-10-17 00:31:31.000 |                     1 |
2021-12-24 00:31:31.000 |                     2 |
2022-01-01 08:00:01.000 |                    19 |
2021-10-17 00:31:31.000 |                  NULL |
2022-01-01 08:00:02.000 |                  NULL |
2022-01-01 08:00:03.000 |                     9 |
Query OK, 8 row(s) in set (0.003018s)

taos> select unique(voltage) from unique1;
ts                      |    unique(voltage)    |
==================================================
2021-10-17 00:31:31.000 |                     1 |
2021-10-17 00:31:31.000 |                  NULL |
2021-12-24 00:31:31.000 |                     2 |
2022-01-01 08:00:01.000 |                    19 |
2022-01-01 08:00:03.000 |                     9 |
Query OK, 5 row(s) in set (0.108458s)
```

## 计算函数

### DIFF

  ```sql
  SELECT {DIFF(field_name, ignore_negative) | DIFF(field_name)} FROM tb_name [WHERE clause];
  ```

**功能说明**：统计表中某列的值与前一行对应值的差。 ignore_negative 取值为 0|1 , 可以不填，默认值为 0. 不忽略负值。ignore_negative 为 1 时表示忽略负数。

**返回结果数据类型**：同应用字段。

**应用字段**：不能应用在 timestamp、binary、nchar、bool 类型字段。

**适用于**：表、超级表。

**使用说明**:

- 输出结果行数是范围内总行数减一，第一行没有结果输出。
- 从 2.1.3.0 版本开始，DIFF 函数可以在由 GROUP BY 划分出单独时间线的情况下用于超级表（也即 GROUP BY tbname）。
- 从 2.6.0 开始，DIFF 函数支持 ignore_negative 参数

**示例**：

  ```sql
  taos> SELECT DIFF(current) FROM d1001;
          ts            |    diff(current)     |
  =================================================
  2018-10-03 14:38:15.000 |              2.30000 |
  2018-10-03 14:38:16.800 |             -0.30000 |
  Query OK, 2 row(s) in set (0.001162s)
  ```

### DERIVATIVE

```
SELECT DERIVATIVE(field_name, time_interval, ignore_negative) FROM tb_name [WHERE clause];
```

**功能说明**：统计表中某列数值的单位变化率。其中单位时间区间的长度可以通过 time_interval 参数指定，最小可以是 1 秒（1s）；ignore_negative 参数的值可以是 0 或 1，为 1 时表示忽略负值。

**返回数据类型**：双精度浮点数。

**应用字段**：不能应用在 timestamp、binary、nchar、bool 类型字段。

**适用于**：表、超级表

**使用说明**:

- 从 2.1.3.0 及以后版本可用；输出结果行数是范围内总行数减一，第一行没有结果输出。
- DERIVATIVE 函数可以在由 GROUP BY 划分出单独时间线的情况下用于超级表（也即 GROUP BY tbname）。

**示例**：

```
taos> select derivative(current, 10m, 0) from t1;
           ts            | derivative(current, 10m, 0) |
========================================================
 2021-08-20 10:11:22.790 |                 0.500000000 |
 2021-08-20 11:11:22.791 |                 0.166666620 |
 2021-08-20 12:11:22.791 |                 0.000000000 |
 2021-08-20 13:11:22.792 |                 0.166666620 |
 2021-08-20 14:11:22.792 |                -0.666666667 |
Query OK, 5 row(s) in set (0.004883s)
```

### SPREAD

```
SELECT SPREAD(field_name) FROM { tb_name | stb_name } [WHERE clause];
```

**功能说明**：统计表/超级表中某列的最大值和最小值之差。

**返回数据类型**：双精度浮点数。

**应用字段**：不能应用在 binary、nchar、bool 类型字段。

**适用于**：表、超级表。

**使用说明**：可用于 TIMESTAMP 字段，此时表示记录的时间覆盖范围。

**示例**：

```
taos> SELECT SPREAD(voltage) FROM meters;
    spread(voltage)      |
============================
            5.000000000 |
Query OK, 1 row(s) in set (0.001792s)

taos> SELECT SPREAD(voltage) FROM d1001;
    spread(voltage)      |
============================
            3.000000000 |
Query OK, 1 row(s) in set (0.000836s)
```

### CEIL

```
SELECT CEIL(field_name) FROM { tb_name | stb_name } [WHERE clause];
```

**功能说明**：获得指定列的向上取整数的结果。

**返回结果类型**：与指定列的原始数据类型一致。例如，如果指定列的原始数据类型为 Float，那么返回的数据类型也为 Float；如果指定列的原始数据类型为 Double，那么返回的数据类型也为 Double。

**适用数据类型**：不能应用在 timestamp、binary、nchar、bool 类型字段上；在超级表查询中使用时，不能应用在 tag 列，无论 tag 列的类型是什么类型。

**适用于**: 普通表、超级表。

**嵌套子查询支持**：适用于内层查询和外层查询。

**使用说明**:

- 支持 +、-、\*、/ 运算，如 ceil(col1) + ceil(col2)。
- 只能与普通列，选择（Selection）、投影（Projection）函数一起使用，不能与聚合（Aggregation）函数一起使用。

### FLOOR

```
SELECT FLOOR(field_name) FROM { tb_name | stb_name } [WHERE clause];
```

**功能说明**：获得指定列的向下取整数的结果。  
 其他使用说明参见 CEIL 函数描述。

### ROUND

```
SELECT ROUND(field_name) FROM { tb_name | stb_name } [WHERE clause];
```

**功能说明**：获得指定列的四舍五入的结果。  
 其他使用说明参见 CEIL 函数描述。

### CSUM

```sql
    SELECT CSUM(field_name) FROM { tb_name | stb_name } [WHERE clause]
```

  **功能说明**：累加和（Cumulative sum），输出行与输入行数相同。

  **返回结果类型**： 输入列如果是整数类型返回值为长整型 （int64_t），浮点数返回值为双精度浮点数（Double）。无符号整数类型返回值为无符号长整型（uint64_t）。 返回结果中同时带有每行记录对应的时间戳。

  **适用数据类型**：不能应用在 timestamp、binary、nchar、bool 类型字段上；在超级表查询中使用时，不能应用在标签之上。

  **嵌套子查询支持**： 适用于内层查询和外层查询。

  **使用说明**： 
  
  - 不支持 +、-、*、/ 运算，如 csum(col1) + csum(col2)。
  - 只能与聚合（Aggregation）函数一起使用。 该函数可以应用在普通表和超级表上。 
  - 使用在超级表上的时候，需要搭配 Group by tbname使用，将结果强制规约到单个时间线。

**支持版本**： 从2.3.0.x开始支持

### MAVG

```sql
    SELECT MAVG(field_name, K) FROM { tb_name | stb_name } [WHERE clause]
```

  **功能说明**： 计算连续 k 个值的移动平均数（moving average）。如果输入行数小于 k，则无结果输出。参数 k 的合法输入范围是 1≤ k ≤ 1000。

  **返回结果类型**： 返回双精度浮点数类型。

  **适用数据类型**： 不能应用在 timestamp、binary、nchar、bool 类型上；在超级表查询中使用时，不能应用在标签之上。

  **嵌套子查询支持**： 适用于内层查询和外层查询。

  **使用说明**： 
  
  - 不支持 +、-、*、/ 运算，如 mavg(col1, k1) + mavg(col2, k1); 
  - 只能与普通列，选择（Selection）、投影（Projection）函数一起使用，不能与聚合（Aggregation）函数一起使用；
  - 该函数可以应用在普通表和超级表上；使用在超级表上的时候，需要搭配 Group by tbname使用，将结果强制规约到单个时间线。

**支持版本**： 从2.3.0.x开始支持

### SAMPLE

```sql
    SELECT SAMPLE(field_name, K) FROM { tb_name | stb_name } [WHERE clause]
```

  **功能说明**： 获取数据的 k 个采样值。参数 k 的合法输入范围是 1≤ k ≤ 1000。

  **返回结果类型**： 同原始数据类型， 返回结果中带有该行记录的时间戳。

  **适用数据类型**： 在超级表查询中使用时，不能应用在标签之上。

  **嵌套子查询支持**： 适用于内层查询和外层查询。

  **使用说明**： 
  
  - 不能参与表达式计算；该函数可以应用在普通表和超级表上；
  - 使用在超级表上的时候，需要搭配 Group by tbname 使用，将结果强制规约到单个时间线。

**支持版本**： 从2.3.0.x开始支持

### ASIN

```sql
  SELECT ASIN(field_name) FROM { tb_name | stb_name } [WHERE clause]
```

**功能说明**：获得指定列的反正弦结果

**返回结果类型**：DOUBLE。如果输入值为 NULL，输出值也为 NULL

**适用数据类型**：不能应用在 timestamp、binary、nchar、bool 类型字段上；在超级表查询中使用时，不能应用在 tag 列

**嵌套子查询支持**：适用于内层查询和外层查询。

**使用说明**：

- 只能与普通列，选择（Selection）、投影（Projection）函数一起使用，不能与聚合（Aggregation）函数一起使用。
- 该函数可以应用在普通表和超级表上。
- 版本2.6.0.x后支持

### ACOS

```sql
  SELECT ACOS(field_name) FROM { tb_name | stb_name } [WHERE clause]
```

**功能说明**：获得指定列的反余弦结果

**返回结果类型**：DOUBLE。如果输入值为 NULL，输出值也为 NULL

**适用数据类型**：不能应用在 timestamp、binary、nchar、bool 类型字段上；在超级表查询中使用时，不能应用在 tag 列

**嵌套子查询支持**：适用于内层查询和外层查询。

**使用说明**：

- 只能与普通列，选择（Selection）、投影（Projection）函数一起使用，不能与聚合（Aggregation）函数一起使用。
- 该函数可以应用在普通表和超级表上。
- 版本2.6.0.x后支持

### ATAN

```sql
  SELECT ATAN(field_name) FROM { tb_name | stb_name } [WHERE clause]
```

**功能说明**：获得指定列的反正切结果

**返回结果类型**：DOUBLE。如果输入值为 NULL，输出值也为 NULL

**适用数据类型**：不能应用在 timestamp、binary、nchar、bool 类型字段上；在超级表查询中使用时，不能应用在 tag 列

**嵌套子查询支持**：适用于内层查询和外层查询。

**使用说明**：

- 只能与普通列，选择（Selection）、投影（Projection）函数一起使用，不能与聚合（Aggregation）函数一起使用。
- 该函数可以应用在普通表和超级表上。
- 版本2.6.0.x后支持

### SIN

```sql
  SELECT SIN(field_name) FROM { tb_name | stb_name } [WHERE clause]
```

**功能说明**：获得指定列的正弦结果

**返回结果类型**：DOUBLE。如果输入值为 NULL，输出值也为 NULL

**适用数据类型**：不能应用在 timestamp、binary、nchar、bool 类型字段上；在超级表查询中使用时，不能应用在 tag 列

**嵌套子查询支持**：适用于内层查询和外层查询。

**使用说明**：

- 只能与普通列，选择（Selection）、投影（Projection）函数一起使用，不能与聚合（Aggregation）函数一起使用。
- 该函数可以应用在普通表和超级表上。
- 版本2.6.0.x后支持

### COS

```sql
  SELECT COS(field_name) FROM { tb_name | stb_name } [WHERE clause]
```

**功能说明**：获得指定列的余弦结果

**返回结果类型**：DOUBLE。如果输入值为 NULL，输出值也为 NULL

**适用数据类型**：不能应用在 timestamp、binary、nchar、bool 类型字段上；在超级表查询中使用时，不能应用在 tag 列

**嵌套子查询支持**：适用于内层查询和外层查询。

**使用说明**：

- 只能与普通列，选择（Selection）、投影（Projection）函数一起使用，不能与聚合（Aggregation）函数一起使用。
- 该函数可以应用在普通表和超级表上。
- 版本2.6.0.x后支持

### TAN

```sql
  SELECT TAN(field_name) FROM { tb_name | stb_name } [WHERE clause]
```

**功能说明**：获得指定列的正切结果

**返回结果类型**：DOUBLE。如果输入值为 NULL，输出值也为 NULL

**适用数据类型**：不能应用在 timestamp、binary、nchar、bool 类型字段上；在超级表查询中使用时，不能应用在 tag 列

**嵌套子查询支持**：适用于内层查询和外层查询。

**使用说明**：

- 只能与普通列，选择（Selection）、投影（Projection）函数一起使用，不能与聚合（Aggregation）函数一起使用。
- 该函数可以应用在普通表和超级表上。
- 版本2.6.0.x后支持

### POW

```sql
  SELECT POW(field_name, power) FROM { tb_name | stb_name } [WHERE clause]
```

**功能说明**：获得指定列的指数为 power 的幂

**返回结果类型**：DOUBLE。如果输入值为 NULL，输出值也为 NULL

**适用数据类型**：不能应用在 timestamp、binary、nchar、bool 类型字段上；在超级表查询中使用时，不能应用在 tag 列

**嵌套子查询支持**：适用于内层查询和外层查询。

**使用说明**：

- 只能与普通列，选择（Selection）、投影（Projection）函数一起使用，不能与聚合（Aggregation）函数一起使用。
- 该函数可以应用在普通表和超级表上。
- 版本2.6.0.x后支持

### LOG

```sql
  SELECT LOG(field_name, base) FROM { tb_name | stb_name } [WHERE clause]
```

**功能说明**：获得指定列对于底数 base 的对数

**返回结果类型**：DOUBLE。如果输入值为 NULL，输出值也为 NULL

**适用数据类型**：不能应用在 timestamp、binary、nchar、bool 类型字段上；在超级表查询中使用时，不能应用在 tag 列

**嵌套子查询支持**：适用于内层查询和外层查询。

**使用说明**：

- 只能与普通列，选择（Selection）、投影（Projection）函数一起使用，不能与聚合（Aggregation）函数一起使用。
- 该函数可以应用在普通表和超级表上。
- 版本2.6.0.x后支持

### ABS

```sql
  SELECT ABS(field_name) FROM { tb_name | stb_name } [WHERE clause]
```

**功能说明**：获得指定列的绝对值

**返回结果类型**：如果输入值为整数，输出值是 UBIGINT 类型。如果输入值是 FLOAT/DOUBLE 数据类型，输出值是 DOUBLE 数据类型。

**适用数据类型**：不能应用在 timestamp、binary、nchar、bool 类型字段上；在超级表查询中使用时，不能应用在 tag 列

**嵌套子查询支持**：适用于内层查询和外层查询。

**使用说明**：

- 只能与普通列，选择（Selection）、投影（Projection）函数一起使用，不能与聚合（Aggregation）函数一起使用。
- 该函数可以应用在普通表和超级表上。
- 版本2.6.0.x后支持

### SQRT

```sql
  SELECT SQRT(field_name) FROM { tb_name | stb_name } [WHERE clause]
```

**功能说明**：获得指定列的平方根

**返回结果类型**：DOUBLE。如果输入值为 NULL，输出值也为 NULL

**适用数据类型**：不能应用在 timestamp、binary、nchar、bool 类型字段上；在超级表查询中使用时，不能应用在 tag 列

**嵌套子查询支持**：适用于内层查询和外层查询。

**使用说明**：

- 只能与普通列，选择（Selection）、投影（Projection）函数一起使用，不能与聚合（Aggregation）函数一起使用。
- 该函数可以应用在普通表和超级表上。
- 版本2.6.0.x后支持

### CAST

```sql
  SELECT CAST(expression AS type_name) FROM { tb_name | stb_name } [WHERE clause]
```

**功能说明**：数据类型转换函数，输入参数 expression 支持普通列、常量、标量函数及它们之间的四则运算，不支持 tag 列，只适用于 select 子句中。

**返回结果类型**：CAST 中指定的类型（type_name）。

**适用数据类型**：

- 输入参数 expression 的类型可以是除 JSON 外目前所有类型字段（BOOL/TINYINT/SMALLINT/INT/BIGINT/FLOAT/DOUBLE/BINARY(M)/TIMESTAMP/NCHAR(M)/TINYINT UNSIGNED/SMALLINT UNSIGNED/INT UNSIGNED/BIGINT UNSIGNED）; 
- 输出目标类型只支持 BIGINT/BINARY(N)/TIMESTAMP/NCHAR(N)/BIGINT UNSIGNED。

**使用说明**：

- 对于不能支持的类型转换会直接报错。
- 如果输入值为NULL则输出值也为NULL。
- 对于类型支持但某些值无法正确转换的情况对应的转换后的值以转换函数输出为准。目前可能遇到的几种情况：
        1）BINARY/NCHAR转BIGINT/BIGINT UNSIGNED时可能出现的无效字符情况，例如"a"可能转为0。
        2）有符号数或TIMESTAMP转BIGINT UNSIGNED可能遇到的溢出问题。
        3）BIGINT UNSIGNED转BIGINT可能遇到的溢出问题。
        4）FLOAT/DOUBLE转BIGINT/BIGINT UNSIGNED可能遇到的溢出问题。
- 版本2.6.0.x后支持

### CONCAT

```sql
  SELECT CONCAT(str1|column1, str2|column2, ...) FROM { tb_name | stb_name } [WHERE clause]
```

**功能说明**：字符串连接函数。

**返回结果类型**：同输入参数类型，BINARY 或者 NCHAR。

**适用数据类型**：输入参数或者全部是 BINARY 格式的字符串或者列，或者全部是 NCHAR 格式的字符串或者列。不能应用在 TAG 列。

**使用说明**：

- 如果输入值为NULL，输出值为NULL。
- 该函数最小参数个数为2个，最大参数个数为8个。
- 该函数可以应用在普通表和超级表上。
- 该函数适用于内层查询和外层查询。
- 版本2.6.0.x后支持

### CONCAT_WS

```
  SELECT CONCAT_WS(separator, str1|column1, str2|column2, ...) FROM { tb_name | stb_name } [WHERE clause]
```

**功能说明**：带分隔符的字符串连接函数。

**返回结果类型**：同输入参数类型，BINARY 或者 NCHAR。

**适用数据类型**：输入参数或者全部是 BINARY 格式的字符串或者列，或者全部是 NCHAR 格式的字符串或者列。不能应用在 TAG 列。

**使用说明**：

- 如果separator值为NULL，输出值为NULL。如果separator值不为NULL，其他输入为NULL，输出为空串
- 该函数最小参数个数为3个，最大参数个数为9个。
- 该函数可以应用在普通表和超级表上。
- 该函数适用于内层查询和外层查询。
- 版本2.6.0.x后支持

### LENGTH

```
  SELECT LENGTH(str|column) FROM { tb_name | stb_name } [WHERE clause]
```

**功能说明**：以字节计数的字符串长度。

**返回结果类型**：INT。

**适用数据类型**：输入参数是 BINARY 类型或者 NCHAR 类型的字符串或者列。不能应用在 TAG 列。

**使用说明**

- 如果输入值为NULL，输出值为NULL。
- 该函数可以应用在普通表和超级表上。
- 函数适用于内层查询和外层查询。
- 版本2.6.0.x后支持

### CHAR_LENGTH

```
  SELECT CHAR_LENGTH(str|column) FROM { tb_name | stb_name } [WHERE clause]
```

**功能说明**：以字符计数的字符串长度。

**返回结果类型**：INT。

**适用数据类型**：输入参数是 BINARY 类型或者 NCHAR 类型的字符串或者列。不能应用在 TAG 列。

**使用说明**

- 如果输入值为NULL，输出值为NULL。
- 该函数可以应用在普通表和超级表上。
- 该函数适用于内层查询和外层查询。
- 版本2.6.0.x后支持

### LOWER

```
  SELECT LOWER(str|column) FROM { tb_name | stb_name } [WHERE clause]
```

**功能说明**：将字符串参数值转换为全小写字母。

**返回结果类型**：同输入类型。

**适用数据类型**：输入参数是 BINARY 类型或者 NCHAR 类型的字符串或者列。不能应用在 TAG 列。

**使用说明**：

- 如果输入值为NULL，输出值为NULL。
- 该函数可以应用在普通表和超级表上。
- 该函数适用于内层查询和外层查询。
- 版本2.6.0.x后支持

### UPPER

```
  SELECT UPPER(str|column) FROM { tb_name | stb_name } [WHERE clause]
```

**功能说明**：将字符串参数值转换为全大写字母。

**返回结果类型**：同输入类型。

**适用数据类型**：输入参数是 BINARY 类型或者 NCHAR 类型的字符串或者列。不能应用在 TAG 列。

**使用说明**：

- 如果输入值为NULL，输出值为NULL。
- 该函数可以应用在普通表和超级表上。
- 该函数适用于内层查询和外层查询。
- 版本2.6.0.x后支持

### LTRIM

```
  SELECT LTRIM(str|column) FROM { tb_name | stb_name } [WHERE clause]
```

**功能说明**：返回清除左边空格后的字符串。

**返回结果类型**：同输入类型。

**适用数据类型**：输入参数是 BINARY 类型或者 NCHAR 类型的字符串或者列。不能应用在 TAG 列。

**使用说明**：

- 如果输入值为NULL，输出值为NULL。
- 该函数可以应用在普通表和超级表上。
- 该函数适用于内层查询和外层查询。
- 版本2.6.0.x后支持

### RTRIM

```
  SELECT RTRIM(str|column) FROM { tb_name | stb_name } [WHERE clause]
```

**功能说明**：返回清除右边空格后的字符串。

**返回结果类型**：同输入类型。

**适用数据类型**：输入参数是 BINARY 类型或者 NCHAR 类型的字符串或者列。不能应用在 TAG 列。

**使用说明**：

- 如果输入值为NULL，输出值为NULL。
- 该函数可以应用在普通表和超级表上。
- 该函数适用于内层查询和外层查询。
- 版本2.6.0.x后支持

### SUBSTR

```
  SELECT SUBSTR(str,pos[,len]) FROM { tb_name | stb_name } [WHERE clause]
```

**功能说明**：从源字符串 str 中的指定位置 pos 开始取一个长度为 len 的子串并返回。

**返回结果类型**：同输入类型。

**适用数据类型**：输入参数是 BINARY 类型或者 NCHAR 类型的字符串或者列。不能应用在 TAG 列。

**使用说明**：

- 如果输入值为NULL，输出值为NULL。
- 输入参数pos可以为正数，也可以为负数。如果pos是正数，表示开始位置从字符串开头正数计算。如果pos为负数，表示开始位置从字符串结尾倒数计算。如果输入参数len被忽略，返回的子串包含从pos开始的整个字串。
- 该函数可以应用在普通表和超级表上。
- 该函数适用于内层查询和外层查询。
- 版本2.6.0.x后支持

### 四则运算

```
SELECT field_name [+|-|*|/|%][Value|field_name] FROM { tb_name | stb_name }  [WHERE clause];
```

**功能说明**：统计表/超级表中某列或多列间的值加、减、乘、除、取余计算结果。

**返回数据类型**：双精度浮点数。

**应用字段**：不能应用在 timestamp、binary、nchar、bool 类型字段。

**适用于**：表、超级表。

**使用说明**：

- 支持两列或多列之间进行计算，可使用括号控制计算优先级；
- NULL 字段不参与计算，如果参与计算的某行中包含 NULL，该行的计算结果为 NULL。

```
taos> SELECT current + voltage * phase FROM d1001;
(current+(voltage*phase)) |
============================
            78.190000713 |
            84.540003240 |
            80.810000718 |
Query OK, 3 row(s) in set (0.001046s)
```

### STATECOUNT

```
SELECT STATECOUNT(field_name, oper, val) FROM { tb_name | stb_name } [WHERE clause];
```

**功能说明**：返回满足某个条件的连续记录的个数，结果作为新的一列追加在每行后面。条件根据参数计算，如果条件为 true 则加 1，条件为 false 则重置为-1，如果数据为 NULL，跳过该条数据。

**参数范围**：

- oper : LT (小于)、GT（大于）、LE（小于等于）、GE（大于等于）、NE（不等于）、EQ（等于），不区分大小写。
- val : 数值型

**返回结果类型**：整形。

**适用数据类型**：不能应用在 timestamp、binary、nchar、bool 类型字段上。

**嵌套子查询支持**：不支持应用在子查询上。

**支持的版本**：2.6 开始的版本。

**使用说明**：

- 该函数可以应用在普通表上，在由 GROUP BY 划分出单独时间线的情况下用于超级表（也即 GROUP BY tbname）
- 不能和窗口操作一起使用，例如 interval/state_window/session_window。

**示例**：

```
taos> select ts,dbig from statef2;
          ts               |         dbig          |
========================================================
2021-10-15 00:31:33.000000000 |                     1 |
2021-10-17 00:31:31.000000000 |                  NULL |
2021-12-24 00:31:34.000000000 |                     2 |
2022-01-01 08:00:05.000000000 |                    19 |
2022-01-01 08:00:06.000000000 |                  NULL |
2022-01-01 08:00:07.000000000 |                     9 |
Query OK, 6 row(s) in set (0.002977s)

taos> select stateCount(dbig,GT,2) from statef2;
ts               |         dbig          | statecount(dbig,gt,2) |
================================================================================
2021-10-15 00:31:33.000000000 |                     1 |                    -1 |
2021-10-17 00:31:31.000000000 |                  NULL |                  NULL |
2021-12-24 00:31:34.000000000 |                     2 |                    -1 |
2022-01-01 08:00:05.000000000 |                    19 |                     1 |
2022-01-01 08:00:06.000000000 |                  NULL |                  NULL |
2022-01-01 08:00:07.000000000 |                     9 |                     2 |
Query OK, 6 row(s) in set (0.002791s)
```

### STATEDURATION

```sql
SELECT stateDuration(field_name, oper, val, unit) FROM { tb_name | stb_name } [WHERE clause];
```

**功能说明**：返回满足某个条件的连续记录的时间长度，结果作为新的一列追加在每行后面。条件根据参数计算，如果条件为 true 则加上两个记录之间的时间长度（第一个满足条件的记录时间长度记为 0），条件为 false 则重置为-1，如果数据为 NULL，跳过该条数据。

**参数范围**：

- oper : LT (小于)、GT（大于）、LE（小于等于）、GE（大于等于）、NE（不等于）、EQ（等于），不区分大小写。
- val : 数值型
- unit : 时间长度的单位，范围[1s、1m、1h ]，不足一个单位舍去。默认为 1s。

**返回结果类型**：整形。

**适用数据类型**：不能应用在 timestamp、binary、nchar、bool 类型字段上。

**嵌套子查询支持**：不支持应用在子查询上。

**支持的版本**：2.6 开始的版本。

**使用说明**：

- 该函数可以应用在普通表上，在由 GROUP BY 划分出单独时间线的情况下用于超级表（也即 GROUP BY tbname）
- 不能和窗口操作一起使用，例如 interval/state_window/session_window。

**示例**：

```
taos> select ts,dbig from statef2;
          ts               |         dbig          |
========================================================
2021-10-15 00:31:33.000000000 |                     1 |
2021-10-17 00:31:31.000000000 |                  NULL |
2021-12-24 00:31:34.000000000 |                     2 |
2022-01-01 08:00:05.000000000 |                    19 |
2022-01-01 08:00:06.000000000 |                  NULL |
2022-01-01 08:00:07.000000000 |                     9 |
Query OK, 6 row(s) in set (0.002407s)

taos> select stateDuration(dbig,GT,2) from statef2;
ts               |         dbig          | stateduration(dbig,gt,2) |
===================================================================================
2021-10-15 00:31:33.000000000 |                     1 |                       -1 |
2021-10-17 00:31:31.000000000 |                  NULL |                     NULL |
2021-12-24 00:31:34.000000000 |                     2 |                       -1 |
2022-01-01 08:00:05.000000000 |                    19 |                        0 |
2022-01-01 08:00:06.000000000 |                  NULL |                     NULL |
2022-01-01 08:00:07.000000000 |                     9 |                        2 |
Query OK, 6 row(s) in set (0.002613s)
```

## 时间函数

从 2.6.0.0 版本开始，TDengine 查询引擎支持以下时间相关函数：

### NOW

```sql
SELECT NOW() FROM { tb_name | stb_name } [WHERE clause];
SELECT select_expr FROM { tb_name | stb_name } WHERE ts_col cond_operatior NOW();
INSERT INTO tb_name VALUES (NOW(), ...);
```

**功能说明**：返回客户端当前系统时间。

**返回结果数据类型**：TIMESTAMP 时间戳类型。

**应用字段**：在 WHERE 或 INSERT 语句中使用时只能作用于 TIMESTAMP 类型的字段。

**适用于**：表、超级表。

**使用说明**：

- 支持时间加减操作，如 NOW() + 1s, 支持的时间单位如下：
        b(纳秒)、u(微秒)、a(毫秒)、s(秒)、m(分)、h(小时)、d(天)、w(周)。
- 返回的时间戳精度与当前 DATABASE 设置的时间精度一致。

**示例**：

```sql
taos> SELECT NOW() FROM meters;
          now()          |
==========================
  2022-02-02 02:02:02.456 |
Query OK, 1 row(s) in set (0.002093s)

taos> SELECT NOW() + 1h FROM meters;
       now() + 1h         |
==========================
  2022-02-02 03:02:02.456 |
Query OK, 1 row(s) in set (0.002093s)

taos> SELECT COUNT(voltage) FROM d1001 WHERE ts < NOW();
        count(voltage)       |
=============================
                           5 |
Query OK, 5 row(s) in set (0.004475s)

taos> INSERT INTO d1001 VALUES (NOW(), 10.2, 219, 0.32);
Query OK, 1 of 1 row(s) in database (0.002210s)
```

### TODAY

```sql
SELECT TODAY() FROM { tb_name | stb_name } [WHERE clause];
SELECT select_expr FROM { tb_name | stb_name } WHERE ts_col cond_operatior TODAY()];
INSERT INTO tb_name VALUES (TODAY(), ...);
```

**功能说明**：返回客户端当日零时的系统时间。

**返回结果数据类型**：TIMESTAMP 时间戳类型。

**应用字段**：在 WHERE 或 INSERT 语句中使用时只能作用于 TIMESTAMP 类型的字段。

**适用于**：表、超级表。

**使用说明**：

- 支持时间加减操作，如 TODAY() + 1s, 支持的时间单位如下：
                b(纳秒)，u(微秒)，a(毫秒)，s(秒)，m(分)，h(小时)，d(天)，w(周)。
- 返回的时间戳精度与当前 DATABASE 设置的时间精度一致。

**示例**：

```sql
taos> SELECT TODAY() FROM meters;
         today()          |
==========================
  2022-02-02 00:00:00.000 |
Query OK, 1 row(s) in set (0.002093s)

taos> SELECT TODAY() + 1h FROM meters;
      today() + 1h        |
==========================
  2022-02-02 01:00:00.000 |
Query OK, 1 row(s) in set (0.002093s)

taos> SELECT COUNT(voltage) FROM d1001 WHERE ts < TODAY();
        count(voltage)       |
=============================
                           5 |
Query OK, 5 row(s) in set (0.004475s)

taos> INSERT INTO d1001 VALUES (TODAY(), 10.2, 219, 0.32);
Query OK, 1 of 1 row(s) in database (0.002210s)
```

### TIMEZONE

```sql
SELECT TIMEZONE() FROM { tb_name | stb_name } [WHERE clause];
```

**功能说明**：返回客户端当前时区信息。

**返回结果数据类型**：BINARY 类型。

**应用字段**：无

**适用于**：表、超级表。

**示例**：

```sql
taos> SELECT TIMEZONE() FROM meters;
           timezone()           |
=================================
 UTC (UTC, +0000)               |
Query OK, 1 row(s) in set (0.002093s)
```

### TO_ISO8601

```sql
SELECT TO_ISO8601(ts_val | ts_col) FROM { tb_name | stb_name } [WHERE clause];
```

**功能说明**：将 UNIX 时间戳转换成为 ISO8601 标准的日期时间格式，并附加客户端时区信息。

**返回结果数据类型**：BINARY 类型。

**应用字段**：UNIX 时间戳常量或是 TIMESTAMP 类型的列

**适用于**：表、超级表。

**使用说明**：

- 如果输入是 UNIX 时间戳常量，返回格式精度由时间戳的位数决定; 
- 如果输入是 TIMSTAMP 类型的列，返回格式的时间戳精度与当前 DATABASE 设置的时间精度一致。

**示例**：

```sql
taos> SELECT TO_ISO8601(1643738400) FROM meters;
   to_iso8601(1643738400)    |
==============================
 2022-02-02T02:00:00+0800    |

taos> SELECT TO_ISO8601(ts) FROM meters;
       to_iso8601(ts)        |
==============================
 2022-02-02T02:00:00+0800    |
 2022-02-02T02:00:00+0800    |
 2022-02-02T02:00:00+0800    |
```

### TO_UNIXTIMESTAMP

```sql
SELECT TO_UNIXTIMESTAMP(datetime_string | ts_col) FROM { tb_name | stb_name } [WHERE clause];
```

**功能说明**：将日期时间格式的字符串转换成为 UNIX 时间戳。

**返回结果数据类型**：长整型 INT64。

**应用字段**：字符串常量或是 BINARY/NCHAR 类型的列。

**适用于**：表、超级表。

**使用说明**：

- 输入的日期时间字符串须符合 ISO8601/RFC3339 标准，无法转换的字符串格式将返回 0。
- 返回的时间戳精度与当前 DATABASE 设置的时间精度一致。

**示例**：

```sql
taos> SELECT TO_UNIXTIMESTAMP("2022-02-02T02:00:00.000Z") FROM meters;
to_unixtimestamp("2022-02-02T02:00:00.000Z") |
==============================================
                               1643767200000 |

taos> SELECT TO_UNIXTIMESTAMP(col_binary) FROM meters;
      to_unixtimestamp(col_binary)     |
========================================
                         1643767200000 |
                         1643767200000 |
                         1643767200000 |
```

### TIMETRUNCATE

```sql
SELECT TIMETRUNCATE(ts_val | datetime_string | ts_col, time_unit) FROM { tb_name | stb_name } [WHERE clause];
```

**功能说明**：将时间戳按照指定时间单位 time_unit 进行截断。

**返回结果数据类型**：TIMESTAMP 时间戳类型。

**应用字段**：UNIX 时间戳，日期时间格式的字符串，或者 TIMESTAMP 类型的列。

**适用于**：表、超级表。

**使用说明**：
- 支持的时间单位 time_unit 如下：
          1u(微秒)，1a(毫秒)，1s(秒)，1m(分)，1h(小时)，1d(天)。
- 返回的时间戳精度与当前 DATABASE 设置的时间精度一致。

**示例**：

```sql
taos> SELECT TIMETRUNCATE(1643738522000, 1h) FROM meters;
  timetruncate(1643738522000, 1h) |
===================================
      2022-02-02 02:00:00.000     |
Query OK, 1 row(s) in set (0.001499s)

taos> SELECT TIMETRUNCATE("2022-02-02 02:02:02", 1h) FROM meters;
  timetruncate("2022-02-02 02:02:02", 1h) |
===========================================
      2022-02-02 02:00:00.000             |
Query OK, 1 row(s) in set (0.003903s)

taos> SELECT TIMETRUNCATE(ts, 1h) FROM meters;
  timetruncate(ts, 1h)   |
==========================
 2022-02-02 02:00:00.000 |
 2022-02-02 02:00:00.000 |
 2022-02-02 02:00:00.000 |
Query OK, 3 row(s) in set (0.003903s)
```

### TIMEDIFF

```sql
SELECT TIMEDIFF(ts_val1 | datetime_string1 | ts_col1, ts_val2 | datetime_string2 | ts_col2 [, time_unit]) FROM { tb_name | stb_name } [WHERE clause];
```

**功能说明**：计算两个时间戳之间的差值，并近似到时间单位 time_unit 指定的精度。

**返回结果数据类型**：长整型 INT64。

**应用字段**：UNIX 时间戳，日期时间格式的字符串，或者 TIMESTAMP 类型的列。

**适用于**：表、超级表。

**使用说明**：
- 支持的时间单位 time_unit 如下：
          1u(微秒)，1a(毫秒)，1s(秒)，1m(分)，1h(小时)，1d(天)。
- 如果时间单位 time_unit 未指定， 返回的时间差值精度与当前 DATABASE 设置的时间精度一致。

**支持的版本**：2.6.0.0 及以后的版本。

**示例**：

```sql
taos> SELECT TIMEDIFF(1643738400000, 1643742000000) FROM meters;
 timediff(1643738400000, 1643742000000) |
=========================================
                                3600000 |
Query OK, 1 row(s) in set (0.002553s)
taos> SELECT TIMEDIFF(1643738400000, 1643742000000, 1h) FROM meters;
 timediff(1643738400000, 1643742000000, 1h) |
=============================================
                                          1 |
Query OK, 1 row(s) in set (0.003726s)

taos> SELECT TIMEDIFF("2022-02-02 03:00:00", "2022-02-02 02:00:00", 1h) FROM meters;
 timediff("2022-02-02 03:00:00", "2022-02-02 02:00:00", 1h) |
=============================================================
                                                          1 |
Query OK, 1 row(s) in set (0.001937s)

taos> SELECT TIMEDIFF(ts_col1, ts_col2, 1h) FROM meters;
   timediff(ts_col1, ts_col2, 1h) |
===================================
                                1 |
Query OK, 1 row(s) in set (0.001937s)
```
