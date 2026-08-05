---
sidebar_label: 数据查询
title: 数据查询
description: 使用 SQL 快速体验时序数据查询、聚合和时间窗口分析
toc_max_heading_level: 4
---

相较于许多时序数据库和实时数据库，TDengine 自首个版本起就支持标准 SQL 查询。这一能力降低了时序数据查询和分析的学习成本。

本章以智能电表数据模型为例，使用快速体验中 `taosBenchmark -y` 写入的 `test` 库数据，在 shell 中快速体验常用查询：按条件过滤、排序、限制返回行数，按标签或子表聚合，以及按时间窗口统计数据。每类查询都会给出 SQL 和代表性返回结果，帮助你理解查询输出的形态。下面先给出查询能力全景；完整语法与进阶能力请按下列链接深入阅读，或参见文末“继续阅读”。

## 查询能力一览

TDengine 在标准 SQL 之上，针对时序与物联网场景扩展了标签过滤、按设备分片、多种时间窗口、插值与关联查询等能力。

- **基础检索**
  `SELECT` / `WHERE` / `ORDER BY` / `LIMIT`，时间范围过滤，正则与 `CASE` 等。详见 [基础查询](../05-tdengine-sql/04-data-query/01-query.md)。

- **运算符与表达式**
  算术、比较、逻辑、位运算、JSON 与集合运算等。详见 [运算符](../05-tdengine-sql/04-data-query/02-operators.md)。

- **聚合与函数**
  `COUNT` / `AVG` / `MAX` 等统计聚合，以及选择、数学、时间、时序专用等内置函数。详见 [内置函数](../05-tdengine-sql/04-data-query/03-function.md)。

- **标签与分片**
  按标签过滤；`GROUP BY` / `PARTITION BY` / `tbname` / `SLIMIT` 按设备或标签聚合与限流。详见 [基础查询](../05-tdengine-sql/04-data-query/01-query.md)。

- **特色查询**
  `INTERVAL` / `SLIDING` 时间窗口，以及状态、会话、事件、计数、外部窗口；`FILL` / `INTERP` 补值与插值。详见 [特色查询](../05-tdengine-sql/04-data-query/04-distinguished.md)、[基础查询](../05-tdengine-sql/04-data-query/01-query.md)（`FILL` / `INTERP`）。

- **关联查询**
  普通 Join，以及面向时序的 ASOF Join、Window Join。详见 [关联查询](../05-tdengine-sql/04-data-query/05-join.md)。

- **窗口函数**
  `OVER` 窗口函数。详见 [窗口函数](../05-tdengine-sql/04-data-query/06-window-function.md)。

- **自定义函数与缓存**
  用户自定义函数（UDF）；最新行读缓存加速。详见 [自定义函数](../05-tdengine-sql/04-data-query/07-udf.md)、[读缓存](../05-tdengine-sql/04-data-query/08-cache-query.md)。

- **执行计划**
  `EXPLAIN` / `EXPLAIN ANALYZE` 查看执行计划。详见 [执行计划](../05-tdengine-sql/04-data-query/09-explain.md)。

相对通用数据库，查询时序数据时尤其常用：**一次查询超级表覆盖多设备**、**用标签缩小设备范围**、**按时间或状态切窗口做降采样与聚合**。下文从最常用的过滤、聚合与时间窗口开始上手。

## 前提条件

请先确认已经完成以下准备：

1. TDengine 服务已经启动，可以通过 shell 连接。
2. 已经在下载与安装章节的快速体验中执行过 `taosBenchmark -y`，生成了 `test` 数据库和 `meters` 超级表。若尚未生成，可先在终端执行 `taosBenchmark -y`。

该命令默认写入约 1 亿条记录：10,000 张子表（`d0`–`d9999`），每张 10,000 条，时间戳范围为 `2017-07-14 10:40:00.000` 到 `2017-07-14 10:40:09.999`（约 10 秒，采集间隔 1 ms）。下文窗口示例因此使用秒级窗口，而不是分钟级窗口。

进入 shell 后，切换到 `test` 数据库。

```sql
USE test;
```

## 基本查询

执行下面的 SQL，从超级表 `meters` 中查询电压大于 250V 的数据，并按时间倒序返回前 5 行。

```sql
SELECT tbname, ts, current, voltage
FROM meters
WHERE voltage > 250 and tbname = 'd1'
ORDER BY ts DESC
LIMIT 5;
```

其中：

- `WHERE voltage > 250` 用于过滤电压大于 250V 的记录。
- `ORDER BY ts DESC` 按时间戳倒序返回结果。
- `LIMIT 5` 只返回前 5 行。

`tbname` 是伪列，表示数据来自哪张子表。
返回结果类似如下，具体子表名和数值可能因 `taosBenchmark` 版本或随机数据不同而略有差异。

```text
 tbname |           ts            | current  | voltage |
========================================================
 d1     | 2017-07-14 10:40:09.998 |  11.7984 |     253 |
 d1     | 2017-07-14 10:40:09.998 |  11.7984 |     253 |
 d1     | 2017-07-14 10:40:09.998 |  11.7984 |     253 |
 d1     | 2017-07-14 10:40:09.998 |  11.7984 |     253 |
 d1     | 2017-07-14 10:40:09.998 |  11.7984 |     253 |

Query OK, 5 row(s) in set
```

## 按标签过滤

标签适合用来描述设备的静态属性，例如位置和分组。下面的 SQL 查询位于 `California.SanFrancisco` 的电表数据。

```sql
SELECT tbname, ts, current, voltage, phase
FROM meters
WHERE location = "California.SanFrancisco"
ORDER BY ts DESC
LIMIT 5;
```

也可以同时使用标签条件和普通列条件。

```sql
SELECT tbname, ts, current, voltage, phase
FROM meters
WHERE location = "California.SanFrancisco" AND voltage > 250
ORDER BY ts DESC
LIMIT 5;
```

返回结果类似如下。

```text
 tbname |           ts            | current  | voltage | phase |
===============================================================
 d3737  | 2017-07-14 10:40:09.998 |  11.7984 |     253 |   147 |
 d8742  | 2017-07-14 10:40:09.998 |  11.7984 |     253 |   147 |
 d8745  | 2017-07-14 10:40:09.998 |  11.7984 |     253 |   147 |
 d6259  | 2017-07-14 10:40:09.998 |  11.7984 |     253 |   147 |
 d6252  | 2017-07-14 10:40:09.998 |  11.7984 |     253 |   147 |

Query OK, 5 row(s) in set
```

## 聚合查询

聚合函数可以帮助你快速计算统计值。下面的 SQL 统计所有电表的平均电压、最大电压和总记录数。

```sql
SELECT AVG(voltage), MAX(voltage), COUNT(*)
FROM meters;
```

返回结果中只有一行，表示全量数据的汇总结果。

```text
 avg(voltage) | max(voltage) |  count(*)  |
===========================================
     243.9314 |          258 |  100000000 |

Query OK, 1 row(s) in set
```

如果需要按分组统计，可以使用 `GROUP BY`。快速体验数据中的分组标签列为 `groupId`。

```sql
SELECT groupId, AVG(voltage), COUNT(*)
FROM meters
GROUP BY groupId
ORDER BY groupId;
```

返回结果会按 `groupId` 分组，每个分组一行。

```text
 groupId | avg(voltage) | count(*) |
====================================
       1 |     243.9314 |  9800000 |
       2 |     243.9314 |  9940000 |
       3 |     243.9314 |  9800000 |
       4 |     243.9314 | 10040000 |
       5 |     243.9314 | 10310000 |
     ...
Query OK, 10 row(s) in set
```

`GROUP BY` 的结果在未排序时不保证固定顺序。如需按统计值排序，可以继续使用 `ORDER BY`。

```sql
SELECT groupId, AVG(voltage) AS avg_voltage
FROM meters
GROUP BY groupId
ORDER BY avg_voltage DESC;
```

## 按子表聚合

如果需要分别统计每个电表，可以使用 `PARTITION BY tbname`。下面的 SQL 按子表统计平均电压，并用 `SLIMIT` 只取前几个分片，避免一次返回 10,000 行。

```sql
SELECT tbname, AVG(voltage), COUNT(*)
FROM meters
PARTITION BY tbname
SLIMIT 3;
```

返回结果会按子表切分。分片出现顺序可能因环境略有不同，示意如下。

```text
 tbname | avg(voltage) | count(*) |
===================================
 d0     |     243.9314 |    10000 |
 d1     |     243.9314 |    10000 |
 d2     |     243.9314 |    10000 |

Query OK, 3 row(s) in set
```

`PARTITION BY` 会先把超级表数据按指定维度切分，再在每个分片中执行计算。它常用于“每台设备分别统计”的场景。

## 窗口查询

窗口查询用于把时序数据按时间、状态、事件或行数切分，再在每个窗口内计算。快速上手阶段可以先理解下面几类窗口：

![常用窗口划分逻辑](../assets/query-and-aggregate-01.png)

- 时间窗口：按固定时间间隔切分，使用 `INTERVAL`。
- 滑动窗口：在时间窗口基础上设置滑动步长，使用 `SLIDING`。
- 状态窗口：按状态值变化切分，使用 `STATE_WINDOW`。
- 会话窗口：按相邻记录时间间隔切分，使用 `SESSION`。
- 事件窗口：按开始条件和结束条件切分，使用 `EVENT_WINDOW`。
- 计数窗口：按固定行数切分，使用 `COUNT_WINDOW`。
- 外部窗口：由子查询显式给出窗口范围，使用 `EXTERNAL_WINDOW`。

下面先体验最常用的几种窗口。示例时间范围覆盖 `test.meters` 的全部数据区间。

### 时间窗口

下面的 SQL 按 1 秒窗口计算每个电表的平均电压。

```sql
SELECT tbname, _wstart, _wend, AVG(voltage)
FROM meters
WHERE ts >= "2017-07-14 10:40:00" AND ts < "2017-07-14 10:40:10"
PARTITION BY tbname
INTERVAL(1s)
SLIMIT 2;
```

其中：

- `INTERVAL(1s)` 表示按 1 秒切分时间窗口。
- `_wstart` 和 `_wend` 是窗口开始时间和结束时间。
- `PARTITION BY tbname` 表示每个子表独立做窗口聚合。
- `SLIMIT 2` 只返回前 2 个分片，避免结果过长。

返回结果中每一行对应一个时间窗口，节选如下。

```text
 tbname |        _wstart         |         _wend          | avg(voltage) |
==========================================================================
 d0     | 2017-07-14 10:40:00.000 | 2017-07-14 10:40:01.000 |     244.003 |
 d0     | 2017-07-14 10:40:01.000 | 2017-07-14 10:40:02.000 |     243.872 |
 d0     | 2017-07-14 10:40:02.000 | 2017-07-14 10:40:03.000 |     244.261 |
 d1     | 2017-07-14 10:40:00.000 | 2017-07-14 10:40:01.000 |     244.003 |
 d1     | 2017-07-14 10:40:01.000 | 2017-07-14 10:40:02.000 |     243.872 |
 ...
```

### 滑动窗口

如果希望窗口按更短步长滑动，可以加上 `SLIDING`。下面的 SQL 使用 1 秒窗口，并每 500 毫秒滑动一次。

```sql
SELECT tbname, _wstart, AVG(voltage)
FROM meters
WHERE ts >= "2017-07-14 10:40:00" AND ts < "2017-07-14 10:40:10"
PARTITION BY tbname
INTERVAL(1s)
SLIDING(500a)
SLIMIT 1;
```

返回结果中，`_wstart` 每次前进 500 毫秒，说明 1 秒窗口正在按 500 毫秒步长滑动。

```text
 tbname |        _wstart         | avg(voltage) |
==================================================
 d0     | 2017-07-14 10:39:59.500 |     243.808 |
 d0     | 2017-07-14 10:40:00.000 |     244.003 |
 d0     | 2017-07-14 10:40:00.500 |     244.089 |
 d0     | 2017-07-14 10:40:01.000 |     243.872 |
 d0     | 2017-07-14 10:40:01.500 |     244.019 |
 ...
```

### 填充缺失窗口

窗口中没有数据时，可以使用 `FILL` 指定填充方式。下面的 SQL 使用前一个非空值填充缺失窗口。

```sql
SELECT _wstart, _wend, AVG(voltage)
FROM d0
WHERE ts >= "2017-07-14 10:40:00" AND ts < "2017-07-14 10:40:10"
INTERVAL(1s)
FILL(prev);
```

本章示例数据较连续，下面结果主要展示 `FILL` 查询的输出结构。若某个窗口没有数据，`FILL(prev)` 会使用前一个非空窗口的结果填充。

```text
        _wstart         |         _wend          | avg(voltage) |
================================================================
 2017-07-14 10:40:00.000 | 2017-07-14 10:40:01.000 |     244.003 |
 2017-07-14 10:40:01.000 | 2017-07-14 10:40:02.000 |     243.872 |
 2017-07-14 10:40:02.000 | 2017-07-14 10:40:03.000 |     244.261 |
 2017-07-14 10:40:03.000 | 2017-07-14 10:40:04.000 |     243.479 |
 2017-07-14 10:40:04.000 | 2017-07-14 10:40:05.000 |     243.972 |
 ...
```

### 状态窗口

状态窗口适合按状态变化切分数据。下面的 SQL 根据电压是否处于 240V 到 250V 的范围划分窗口。

```sql
SELECT _wstart, _wend, COUNT(*),
    CASE WHEN voltage >= 240 AND voltage <= 250 THEN 1 ELSE 0 END AS status
FROM d0
WHERE ts >= "2017-07-14 10:40:00" AND ts < "2017-07-14 10:40:03"
STATE_WINDOW(
    CASE WHEN voltage >= 240 AND voltage <= 250 THEN 1 ELSE 0 END
)
LIMIT 4;
```

返回结果中，相邻窗口的 `status` 不同，表示状态发生了变化。

```text
        _wstart         |         _wend          | count(*) | status |
=====================================================================
 2017-07-14 10:40:00.000 | 2017-07-14 10:40:00.001 |        2 |      0 |
 2017-07-14 10:40:00.002 | 2017-07-14 10:40:00.002 |        1 |      1 |
 2017-07-14 10:40:00.003 | 2017-07-14 10:40:00.006 |        4 |      0 |
 2017-07-14 10:40:00.007 | 2017-07-14 10:40:00.014 |        8 |      1 |

Query OK, 4 row(s) in set
```

### 会话窗口

会话窗口适合按相邻记录的时间间隔切分数据。下面的 SQL 将间隔不超过 30 秒的数据归为同一个会话。由于 `d0` 中相邻点间隔为 1 ms，整段数据会落在同一个会话窗口中。

```sql
SELECT _wstart, _wend, COUNT(*)
FROM d0
WHERE ts >= "2017-07-14 10:40:00" AND ts < "2017-07-14 10:40:10"
SESSION(ts, 30s);
```

返回结果如下。

```text
        _wstart         |         _wend          | count(*) |
=============================================================
 2017-07-14 10:40:00.000 | 2017-07-14 10:40:09.999 |    10000 |

Query OK, 1 row(s) in set
```

### 事件窗口

事件窗口适合“满足开始条件后开窗，满足结束条件后关窗”的场景。例如电压升高到某个阈值后开始观察，降回另一个阈值后结束观察。

```sql
SELECT _wstart, _wend, COUNT(*)
FROM d0
WHERE ts >= "2017-07-14 10:40:00" AND ts < "2017-07-14 10:40:10"
EVENT_WINDOW START WITH voltage >= 250 END WITH voltage < 245
LIMIT 4;
```

返回结果中，每一行表示一次从开窗条件到关窗条件之间的事件区间。

```text
        _wstart         |         _wend          | count(*) |
=============================================================
 2017-07-14 10:40:00.000 | 2017-07-14 10:40:00.001 |        2 |
 2017-07-14 10:40:00.004 | 2017-07-14 10:40:00.005 |        2 |
 2017-07-14 10:40:00.006 | 2017-07-14 10:40:00.011 |        6 |
 2017-07-14 10:40:00.016 | 2017-07-14 10:40:00.017 |        2 |

Query OK, 4 row(s) in set
```

### 计数窗口

计数窗口适合按固定行数分组。下面的 SQL 每 100 行切分一个窗口。

```sql
SELECT _wstart, _wend, COUNT(*)
FROM d0
WHERE ts >= "2017-07-14 10:40:00" AND ts < "2017-07-14 10:40:10"
COUNT_WINDOW(100)
LIMIT 5;
```

返回结果中，每个窗口最多包含 100 行数据。

```text
        _wstart         |         _wend          | count(*) |
=============================================================
 2017-07-14 10:40:00.000 | 2017-07-14 10:40:00.099 |      100 |
 2017-07-14 10:40:00.100 | 2017-07-14 10:40:00.199 |      100 |
 2017-07-14 10:40:00.200 | 2017-07-14 10:40:00.299 |      100 |
 2017-07-14 10:40:00.300 | 2017-07-14 10:40:00.399 |      100 |
 2017-07-14 10:40:00.400 | 2017-07-14 10:40:00.499 |      100 |

Query OK, 5 row(s) in set
```

### 外部窗口

外部窗口更适合用已有事件表、排班表或维护计划定义窗口范围。下面的 SQL 先用子查询显式给出两个窗口边界，再在 `d0` 表中分别计算每个窗口内的平均电压。

```sql
SELECT _wstart, _wend, AVG(voltage)
FROM d0
EXTERNAL_WINDOW (
    (SELECT CAST("2017-07-14 10:40:00" AS TIMESTAMP) AS ws,
            CAST("2017-07-14 10:40:01" AS TIMESTAMP) AS we
     UNION ALL
     SELECT CAST("2017-07-14 10:40:01" AS TIMESTAMP),
            CAST("2017-07-14 10:40:02" AS TIMESTAMP)
     ORDER BY ws) w
);
```

返回结果中，窗口边界来自子查询，而不是由 `INTERVAL` 自动切分。

```text
        _wstart         |         _wend          | avg(voltage) |
=================================================================
 2017-07-14 10:40:00.000 | 2017-07-14 10:40:01.000 |  244.206793 |
 2017-07-14 10:40:01.000 | 2017-07-14 10:40:02.000 |  244.367632 |

Query OK, 2 row(s) in set
```

也可以用 `INTERVAL` 子查询生成有序窗口，效果类似：

```sql
SELECT _wstart, _wend, AVG(voltage)
FROM d0
EXTERNAL_WINDOW (
    (SELECT _wstart, _wend
     FROM d0
     WHERE ts >= "2017-07-14 10:40:00" AND ts < "2017-07-14 10:40:02"
     INTERVAL(1s)) w
);
```

## 常用查询模式

下面是几个快速上手阶段常用的查询模式。

查看某张子表的最新数据：

```sql
SELECT * FROM d0 ORDER BY ts DESC LIMIT 1;
```

查看某个位置的电表记录数：

```sql
SELECT location, COUNT(*)
FROM meters
GROUP BY location
ORDER BY location;
```

查看每个电表的最大电压（用 `SLIMIT` 限制返回分片数）：

```sql
SELECT tbname, MAX(voltage)
FROM meters
PARTITION BY tbname
SLIMIT 3;
```

查看某段时间内的电流平均值：

```sql
SELECT AVG(current)
FROM meters
WHERE ts >= "2017-07-14 10:40:00" AND ts < "2017-07-14 10:40:10";
```

## 继续阅读

本章只覆盖快速上手阶段最常用的查询方式。更多高级查询能力，请继续阅读以下文档：

- [基础查询](../05-tdengine-sql/04-data-query/01-query.md)：`SELECT` 语句语法、常用子句与查询示例
- [运算符](../05-tdengine-sql/04-data-query/02-operators.md)：算术、位运算、比较、逻辑等运算符
- [内置函数](../05-tdengine-sql/04-data-query/03-function.md)：内置函数分类、语法与使用说明
- [特色查询](../05-tdengine-sql/04-data-query/04-distinguished.md)：时序数据特有的查询功能（多种窗口等）
- [关联查询](../05-tdengine-sql/04-data-query/05-join.md)：关联查询（JOIN）概念、类型、语法与限制
- [窗口函数](../05-tdengine-sql/04-data-query/06-window-function.md)：`OVER` 子句与 SQL 标准窗口函数说明
- [自定义函数](../05-tdengine-sql/04-data-query/07-udf.md)：创建、管理与调用用户自定义函数（UDF）
- [读缓存](../05-tdengine-sql/04-data-query/08-cache-query.md)：通过 `CACHEMODEL` 缓存子表最近数据，加速 `LAST` / `LAST_ROW` 查询
- [执行计划](../05-tdengine-sql/04-data-query/09-explain.md)：使用 `EXPLAIN` / `EXPLAIN ANALYZE` 查看查询执行计划与运行期指标
