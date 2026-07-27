---
sidebar_label: 数据查询
title: 数据查询
description: 使用 SQL 快速体验时序数据查询、聚合和时间窗口分析
toc_max_heading_level: 4
---

import win from '../05-tdengine-sql/04-data-query/assets/window.png';

相较于许多时序数据库和实时数据库，TDengine TSDB 自首个版本起就支持标准 SQL 查询。这一能力降低了时序数据查询和分析的学习成本。

本章以智能电表数据模型为例，通过 `taosBenchmark` 生成一批示例数据，然后在 shell 中快速体验常用查询：按条件过滤、排序、限制返回行数，按标签或子表聚合，以及按时间窗口统计数据。每类查询都会给出 SQL 和代表性返回结果，帮助你理解查询输出的形态。完整语法和高级查询能力请参见文末“继续阅读”。

## 前提条件

请先确认已经完成以下准备：

1. TDengine 服务已经启动，可以通过 shell 连接。
2. 已经安装 `taosBenchmark`。如果你使用安装包或 Docker 快速体验，通常已经包含该工具。

## 准备查询数据

在终端中执行下面的命令，生成本章示例需要的电表数据。

```shell
taosBenchmark --start-timestamp=1600000000000 --tables=10 --records=1000 --time-step=10000
```

这条命令会生成 10 张子表，每张子表写入 1000 条数据，采集间隔为 10 秒。时间戳从 `1600000000000` 开始，也就是 `2020-09-13 20:26:40+08:00`。

进入 shell 后，切换到 `taosBenchmark` 默认创建的数据库。

```sql
USE test;
```

## 基本查询

执行下面的 SQL，从超级表 `meters` 中查询电压大于 230V 的数据，并按时间倒序返回前 5 行。

```sql
SELECT * FROM meters
WHERE voltage > 230
ORDER BY ts DESC
LIMIT 5;
```

其中：

- `WHERE voltage > 230` 用于过滤电压大于 230V 的记录。
- `ORDER BY ts DESC` 按时间戳倒序返回结果。
- `LIMIT 5` 只返回前 5 行。

如果只关心部分列，可以在 `SELECT` 后列出需要的列。

```sql
SELECT tbname, ts, current, voltage
FROM meters
WHERE voltage > 230
ORDER BY ts DESC
LIMIT 5;
```

`tbname` 是伪列，表示数据来自哪张子表。
返回结果类似如下，具体数值可能因 `taosBenchmark` 版本或随机数据不同而略有差异。

```text
 tbname |           ts            |  current   | voltage |
==========================================================
 d9     | 2020-09-14 23:13:10.000 | 14.060198  |     232 |
 d8     | 2020-09-14 23:13:10.000 | 14.060198  |     232 |
 d7     | 2020-09-14 23:13:10.000 | 14.060198  |     232 |
 d6     | 2020-09-14 23:13:10.000 | 14.060198  |     232 |
 d5     | 2020-09-14 23:13:10.000 | 14.060198  |     232 |
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
WHERE location = "California.SanFrancisco" AND voltage > 230
ORDER BY ts DESC
LIMIT 5;
```

## 聚合查询

聚合函数可以帮助你快速计算统计值。下面的 SQL 统计所有电表的平均电压、最大电压和总记录数。

```sql
SELECT AVG(voltage), MAX(voltage), COUNT(*)
FROM meters;
```

返回结果中只有一行，表示全量数据的汇总结果。

```text
 avg(voltage) | max(voltage) | count(*) |
=========================================
 222.000000   |          235 |    10000 |
Query OK, 1 row(s) in set
```

如果需要按分组统计，可以使用 `GROUP BY`。

```sql
SELECT groupid, AVG(voltage), COUNT(*)
FROM meters
GROUP BY groupid;
```

返回结果会按 `groupid` 分组，每个分组一行。

```text
 groupid | avg(voltage) | count(*) |
====================================
       1 | 222.000000   |     1000 |
       2 | 222.000000   |     1000 |
       3 | 222.000000   |     1000 |
 ...
Query OK, 10 row(s) in set
```

`GROUP BY` 的结果不保证固定顺序。如需排序，可以继续使用 `ORDER BY`。

```sql
SELECT groupid, AVG(voltage) AS avg_voltage
FROM meters
GROUP BY groupid
ORDER BY avg_voltage DESC;
```

## 按子表聚合

如果需要分别统计每个电表，可以使用 `PARTITION BY tbname`。下面的 SQL 按子表统计平均电压。

```sql
SELECT tbname, AVG(voltage), COUNT(*)
FROM meters
PARTITION BY tbname;
```

返回结果会按子表切分，每个电表一行。

```text
 tbname | avg(voltage) | count(*) |
===================================
 d0     | 222.000000   |     1000 |
 d1     | 222.000000   |     1000 |
 d2     | 222.000000   |     1000 |
 ...
Query OK, 10 row(s) in set
```

`PARTITION BY` 会先把超级表数据按指定维度切分，再在每个分片中执行计算。它常用于“每台设备分别统计”的场景。

## 窗口查询

窗口查询用于把时序数据按时间、状态、事件或行数切分，再在每个窗口内计算。快速上手阶段可以先理解下面几类窗口：

<img src={win} width="500" alt="常用窗口划分逻辑" />

- 时间窗口：按固定时间间隔切分，使用 `INTERVAL`。
- 滑动窗口：在时间窗口基础上设置滑动步长，使用 `SLIDING`。
- 状态窗口：按状态值变化切分，使用 `STATE_WINDOW`。
- 会话窗口：按相邻记录时间间隔切分，使用 `SESSION`。
- 事件窗口：按开始条件和结束条件切分，使用 `EVENT_WINDOW`。
- 计数窗口：按固定行数切分，使用 `COUNT_WINDOW`。
- 外部窗口：由子查询显式给出窗口范围，使用 `EXTERNAL_WINDOW`。

下面先体验最常用的几种窗口。

### 时间窗口

下面的 SQL 按 1 分钟窗口计算每个电表的平均电压。

```sql
SELECT tbname, _wstart, _wend, AVG(voltage)
FROM meters
WHERE ts >= "2020-09-13 20:26:40" AND ts < "2020-09-13 20:36:40"
PARTITION BY tbname
INTERVAL(1m)
SLIMIT 2;
```

其中：

- `INTERVAL(1m)` 表示按 1 分钟切分时间窗口。
- `_wstart` 和 `_wend` 是窗口开始时间和结束时间。
- `PARTITION BY tbname` 表示每个子表独立做窗口聚合。
- `SLIMIT 2` 只返回前 2 个分片，避免结果过长。

返回结果中每一行对应一个时间窗口，节选如下。

```text
 tbname |        _wstart          |         _wend          | avg(voltage) |
==========================================================================
 d0     | 2020-09-13 20:26:00.000 | 2020-09-13 20:27:00.000 | 222.000000   |
 d0     | 2020-09-13 20:27:00.000 | 2020-09-13 20:28:00.000 | 222.500000   |
 d1     | 2020-09-13 20:26:00.000 | 2020-09-13 20:27:00.000 | 222.000000   |
 d1     | 2020-09-13 20:27:00.000 | 2020-09-13 20:28:00.000 | 222.500000   |
 ...
```

### 滑动窗口

如果希望窗口按更短步长滑动，可以加上 `SLIDING`。下面的 SQL 使用 1 分钟窗口，并每 30 秒滑动一次。

```sql
SELECT tbname, _wstart, AVG(voltage)
FROM meters
WHERE ts >= "2020-09-13 20:26:40" AND ts < "2020-09-13 20:36:40"
PARTITION BY tbname
INTERVAL(1m)
SLIDING(30s)
SLIMIT 1;
```

返回结果中，`_wstart` 每次前进 30 秒，说明 1 分钟窗口正在按 30 秒步长滑动。

```text
 tbname |        _wstart          | avg(voltage) |
==================================================
 d0     | 2020-09-13 20:26:30.000 | 222.000000   |
 d0     | 2020-09-13 20:27:00.000 | 222.500000   |
 d0     | 2020-09-13 20:27:30.000 | 223.000000   |
 d0     | 2020-09-13 20:28:00.000 | 222.500000   |
 d0     | 2020-09-13 20:28:30.000 | 222.000000   |
 ...
```

### 填充缺失窗口

窗口中没有数据时，可以使用 `FILL` 指定填充方式。下面的 SQL 使用前一个非空值填充缺失窗口。

```sql
SELECT tbname, _wstart, _wend, AVG(voltage)
FROM meters
WHERE ts >= "2020-09-13 20:26:40" AND ts < "2020-09-13 20:36:40"
PARTITION BY tbname
INTERVAL(1m)
FILL(prev)
SLIMIT 1;
```

本章示例数据较连续，下面结果主要展示 `FILL` 查询的输出结构。若某个窗口没有数据，`FILL(prev)` 会使用前一个非空窗口的结果填充。

```text
 tbname |        _wstart          |         _wend          | avg(voltage) |
==========================================================================
 d0     | 2020-09-13 20:26:00.000 | 2020-09-13 20:27:00.000 | 222.000000   |
 d0     | 2020-09-13 20:27:00.000 | 2020-09-13 20:28:00.000 | 222.500000   |
 d0     | 2020-09-13 20:28:00.000 | 2020-09-13 20:29:00.000 | 223.000000   |
 d0     | 2020-09-13 20:29:00.000 | 2020-09-13 20:30:00.000 | 222.500000   |
 d0     | 2020-09-13 20:30:00.000 | 2020-09-13 20:31:00.000 | 222.000000   |
 ...
```

### 状态窗口

状态窗口适合按状态变化切分数据。下面的 SQL 根据电压是否处于 225V 到 235V 的正常范围划分窗口。

```sql
SELECT tbname, _wstart, _wend, COUNT(*),
    CASE WHEN voltage >= 225 AND voltage <= 235 THEN 1 ELSE 0 END AS status
FROM meters
WHERE ts >= "2020-09-13 20:26:40" AND ts < "2020-09-13 20:36:40"
PARTITION BY tbname
STATE_WINDOW(CASE WHEN voltage >= 225 AND voltage <= 235 THEN 1 ELSE 0 END)
SLIMIT 2;
```

返回结果中，相邻窗口的 `status` 不同，表示状态发生了变化。

```text
 tbname |        _wstart          |         _wend          | count(*) | status |
===============================================================================
 d0     | 2020-09-13 20:26:40.000 | 2020-09-13 20:28:10.000 |       10 |      0 |
 d0     | 2020-09-13 20:28:20.000 | 2020-09-13 20:30:00.000 |       11 |      1 |
 d1     | 2020-09-13 20:26:40.000 | 2020-09-13 20:28:10.000 |       10 |      0 |
 d1     | 2020-09-13 20:28:20.000 | 2020-09-13 20:30:00.000 |       11 |      1 |
 ...
```

### 会话窗口

会话窗口适合按相邻记录的时间间隔切分数据。下面的 SQL 将间隔不超过 30 秒的数据归为同一个会话。

```sql
SELECT tbname, _wstart, _wend, COUNT(*)
FROM meters
WHERE ts >= "2020-09-13 20:26:40" AND ts < "2020-09-13 20:36:40"
PARTITION BY tbname
SESSION(ts, 30s)
SLIMIT 1;
```

返回结果中，一个会话窗口表示一段连续采集的数据。

```text
 tbname |        _wstart          |         _wend          | count(*) |
=====================================================================
 d0     | 2020-09-13 20:26:40.000 | 2020-09-13 20:36:30.000 |       60 |
 ...
```

### 事件窗口

事件窗口适合“满足开始条件后开窗，满足结束条件后关窗”的场景。例如电压升高到某个阈值后开始观察，降回另一个阈值后结束观察。

```sql
SELECT tbname, _wstart, _wend, COUNT(*)
FROM meters
WHERE ts >= "2020-09-13 20:26:40" AND ts < "2020-09-13 20:36:40"
PARTITION BY tbname
EVENT_WINDOW START WITH voltage >= 235 END WITH voltage < 230
SLIMIT 1;
```

返回结果中，每一行表示一次从开窗条件到关窗条件之间的事件区间。

```text
 tbname |        _wstart          |         _wend          | count(*) |
=====================================================================
 d0     | 2020-09-13 20:29:20.000 | 2020-09-13 20:30:10.000 |        6 |
 d0     | 2020-09-13 20:34:20.000 | 2020-09-13 20:35:10.000 |        6 |
 ...
```

### 计数窗口

计数窗口适合按固定行数分组。下面的 SQL 每 100 行切分一个窗口。

```sql
SELECT _wstart, _wend, COUNT(*)
FROM meters
WHERE ts >= "2020-09-13 20:26:40" AND ts < "2020-09-13 20:36:40"
COUNT_WINDOW(100);
```

返回结果中，每个窗口最多包含 100 行数据。

```text
        _wstart          |         _wend          | count(*) |
==============================================================
 2020-09-13 20:26:40.000 | 2020-09-13 20:28:20.000 |      100 |
 2020-09-13 20:28:20.000 | 2020-09-13 20:30:00.000 |      100 |
 2020-09-13 20:30:00.000 | 2020-09-13 20:31:40.000 |      100 |
 ...
```

### 外部窗口

外部窗口更适合用已有事件表、排班表或维护计划定义窗口范围。下面的 SQL 先在子查询中定义两个窗口，再在 `d0` 表中分别计算每个窗口内的平均电压。

```sql
SELECT _wstart, _wend, AVG(voltage)
FROM d0
EXTERNAL_WINDOW (
    (SELECT "2020-09-13 20:26:40"::TIMESTAMP,
            "2020-09-13 20:27:40"::TIMESTAMP
     UNION ALL
     SELECT "2020-09-13 20:27:40"::TIMESTAMP,
            "2020-09-13 20:28:40"::TIMESTAMP) w
);
```

返回结果中，窗口边界来自子查询，而不是由 `INTERVAL` 自动切分。

```text
        _wstart          |         _wend          | avg(voltage) |
===================================================================
 2020-09-13 20:26:40.000 | 2020-09-13 20:27:40.000 | 222.000000   |
 2020-09-13 20:27:40.000 | 2020-09-13 20:28:40.000 | 222.500000   |
```

## 常用查询模式

下面是几个快速上手阶段常用的查询模式。

查看某张子表的最新数据：

```sql
SELECT * FROM d0 ORDER BY ts DESC LIMIT 1;
```

查看某个位置的电表数量：

```sql
SELECT location, COUNT(*)
FROM meters
GROUP BY location;
```

查看每个电表的最大电压：

```sql
SELECT tbname, MAX(voltage)
FROM meters
PARTITION BY tbname;
```

查看某段时间内的电流平均值：

```sql
SELECT AVG(current)
FROM meters
WHERE ts >= "2020-09-13 20:26:40" AND ts < "2020-09-13 20:36:40";
```

## 继续阅读

本章只覆盖快速上手阶段最常用的查询方式。更多高级查询能力，请继续阅读以下文档：

- [数据查询](../05-tdengine-sql/04-data-query/01-query.md)：完整查询语法、过滤、排序、分组、嵌套查询和 `UNION`。
- [函数](../05-tdengine-sql/04-data-query/03-function.md)：聚合函数、选择函数和时序数据特有函数。
- [特色查询](../05-tdengine-sql/04-data-query/06-distinguished.md)：时间窗口、状态窗口、会话窗口、事件窗口、计数窗口和外部窗口。
- [关联查询](../05-tdengine-sql/04-data-query/07-join.md)：普通 Join、ASOF Join 和 Window Join。
