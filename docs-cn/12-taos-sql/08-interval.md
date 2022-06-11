---
sidebar_label: 按窗口切分聚合
title: 按窗口切分聚合
---


TDengine 支持按时间段窗口切分方式进行聚合结果查询，比如温度传感器每秒采集一次数据，但需查询每隔 10 分钟的温度平均值。这种场景下可以使用窗口子句来获得需要的查询结果。
窗口子句用于针对查询的数据集合进行按照窗口切分成为查询子集并进行聚合，窗口包含时间窗口（time window）、状态窗口（status window）、会话窗口（session window）三种窗口。其中时间窗口又可划分为滑动时间窗口和翻转时间窗口。

## 时间窗口

INTERVAL 子句用于产生相等时间周期的窗口，SLIDING 用以指定窗口向前滑动的时间。每次执行的查询是一个时间窗口，时间窗口随着时间流动向前滑动。在定义连续查询的时候需要指定时间窗口（time window ）大小和每次前向增量时间（forward sliding times）。如图，[t0s, t0e] ，[t1s , t1e]， [t2s, t2e] 是分别是执行三次连续查询的时间窗口范围，窗口的前向滑动的时间范围 sliding time 标识 。查询过滤、聚合等操作按照每个时间窗口为独立的单位执行。当 SLIDING 与 INTERVAL 相等的时候，滑动窗口即为翻转窗口。

![TDengine Database 时间窗口示意图](./timewindow-1.webp)

INTERVAL 和 SLIDING 子句需要配合聚合和选择函数来使用。以下 SQL 语句非法：

```
SELECT * FROM temp_tb_1 INTERVAL(1m);
```

SLIDING 的向前滑动的时间不能超过一个窗口的时间范围。以下语句非法：

```
SELECT COUNT(*) FROM temp_tb_1 INTERVAL(1m) SLIDING(2m);
```

当 SLIDING 与 INTERVAL 取值相等的时候，滑动窗口即为翻转窗口。
_ 聚合时间段的窗口宽度由关键词 INTERVAL 指定，最短时间间隔 10 毫秒（10a）；并且支持偏移 offset（偏移必须小于间隔），也即时间窗口划分与“UTC 时刻 0”相比的偏移量。SLIDING 语句用于指定聚合时间段的前向增量，也即每次窗口向前滑动的时长。
_ 从 2.1.5.0 版本开始，INTERVAL 语句允许的最短时间间隔调整为 1 微秒（1u），当然如果所查询的 DATABASE 的时间精度设置为毫秒级，那么允许的最短时间间隔为 1 毫秒（1a）。 \* **注意**：用到 INTERVAL 语句时，除非极特殊的情况，都要求把客户端和服务端的 taos.cfg 配置文件中的 timezone 参数配置为相同的取值，以避免时间处理函数频繁进行跨时区转换而导致的严重性能影响。

## 状态窗口

使用整数（布尔值）或字符串来标识产生记录时候设备的状态量。产生的记录如果具有相同的状态量数值则归属于同一个状态窗口，数值改变后该窗口关闭。如下图所示，根据状态量确定的状态窗口分别是[2019-04-28 14:22:07，2019-04-28 14:22:10]和[2019-04-28 14:22:11，2019-04-28 14:22:12]两个。（状态窗口暂不支持对超级表使用）

![TDengine Database 时间窗口示意图](./timewindow-3.webp)

使用 STATE_WINDOW 来确定状态窗口划分的列。例如：

```
SELECT COUNT(*), FIRST(ts), status FROM temp_tb_1 STATE_WINDOW(status);
```

## 会话窗口

会话窗口根据记录的时间戳主键的值来确定是否属于同一个会话。如下图所示，如果设置时间戳的连续的间隔小于等于 12 秒，则以下 6 条记录构成 2 个会话窗口，分别是：[2019-04-28 14:22:10，2019-04-28 14:22:30]和[2019-04-28 14:23:10，2019-04-28 14:23:30]。因为 2019-04-28 14:22:30 与 2019-04-28 14:23:10 之间的时间间隔是 40 秒，超过了连续时间间隔（12 秒）。

![TDengine Database 时间窗口示意图](./timewindow-2.webp)

在 tol_value 时间间隔范围内的结果都认为归属于同一个窗口，如果连续的两条记录的时间超过 tol_val，则自动开启下一个窗口。（会话窗口暂不支持对超级表使用）

```

SELECT COUNT(*), FIRST(ts) FROM temp_tb_1 SESSION(ts, tol_val);
```

这种类型的查询语法如下：

```
SELECT function_list FROM tb_name
  [WHERE where_condition]
  [SESSION(ts_col, tol_val)]
  [STATE_WINDOW(col)]
  [INTERVAL(interval [, offset]) [SLIDING sliding]]
  [FILL({NONE | VALUE | PREV | NULL | LINEAR | NEXT})]

SELECT function_list FROM stb_name
  [WHERE where_condition]
  [INTERVAL(interval [, offset]) [SLIDING sliding]]
  [FILL({NONE | VALUE | PREV | NULL | LINEAR | NEXT})]
  [GROUP BY tags]
```

- 在聚合查询中，function_list 位置允许使用聚合和选择函数，并要求每个函数仅输出单个结果（例如：COUNT、AVG、SUM、STDDEV、LEASTSQUARES、PERCENTILE、MIN、MAX、FIRST、LAST），而不能使用具有多行输出结果的函数（例如：DIFF 以及四则运算）。
- 此外 LAST_ROW 查询也不能与窗口聚合同时出现。
- 标量函数（如：CEIL/FLOOR 等）也不能使用在窗口聚合查询中。
-

- WHERE 语句可以指定查询的起止时间和其他过滤条件。
- FILL 语句指定某一窗口区间数据缺失的情况下的填充模式。填充模式包括以下几种：
  1. 不进行填充：NONE（默认填充模式）。
  2. VALUE 填充：固定值填充，此时需要指定填充的数值。例如：FILL(VALUE, 1.23)。
  3. PREV 填充：使用前一个非 NULL 值填充数据。例如：FILL(PREV)。
  4. NULL 填充：使用 NULL 填充数据。例如：FILL(NULL)。
  5. LINEAR 填充：根据前后距离最近的非 NULL 值做线性插值填充。例如：FILL(LINEAR)。
  6. NEXT 填充：使用下一个非 NULL 值填充数据。例如：FILL(NEXT)。

:::info

1. 使用 FILL 语句的时候可能生成大量的填充输出，务必指定查询的时间区间。针对每次查询，系统可返回不超过 1 千万条具有插值的结果。
2. 在时间维度聚合中，返回的结果中时间序列严格单调递增。
3. 如果查询对象是超级表，则聚合函数会作用于该超级表下满足值过滤条件的所有表的数据。如果查询中没有使用 GROUP BY 语句，则返回的结果按照时间序列严格单调递增；如果查询中使用了 GROUP BY 语句分组，则返回结果中每个 GROUP 内不按照时间序列严格单调递增。

:::

时间聚合也常被用于连续查询场景，可以参考文档 [连续查询(Continuous Query)](/develop/continuous-query)。

## 示例

智能电表的建表语句如下：

```
CREATE TABLE meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (location BINARY(64), groupId INT);
```

针对智能电表采集的数据，以 10 分钟为一个阶段，计算过去 24 小时的电流数据的平均值、最大值、电流的中位数。如果没有计算值，用前一个非 NULL 值填充。使用的查询语句如下：

```
SELECT AVG(current), MAX(current), APERCENTILE(current, 50) FROM meters
  WHERE ts>=NOW-1d and ts<=now
  INTERVAL(10m)
  FILL(PREV);
```
