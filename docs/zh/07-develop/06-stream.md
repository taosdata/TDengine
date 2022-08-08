---
sidebar_label: 流式计算
description: "TDengine 流式计算将数据的写入、预处理、复杂分析、实时计算、报警触发等功能融为一体，是一个能够降低用户部署成本、存储成本和运维成本的计算引擎。"
title: 流式计算
---

在时序数据的处理中，经常要对原始数据进行清洗、预处理，再使用时序数据库进行长久的储存。用户通常需要在时序数据库之外再搭建 Kafka、Flink、Spark 等流计算处理引擎，增加了用户的开发成本和维护成本。
使用 TDengine 3.0 的流式计算引擎能够最大限度的减少对这些额外中间件的依赖，真正将数据的写入、预处理、长期存储、复杂分析、实时计算、实时报警触发等功能融为一体，并且，所有这些任务只需要使用 SQL 完成，极大降低了用户的学习成本、使用成本。

## 创建流

```sql
CREATE STREAM [IF NOT EXISTS] stream_name [stream_options] INTO stb_name AS subquery
stream_options: {
 TRIGGER    [AT_ONCE | WINDOW_CLOSE | MAX_DELAY time]
 WATERMARK   time
 IGNORE EXPIRED
}

其中 subquery 是 select 普通查询语法的子集:

subquery: SELECT [DISTINCT] select_list
    from_clause
    [WHERE condition]
    [PARTITION BY tag_list]
    [window_clause]
    [group_by_clause]

不支持 order_by，limit，slimit，fill 语句
```

### 流式计算与窗口切分查询

窗口子句语法如下：

```sql
window_clause: {
    SESSION(ts_col, tol_val)
  | STATE_WINDOW(col)
  | INTERVAL(interval_val [, interval_offset]) [SLIDING (sliding_val)]
}
```

其中，SESSION 是会话窗口，tol_val 是时间间隔的最大范围。在 tol_val 时间间隔范围内的数据都属于同一个窗口，如果连续的两条数据的时间超过 tol_val，则自动开启下一个窗口；
STATE_WINDOW 是状态窗口，产生的连续记录如果具有相同的状态量数值则归属于同一个状态窗口，数值改变后该窗口关闭；
INTERVAL 是时间窗口，用于产生相等时间周期的窗口，查询过滤、聚合等操作按照每个时间窗口为独立的单位执行；

为了便于理解，下面以 interval(10s) 为例，假设事件时间为 "2022-08-06 00:00:01"，对应的窗口为 "2022-08-06 00:00:00" ~ "2022-08-06 00:00:10"（窗口 A），那么下一个窗口为 "2022-08-06 00:00:10" ~ "2022-08-06 00:00:20"（窗口 B）。

### 流式计算的触发模式

在创建流时，可以通过 TRIGGER 指令指定流式计算的触发模式。

对于非窗口计算，流式计算的触发是实时的；

对于窗口计算，目前提供 3 种触发模式：

1. AT_ONCE：写入立即触发，窗口 A 和窗口 B 的数据写入后均可以立即触发计算；

2. WINDOW_CLOSE：窗口关闭时触发（可配合 WATERMARK 使用，详见[流式计算的乱序数据容忍策略](#流式计算的乱序数据容忍策略)，当窗口 B 的数据到达时，窗口 A 才会关闭并触发 WINDOW_CLOSE；

3. MAX_DELAY time：若窗口 A 关闭，则触发计算。若窗口 B 未关闭，且未关闭时长超过 MAX_DELAY 指定的时间，则触发计算。

由于窗口关闭是由事件时间决定的，如事件流中断、或持续延迟，则事件时间无法更新，可能导致无法得到最新的计算结果；

因此，流式计算提供了以事件时间结合处理时间计算的 MAX_DELAY 触发模式；

MAX_DELAY 模式在窗口关闭时或者数据写入后计算触发的时间超过 MAX_DELAY 指定的时间，会立即触发计算。

### 流式计算的乱序数据容忍策略

在创建流时，可以在 stream_options 中指定 WATERMARK；

流式计算通过 WATERMARK 来度量对乱序数据的容忍程度，WATERMARK 默认为 0。

    T = 最新事件时间 - WATERMARK

每批到来的数据都会以上述公式更新窗口关闭时间，并将窗口结束时间 < T 的所有打开的窗口关闭，若触发模式为 WINDOW_CLOSE 或 MAX_DELAY，则推送窗口聚合结果。

以上面的 WINDOW_CLOSE 为例，如果设置了 WATERMARK 15s，那么窗口 A 和 B 均会延迟推送计算结果，当最新事件时间为 "2022-08-06 00:00:25" 时会推送窗口 A 的结果，当最新事件时间为 "2022-08-06 00:00:35" 时会推送窗口 B 的结果。

### 流式计算的过期数据处理策略

对于已关闭的窗口，再次落入该窗口中的数据被标记为过期数据，对于过期数据，流式计算提供两种处理方式：

1. 直接丢弃：这是常见流式计算引擎提供的默认（甚至是唯一）计算模式；

2. 重新计算：从 TSDB 中重新查找对应窗口的所有数据并重新计算得到最新结果；

模式 1 创建流时需要在 stream_options 中配置 IGNORE EXPIRED，对于已经关闭的窗口，再次落入该窗口的乱序数据会被直接丢弃；

无论在哪种模式下，WATERMARK 都应该被妥善设置，来得到正确结果（直接丢弃模式）或避免频繁触发重算带来的性能开销（重新计算模式）。

## 流式计算的展示

```sql
SHOW STREAMS;
```

## 流式计算的删除

```sql
DROP STREAM [IF EXISTS] stream_name;
```

## 使用案例

通过以下案例，进一步了解 TDengine 流计算的使用

### 创建 DB 和原始数据表

首先准备数据，完成建库、建一张超级表和多张子表操作

```sql
drop database if exists stream_db;
create database stream_db;

create table stream_db.stb (ts timestamp, c1 int, c2 float, c3 varchar(16)) tags(t1 int, t3 varchar(16));
create table stream_db.ctb0 using stream_db.stb tags(0, "subtable0");
create table stream_db.ctb1 using stream_db.stb tags(1, "subtable1");
create table stream_db.ctb2 using stream_db.stb tags(2, "subtable2");
create table stream_db.ctb3 using stream_db.stb tags(3, "subtable3");
```

### 创建流

case1. 创建流实现数据过滤

```sql
create stream stream1 into stream_db.stream1_output_stb as select * from stream_db.stb where c1 > 0 and c2 != 10 and c3 is not Null;
```

case2. 创建流实现标量函数的数据转换

```sql
create stream stream2 into stream_db.stream2_output_stb as select ts, abs(c2), char_length(c3), cast(c1 as binary(16)),timezone() from stream_db.stb partition by tbname;
```

case3. 创建流实现数据降采样

```sql
create stream stream3 into stream_db.stream3_output_stb as select _wstart as start, min(c1), max(c2), count(c3) from stream_db.stb interval (10s);
```

case4. 通过 trigger window_close 控制流的触发频率

```sql
create stream stream4 trigger window_close into stream_db.stream4_output_stb as select _wstart as start, min(c1), max(c2), count(c3) from stream_db.stb interval (10s);
```

case4. 通过 trigger max_delay 控制流的触发频率

```sql
create stream stream5 trigger max_delay 3s into stream_db.stream5_output_stb as select _wstart as start, min(c1), max(c2), count(c3) from stream_db.stb interval (10s);
```

case6. 通过 watermark 实现乱序数据容忍

```sql
create stream stream6 trigger window_close watermark 15s into stream_db.stream6_output_stb as select _wstart as start, min(c1), max(c2), count(c3) from stream_db.stb interval (10s);
```

case7. 通过 ignore expired 实现乱序数据丢弃

```sql
create stream stream7 trigger at_once ignore expired into stream_db.stream7_output_stb as select _wstart as start, min(c1), max(c2), count(c3) from stream_db.stb interval (10s);
```

### 写入数据

```sql
insert into stream_db.ctb0 values("2022-08-13 00:00:01", 1, 1, 'a1');
insert into stream_db.ctb0 values("2022-08-13 00:00:07", 7, 7, 'a7');
insert into stream_db.ctb0 values("2022-08-13 00:00:11", 11, 11, 'a11');
insert into stream_db.ctb0 values("2022-08-13 00:00:21", 21, 21, 'a21');
insert into stream_db.ctb0 values("2022-08-13 00:00:24", 24, 24, 'a24');
insert into stream_db.ctb0 values("2022-08-13 00:00:25", 25, 25, 'a25');
insert into stream_db.ctb0 values("2022-08-13 00:00:34", 34, 34, 'a34');
insert into stream_db.ctb0 values("2022-08-13 00:00:35", 35, 35, 'a35');
insert into stream_db.ctb0 values("2022-08-13 00:00:02", 2, 2, 'a2');
```

### 查询以观查结果

```sql
select * from stream_db.stream1_output_stb;
select * from stream_db.stream2_output_stb;
select * from stream_db.stream3_output_stb;
select * from stream_db.stream4_output_stb;
select * from stream_db.stream5_output_stb;
select * from stream_db.stream6_output_stb;
select * from stream_db.stream7_output_stb;
```
