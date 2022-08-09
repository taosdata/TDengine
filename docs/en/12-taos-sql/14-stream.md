---
sidebar_label: 流式计算
title: 流式计算
---

在时序数据的处理中，经常要对原始数据进行清洗、预处理，再使用时序数据库进行长久的储存。用户通常需要在时序数据库之外再搭建 Kafka、Flink、Spark 等流计算处理引擎，增加了用户的开发成本和维护成本。

使用 TDengine 3.0 的流式计算引擎能够最大限度的减少对这些额外中间件的依赖，真正将数据的写入、预处理、长期存储、复杂分析、实时计算、实时报警触发等功能融为一体，并且，所有这些任务只需要使用 SQL 完成，极大降低了用户的学习成本、使用成本。

## 创建流式计算

```sql
CREATE STREAM [IF NOT EXISTS] stream_name [stream_options] INTO stb_name AS subquery
stream_options: {
 TRIGGER    [AT_ONCE | WINDOW_CLOSE | MAX_DELAY time]
 WATERMARK   time
}

```

其中 subquery 是 select 普通查询语法的子集:

```sql
subquery: SELECT [DISTINCT] select_list
    from_clause
    [WHERE condition]
    [PARTITION BY tag_list]
    [window_clause]
    [group_by_clause]
```

不支持 order_by，limit，slimit，fill 语句

例如，如下语句创建流式计算，同时自动创建名为 avg_vol 的超级表，此流计算以一分钟为时间窗口、30 秒为前向增量统计这些电表的平均电压，并将来自 meters 表的数据的计算结果写入 avg_vol 表，不同 partition 的数据会分别创建子表并写入不同子表。

```sql
CREATE STREAM avg_vol_s INTO avg_vol AS
SELECT _wstartts, count(*), avg(voltage) FROM meters PARTITION BY tbname INTERVAL(1m) SLIDING(30s);
```

## 删除流式计算

```sql
DROP STREAM [IF NOT EXISTS] stream_name
```

仅删除流式计算任务，由流式计算写入的数据不会被删除。

## 展示流式计算

```sql
SHOW STREAMS;
```

## 流式计算的触发模式

在创建流时，可以通过 TRIGGER 指令指定流式计算的触发模式。

对于非窗口计算，流式计算的触发是实时的；对于窗口计算，目前提供 3 种触发模式：

1. AT_ONCE：写入立即触发

2. WINDOW_CLOSE：窗口关闭时触发（窗口关闭由事件时间决定，可配合 watermark 使用，详见《流式计算的乱序数据容忍策略》）

3. MAX_DELAY time：若窗口关闭，则触发计算。若窗口未关闭，且未关闭时长超过 max delay 指定的时间，则触发计算。

由于窗口关闭是由事件时间决定的，如事件流中断、或持续延迟，则事件时间无法更新，可能导致无法得到最新的计算结果。

因此，流式计算提供了以事件时间结合处理时间计算的 MAX_DELAY 触发模式。

MAX_DELAY 模式在窗口关闭时会立即触发计算。此外，当数据写入后，计算触发的时间超过 max delay 指定的时间，则立即触发计算

## 流式计算的乱序数据容忍策略

在创建流时，可以在 stream_option 中指定 watermark。

流式计算通过 watermark 来度量对乱序数据的容忍程度，watermark 默认为 0。

T = 最新事件时间 - watermark

每批到来的数据都会以上述公式更新窗口关闭时间，并将窗口结束时间 < T 的所有打开的窗口关闭，若触发模式为 WINDOW_CLOSE 或 MAX_DELAY，则推送窗口聚合结果。

流式计算的过期数据处理策略
对于已关闭的窗口，再次落入该窗口中的数据被标记为过期数据，对于过期数据，流式计算提供两种处理方式：

1. 直接丢弃：这是常见流式计算引擎提供的默认（甚至是唯一）计算模式

2. 重新计算：从 TSDB 中重新查找对应窗口的所有数据并重新计算得到最新结果

无论在哪种模式下，watermark 都应该被妥善设置，来得到正确结果（直接丢弃模式）或避免频繁触发重算带来的性能开销（重新计算模式）。

## 流式计算的数据填充策略

TODO

## 流式计算与会话窗口（session window）

```sql
window_clause: {
    SESSION(ts_col, tol_val)
  | STATE_WINDOW(col)
  | INTERVAL(interval_val [, interval_offset]) [SLIDING (sliding_val)] [FILL(fill_mod_and_val)]
}
```

其中，SESSION 是会话窗口，tol_val 是时间间隔的最大范围。在 tol_val 时间间隔范围内的数据都属于同一个窗口，如果连续的两条数据的时间超过 tol_val，则自动开启下一个窗口。

## 流式计算的监控与流任务分布查询

TODO

## 流式计算的内存控制与存算分离

TODO

## 流式计算的暂停与恢复

```sql
STOP STREAM stream_name;

RESUME STREAM stream_name;
```
