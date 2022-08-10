---
sidebar_label: 连续查询
description: "连续查询是一个按照预设频率自动执行的查询功能，提供按照时间窗口的聚合查询能力，是一种简化的时间驱动流式计算。"
title: "连续查询（Continuous Query）"
---

连续查询是 TDengine 定期自动执行的查询，采用滑动窗口的方式进行计算，是一种简化的时间驱动的流式计算。针对库中的表或超级表，TDengine 可提供定期自动执行的连续查询，用户可让 TDengine 推送查询的结果，也可以将结果再写回到 TDengine 中。每次执行的查询是一个时间窗口，时间窗口随着时间流动向前滑动。在定义连续查询的时候需要指定时间窗口（time window, 参数 interval）大小和每次前向增量时间（forward sliding times, 参数 sliding）。

TDengine 的连续查询采用时间驱动模式，可以直接使用 TAOS SQL 进行定义，不需要额外的操作。使用连续查询，可以方便快捷地按照时间窗口生成结果，从而对原始采集数据进行降采样（down sampling）。用户通过 TAOS SQL 定义连续查询以后，TDengine 自动在最后的一个完整的时间周期末端拉起查询，并将计算获得的结果推送给用户或者写回 TDengine。

TDengine 提供的连续查询与普通流计算中的时间窗口计算具有以下区别：

- 不同于流计算的实时反馈计算结果，连续查询只在时间窗口关闭以后才开始计算。例如时间周期是 1 天，那么当天的结果只会在 23:59:59 以后才会生成。
- 如果有历史记录写入到已经计算完成的时间区间，连续查询并不会重新进行计算，也不会重新将结果推送给用户。对于写回 TDengine 的模式，也不会更新已经存在的计算结果。
- 使用连续查询推送结果的模式，服务端并不缓存客户端计算状态，也不提供 Exactly-Once 的语义保证。如果用户的应用端崩溃，再次拉起的连续查询将只会从再次拉起的时间开始重新计算最近的一个完整的时间窗口。如果使用写回模式，TDengine 可确保数据写回的有效性和连续性。

## 连续查询语法

```sql
[CREATE TABLE AS] SELECT select_expr [, select_expr ...]
    FROM {tb_name_list}
    [WHERE where_condition]
    [INTERVAL(interval_val [, interval_offset]) [SLIDING sliding_val]]

```

INTERVAL: 连续查询作用的时间窗口

SLIDING: 连续查询的时间窗口向前滑动的时间间隔

## 使用连续查询

下面以智能电表场景为例介绍连续查询的具体使用方法。假设我们通过下列 SQL 语句创建了超级表和子表：

```sql
create table meters (ts timestamp, current float, voltage int, phase float) tags (location binary(64), groupId int);
create table D1001 using meters tags ("California.SanFrancisco", 2);
create table D1002 using meters tags ("California.LosAngeles", 2);
...
```

可以通过下面这条 SQL 语句以一分钟为时间窗口、30 秒为前向增量统计这些电表的平均电压。

```sql
select avg(voltage) from meters interval(1m) sliding(30s);
```

每次执行这条语句，都会重新计算所有数据。 如果需要每隔 30 秒执行一次来增量计算最近一分钟的数据，可以把上面的语句改进成下面的样子，每次使用不同的 `startTime` 并定期执行：

```sql
select avg(voltage) from meters where ts > {startTime} interval(1m) sliding(30s);
```

这样做没有问题，但 TDengine 提供了更简单的方法，只要在最初的查询语句前面加上 `create table {tableName} as` 就可以了，例如：

```sql
create table avg_vol as select avg(voltage) from meters interval(1m) sliding(30s);
```

会自动创建一个名为 `avg_vol` 的新表，然后每隔 30 秒，TDengine 会增量执行 `as` 后面的 SQL 语句，并将查询结果写入这个表中，用户程序后续只要从 `avg_vol` 中查询数据即可。例如：

```sql
taos> select * from avg_vol;
            ts           |        avg_voltage_    |
===================================================
 2020-07-29 13:37:30.000 |            222.0000000 |
 2020-07-29 13:38:00.000 |            221.3500000 |
 2020-07-29 13:38:30.000 |            220.1700000 |
 2020-07-29 13:39:00.000 |            223.0800000 |
```

需要注意，查询时间窗口的最小值是 10 毫秒，没有时间窗口范围的上限。

此外，TDengine 还支持用户指定连续查询的起止时间。如果不输入开始时间，连续查询将从第一条原始数据所在的时间窗口开始；如果没有输入结束时间，连续查询将永久运行；如果用户指定了结束时间，连续查询在系统时间达到指定的时间以后停止运行。比如使用下面的 SQL 创建的连续查询将运行一小时，之后会自动停止。

```sql
create table avg_vol as select avg(voltage) from meters where ts > now and ts <= now + 1h interval(1m) sliding(30s);
```

需要说明的是，上面例子中的 `now` 是指创建连续查询的时间，而不是查询执行的时间，否则，查询就无法自动停止了。另外，为了尽量避免原始数据延迟写入导致的问题，TDengine 中连续查询的计算有一定的延迟。也就是说，一个时间窗口过去后，TDengine 并不会立即计算这个窗口的数据，所以要稍等一会（一般不会超过 1 分钟）才能查到计算结果。

## 管理连续查询

用户可在控制台中通过 `show streams` 命令来查看系统中全部运行的连续查询，并可以通过 `kill stream` 命令杀掉对应的连续查询。后续版本会提供更细粒度和便捷的连续查询管理命令。
