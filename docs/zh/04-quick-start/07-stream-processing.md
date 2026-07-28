---
sidebar_label: 流式计算
title: 流式计算
description: 使用 SQL 快速体验流式计算
toc_max_heading_level: 4
---

在时序数据处理中，实时聚合、降采样、告警前置等需求通常需要持续处理新写入的数据。传统方案常额外部署 Kafka、Flink 等流处理系统，开发和运维链路较长。TDengine 内置流式计算能力，可以直接用 SQL 定义实时处理逻辑，在数据写入后自动触发计算，并把结果写入目标表或发送通知。典型场景包括：

- 实时聚合与降采样：持续计算分钟级、小时级指标，减少后续查询扫描的数据量。
- 报表和大屏预计算：提前生成常用统计结果，降低大范围查询带来的响应延迟。
- 异常检测和告警前置：在数据写入后尽快计算关键指标，为后续告警或检测流程提供输入。

一个流任务通常由“触发”和“计算”两部分组成：触发决定什么时候计算，计算决定使用哪些数据以及结果写到哪里。两者可以来自同一张表，也可以按业务需要分离。

本章继续使用智能电表示例数据，创建一个“每 1 分钟统计每个电表平均电流”的流任务。你将体验完整流程：准备数据、创建流、查看输出表、写入新数据并观察结果更新。

## 前提条件

请先确认已经完成以下准备：

1. TDengine 服务已经启动，可以通过 shell 连接。
2. 集群中已经部署 snode。流式计算任务在 snode 上运行。
3. 已经安装 `taosBenchmark`。如果你使用安装包或 Docker 快速体验，通常已经包含该工具。

可以在 shell 中查看 snode：

```sql
SHOW SNODES;
```

如果没有 snode，可以先查看 dnode，再选择一个 dnode 创建 snode。下面示例中的 `1` 需要替换为实际的 dnode ID。

```sql
SHOW DNODES;
CREATE SNODE ON DNODE 1;
```

更多 snode 部署建议，请参见 [运维与限制](../06-stream-processing/02-instructions.md#部署-snode)。

## 准备示例数据

如果你已经在前一章生成过 `taosBenchmark` 数据，可以直接进入下一节。否则，在终端中执行下面的命令生成电表示例数据。

```shell
taosBenchmark --start-timestamp=1600000000000 --tables=10 --records=1000 --time-step=10000
```

进入 shell 后，切换到 `taosBenchmark` 默认创建的数据库。

```sql
USE test;
```

确认超级表 `meters` 中已经有数据。

```sql
SELECT tbname, ts, current, voltage
FROM meters
ORDER BY ts DESC
LIMIT 5;
```

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

## 创建流任务

为了便于重复执行示例，先清理同名流和输出表。

```sql
DROP STREAM IF EXISTS avg_current_stream;
DROP STABLE IF EXISTS avg_current_stb;
```

执行下面的 SQL，创建一个流任务：每 1 分钟为一个窗口，按子表分别计算平均电流，并把结果写入输出超级表 `avg_current_stb`。

```sql
CREATE STREAM avg_current_stream
  INTERVAL(1m) SLIDING(1m)
  FROM meters PARTITION BY tbname
  STREAM_OPTIONS(FILL_HISTORY_FIRST)
  INTO avg_current_stb
  AS
    SELECT _twstart AS ts,
           _twend AS window_end,
           AVG(current) AS avg_current
    FROM %%trows;
```

其中：

- `INTERVAL(1m) SLIDING(1m)` 表示按 1 分钟窗口触发计算。
- `FROM meters PARTITION BY tbname` 表示每个子表独立触发和计算。
- `STREAM_OPTIONS(FILL_HISTORY_FIRST)` 表示先计算已经写入的历史数据，再处理实时写入的数据。
- `INTO avg_current_stb` 表示把结果写入输出超级表。
- `%%trows` 表示本次触发窗口内的数据集合。

## 查看流任务

创建完成后，可以查看当前数据库中的流任务。

```sql
SHOW STREAMS;
```

返回结果类似如下：

```text
       stream_name  |      create_time        | status  |
=========================================================
 avg_current_stream | 2026-07-27 13:40:00.000 | running |
```

如果需要更详细的状态，可以查询系统表：

```sql
SELECT *
FROM information_schema.ins_streams
WHERE stream_name = 'avg_current_stream';
```

流任务需要异步调度。创建后可以等待几秒，再查询输出表。

## 查看计算结果

查询输出超级表 `avg_current_stb`。

```sql
SELECT tbname, ts, window_end, avg_current
FROM avg_current_stb
ORDER BY ts
LIMIT 5;
```

返回结果中，每一行表示某个电表在一个 1 分钟窗口内的平均电流。

```text
 tbname |           ts            |       window_end        | avg_current |
===========================================================================
 d0     | 2020-09-13 20:26:00.000 | 2020-09-13 20:27:00.000 | 10.400000   |
 d1     | 2020-09-13 20:26:00.000 | 2020-09-13 20:27:00.000 | 10.400000   |
 d2     | 2020-09-13 20:26:00.000 | 2020-09-13 20:27:00.000 | 10.400000   |
 d3     | 2020-09-13 20:26:00.000 | 2020-09-13 20:27:00.000 | 10.400000   |
 d4     | 2020-09-13 20:26:00.000 | 2020-09-13 20:27:00.000 | 10.400000   |
```

这里的 `tbname` 是输出超级表中的子表名。由于流任务按 `meters` 的子表分组，每个电表都会有独立的输出结果。

## 写入新数据并观察更新

向 `d0` 写入一条新的电表数据。

```sql
INSERT INTO d0 VALUES ("2020-09-14 23:13:20", 12.4, 221, 0.31);
```

等待几秒后，查询 `d0` 最近的流计算结果。

```sql
SELECT tbname, ts, window_end, avg_current
FROM avg_current_stb
WHERE tbname = "d0"
ORDER BY ts DESC
LIMIT 3;
```

返回结果类似如下。可以看到最新窗口已经出现在输出表中。

```text
 tbname |           ts            |       window_end        | avg_current |
===========================================================================
 d0     | 2020-09-14 23:13:00.000 | 2020-09-14 23:14:00.000 | 13.230099   |
 d0     | 2020-09-14 23:12:00.000 | 2020-09-14 23:13:00.000 | 12.980000   |
 d0     | 2020-09-14 23:11:00.000 | 2020-09-14 23:12:00.000 | 12.750000   |
```

流任务会持续运行。后续只要 `meters` 中有新数据写入，符合触发条件的窗口就会继续计算并写入输出表。

## 清理示例

如果不再需要本章创建的流任务和输出表，可以执行下面的 SQL。

```sql
DROP STREAM IF EXISTS avg_current_stream;
DROP STABLE IF EXISTS avg_current_stb;
```

## 常见调整

快速上手阶段可以先记住下面几个常见调整方向：

- 如果只想处理新写入的数据，可以去掉 `STREAM_OPTIONS(FILL_HISTORY_FIRST)`。
- 如果需要更低延迟，可以了解 `LOW_LATENCY_CALC` 和 `MAX_DELAY`。
- 如果存在乱序写入、更新或删除，需要结合 `WATERMARK`、重算和最佳实践设计流任务。
- 如果需要把结果发送给外部应用，可以使用 `NOTIFY` 创建通知型流任务。

## 继续阅读

本章只覆盖一个最小可运行的流式计算示例。更多语法、选项和生产环境建议，请继续阅读以下文档：

- [流式计算](../06-stream-processing/index.md)：流式计算能力概览。
- [建流语法](../06-stream-processing/01-syntax.md)：`CREATE STREAM`、触发方式、控制选项和通知语法。
- [运维与限制](../06-stream-processing/02-instructions.md)：snode、权限、重算、乱序写入和配置参数。
- [部署与设计](../06-stream-processing/03-best-practices.md)：部署、配置、建流前设计和典型示例。
- [数据查询](./05-query-and-aggregate.md)：窗口查询、聚合查询和查询结果解释。
