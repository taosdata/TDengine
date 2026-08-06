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

本章继续使用快速体验中 `taosBenchmark -y` 写入的 `test` 库电表数据，创建一个“每 1 分钟统计每个电表平均电流”的流任务。你将体验完整流程：确认数据、创建流、查看输出表、写入新数据并观察结果更新。下面先给出流式计算相关能力全景；完整语法与进阶说明请按下列链接深入阅读，或参见文末“继续阅读”。

## 流式计算能力一览

与传统流计算相比，TDengine 采用**触发与计算分离**的策略：触发决定何时计算，计算决定使用哪些数据以及结果写到哪里；两者可以来自同一张表，也可以按业务需要分离。新的流式计算功能从 `v3.3.7.0` 开始支持。

- **触发方式**  
  支持定时（`PERIOD`）、滑动（`SLIDING`）、时间窗口（`INTERVAL`）、会话 / 状态 / 事件 / 计数窗口等多种触发；可分组触发，并可对触发数据做预过滤。详见 [建流语法](../07-stream-processing/01-syntax.md)。

- **计算与结果输出**  
  计算可为任意查询语句；结果可写入输出表（`INTO`）、发送通知（`NOTIFY`），或两者同时使用。详见 [建流语法](../07-stream-processing/01-syntax.md)。

- **控制选项**  
  通过 `STREAM_OPTIONS` 配置历史回放、乱序水位、最大延迟、低延迟计算等，在结果时效性与资源负载之间做平衡。详见 [建流语法](../07-stream-processing/01-syntax.md)。

- **运维与限制**  
  流任务运行在 snode 上；涵盖高可用、权限、手动重算，以及乱序 / 更新 / 删除等非典型写入场景说明。详见 [运维与限制](../07-stream-processing/02-instructions.md)。

- **部署与设计**  
  部署、配置、建流前设计与典型示例。详见 [部署与设计](../07-stream-processing/03-best-practices.md)。

下文从创建一个 `INTERVAL` 窗口流任务开始上手。

## 前提条件

请先确认已经完成以下准备：

1. TDengine 服务已经启动，可以通过 shell 连接。
2. 集群中已经部署 snode。流式计算任务在 snode 上运行。
3. 已经在下载与安装章节的快速体验中执行过 `taosBenchmark -y`，生成了 `test` 数据库和 `meters` 超级表。若尚未生成，可先在终端执行 `taosBenchmark -y`。

可以在 shell 中查看 snode：

```sql
SHOW SNODES;
```

如果没有 snode，可以先查看 dnode，再选择一个 dnode 创建 snode。下面示例中的 `1` 需要替换为实际的 dnode ID。

```sql
SHOW DNODES;
CREATE SNODE ON DNODE 1;
```

更多 snode 部署建议，请参见 [运维与限制](../07-stream-processing/02-instructions.md#部署-snode)。

## 准备示例数据

进入 shell 后，切换到 `test` 数据库。

```sql
USE test;
```

确认超级表 `meters` 中已经有数据。

```sql
SELECT tbname, ts, current, voltage
FROM meters
WHERE voltage > 250 and tbname = 'd1'
ORDER BY ts DESC
LIMIT 5;
```

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

`test.meters` 的时间戳集中在 `2017-07-14 10:40:00.000` 到 `2017-07-14 10:40:09.999`。下文流任务使用 1 分钟窗口时，历史数据会落在同一个窗口中；写入示例会插入下一分钟的数据，便于观察新窗口生成。

## 创建流任务

为了便于重复执行示例，先清理同名流和输出表。

```sql
DROP STREAM IF EXISTS avg_current_stream;
DROP STABLE IF EXISTS avg_current_stb;
```

执行下面的 SQL，创建一个流任务：每 1 分钟为一个窗口，按子表分别计算平均电流，并把结果写入输出超级表 `avg_current_stb`。输出子表名与源子表对应（如 `d0` → `avg_d0`），并保留源表的 `groupId` 标签。

```sql
CREATE STREAM avg_current_stream
  INTERVAL(1m) SLIDING(1m)
  FROM meters PARTITION BY tbname, groupId
  STREAM_OPTIONS(FILL_HISTORY_FIRST | MAX_DELAY(3s))
  INTO avg_current_stb
  OUTPUT_SUBTABLE(CONCAT("avg_", tbname))
  TAGS (
      groupId INT AS groupId
  )
  AS
    SELECT _twstart AS ts,
           _twend AS window_end,
           AVG(current) AS avg_current
    FROM %%trows;
```

其中：

- `INTERVAL(1m) SLIDING(1m)` 表示按 1 分钟窗口触发计算。
- `FROM meters PARTITION BY tbname, groupId` 表示按子表分组触发；同时把 `groupId` 列入分组列，便于写入输出表标签。
- `STREAM_OPTIONS(FILL_HISTORY_FIRST | MAX_DELAY(3s))` 表示先计算已经写入的历史数据；未关闭的窗口在开启后最多等待约 3 秒也会触发一次计算，便于快速体验。
- `INTO avg_current_stb` 表示把结果写入输出超级表。
- `OUTPUT_SUBTABLE(CONCAT("avg_", tbname))` 表示输出子表名由源子表名生成，例如 `d0` 对应 `avg_d0`。
- `TAGS (groupId INT AS groupId)` 把源表分组标签写入输出超级表，便于按分组查询结果。
- `%%trows` 表示本次触发窗口内的数据集合。

## 查看流任务

创建完成后，可以查看当前数据库中的流任务。

```sql
SHOW STREAMS;
```

返回结果类似如下：

```text
      stream_name     | status |         message         | db_name |
====================================================================
 avg_current_stream   | Idle   | Current deploy times: 0 | test    |

Query OK, 1 row(s) in set
```

如果需要更详细的状态，可以查询系统表：

```sql
SELECT *
FROM information_schema.ins_streams
WHERE stream_name = 'avg_current_stream';
```

流任务需要异步调度。创建后可以等待几秒，再查询输出表。由于 `FILL_HISTORY_FIRST` 会对约 10,000 张子表回放历史窗口，首次计算可能需要稍长时间。

## 查看计算结果

查询输出超级表 `avg_current_stb`。

```sql
SELECT tbname, groupId, ts, window_end, avg_current
FROM avg_current_stb
ORDER BY ts, tbname
LIMIT 5;
```

返回结果中，每一行表示某个电表在一个 1 分钟窗口内的平均电流。`tbname` 为输出子表名（如 `avg_d0`）。历史数据落在 `10:40:00` 到 `10:41:00` 窗口内，示意如下。

```text
 tbname | groupId |           ts            |       window_end        | avg_current |
=======================================================================================
 avg_d0 |       1 | 2017-07-14 10:40:00.000 | 2017-07-14 10:41:00.000 |   10.208475 |
 avg_d1 |       7 | 2017-07-14 10:40:00.000 | 2017-07-14 10:41:00.000 |   10.208475 |
 avg_d2 |       2 | 2017-07-14 10:40:00.000 | 2017-07-14 10:41:00.000 |   10.208475 |
 avg_d3 |       4 | 2017-07-14 10:40:00.000 | 2017-07-14 10:41:00.000 |   10.208475 |
 avg_d4 |       3 | 2017-07-14 10:40:00.000 | 2017-07-14 10:41:00.000 |   10.208475 |
```

`groupId` 的具体取值取决于 `taosBenchmark` 为各子表分配的标签，可能与上表略有差异。

## 写入新数据并观察更新

时间窗口默认在关窗时触发。只写入落在 `10:41:00` 到 `10:42:00` 窗口内的数据时，窗口尚未关闭；本示例建流时配置了 `MAX_DELAY(3s)`，因此窗口开启后最多等待约 3 秒也会产出结果。

向 `d0` 写入一条落在该窗口内的电表数据。

```sql
INSERT INTO d0 VALUES ("2017-07-14 10:41:30", 12.4, 221, 147);
```

也可以再写入一条下一窗口起点的数据，用事件时间直接关窗（即使未配置 `MAX_DELAY` 也会触发）：

```sql
INSERT INTO d0 VALUES ("2017-07-14 10:42:00", 12.5, 220, 147);
```

等待几秒后，查询 `d0` 对应输出子表 `avg_d0` 最近的流计算结果。

```sql
SELECT tbname, groupId, ts, window_end, avg_current
FROM avg_current_stb
WHERE tbname = "avg_d0"
ORDER BY ts DESC
LIMIT 3;
```

返回结果类似如下。可以看到 `10:41:00` 窗口已经出现在输出表中。

```text
 tbname | groupId |           ts            |       window_end        | avg_current |
=======================================================================================
 avg_d0 |       1 | 2017-07-14 10:41:00.000 | 2017-07-14 10:42:00.000 |   12.400000 |
 avg_d0 |       1 | 2017-07-14 10:40:00.000 | 2017-07-14 10:41:00.000 |   10.208475 |
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

- 如果只想处理新写入的数据，可以去掉 `STREAM_OPTIONS` 中的 `FILL_HISTORY_FIRST`。
- 如果希望未关窗时也能尽快出结果，可以保留或调整 `MAX_DELAY`；也可以写入下一窗口的数据，用事件时间推动关窗。
- 如果存在乱序写入、更新或删除，需要结合 `WATERMARK`、重算和最佳实践设计流任务。
- 如果需要把结果发送给外部应用，可以使用 `NOTIFY` 创建通知型流任务。

## 继续阅读

本章只覆盖一个最小可运行的流式计算示例。更完整的流式计算能力，请继续阅读以下文档：

- [流式计算](../07-stream-processing/index.md)：流式计算概述、场景、触发与计算分离及能力扩展
- [建流语法](../07-stream-processing/01-syntax.md)：`CREATE STREAM`、触发方式、结果输出、控制选项和通知语法
- [运维与限制](../07-stream-processing/02-instructions.md)：snode、权限、重算、乱序写入和配置参数
- [部署与设计](../07-stream-processing/03-best-practices.md)：部署、配置、建流前设计和典型示例
- [数据查询](./05-query-and-aggregate.md)：窗口查询、聚合查询和查询结果解释
