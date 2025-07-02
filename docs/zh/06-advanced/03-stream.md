---
sidebar_label: 流计算
title: 流计算
toc_max_heading_level: 4
---

在时序数据的处理中，经常要对原始数据进行清洗、预处理，再使用时序数据库进行长久的储存，而且经常还需要使用原始的时序数据通过计算生成新的时序数据。在传统的时序数据解决方案中，常常需要部署 Kafka、Flink 等流处理系统，而流处理系统的复杂性，带来了高昂的开发与运维成本。TDengine 的流计算采用的是触发与计算分离的处理策略，处理的依然是持续的无界的数据流，但是进行了以下几个方面的扩展：

- **处理对象的扩展**：传统流计算的事件驱动对象与计算对象往往是统一的，根据同一份数据产生事件和计算。TDengine 的流计算支持触发（事件驱动）与计算的分离，也就意味着触发对象可以与计算对象进行分离。触发表与计算的数据源表可以不相同，甚至可以不需要触发表，处理的数据集合无论是列、时间范围都可以不相同。
- **触发方式的扩展**：除了数据写入触发方式外，TDengine 的流计算支持更多触发方式的扩展，例如 TDengine 支持的各种窗口。通过支持窗口触发，用户可以灵活的定义和使用各种方式的窗口来产生触发事件，可以选择在开窗、关窗以及开关窗同时进行触发。除了与触发表关联的事件时间驱动外，TDengine 还支持与事件时间无关的驱动，即定时触发。在事件触发之前，TDengine 还支持对触发数据进行预先过滤处理，只有符合条件的数据才会进入触发判断。
- **计算的扩展**：既可以对触发表进行计算，也可以对其他库、表进行计算。计算类型不受限制，支持任何查询语句。计算结果的应用可根据需要进行选择，支持发送通知、写入输出表，也可以两者同时使用。

TDengine 的流计算还提供了其他使用上的便利。针对结果延迟的不同需求，支持用户在结果时效性与资源负载之间进行平衡。针对非正常顺序写入场景的不同需求，支持用户灵活选择适合的处理方式与策略。它提供了替代复杂流处理系统的轻量级解决方案，并能够在高吞吐的数据写入的情况下，提供毫秒级的计算结果延迟。

流计算使用的具体方法如下，详细内容参见 [SQL 手册](../14-reference/03-taos-sql/14-stream.md#流式计算的通知机制)。

## 创建流式计算
```sql
CREATE STREAM [IF NOT EXISTS] [db_name.]stream_name stream_options [INTO [db_name.]table_name] [OUTPUT_SUBTABLE(tbname_expr)] [(column_name1, column_name2 [COMPOSITE KEY][, ...])] [TAGS (tag_definition [, ...])] [AS subquery]

stream_options: {
    trigger_type [FROM [db_name.]table_name] [PARTITION BY col1 [, ...]] [OPTIONS(stream_option [|...])] [notification_definition]
}
    
trigger_type: {
    PERIOD(period_time[, offset_time])
  | [INTERVAL(interval_val[, interval_offset])] SLIDING(sliding_val[, offset_time]) 
  | SESSION(ts_col, session_val)
  | STATE_WINDOW(col) [TRUE_FOR(duration_time)] 
  | EVENT_WINDOW(START WITH start_condition END WITH end_condition) [TRUE_FOR(duration_time)]
  | COUNT_WINDOW(count_val[, sliding_val][, col1[, ...]]) 
}

stream_option: {WATERMARK(duration_time) | EXPIRED_TIME(exp_time) | IGNORE_DISORDER | DELETE_RECALC | DELETE_OUTPUT_TABLE | FILL_HISTORY[(start_time)] | FILL_HISTORY_FIRST[(start_time)] | CALC_NOTIFY_ONLY | LOW_LATENCY_CALC | PRE_FILTER(expr) | FORCE_OUTPUT | MAX_DELAY(delay_time) | EVENT_TYPE(event_types)}

notification_definition:
    NOTIFY(url [, ...]) [ON (event_types)] [WHERE condition] [NOTIFY_OPTIONS(notify_option[|notify_option])]

notify_option: [NOTIFY_HISTORY | ON_FAILURE_PAUSE]
    
event_types:
    event_type [|event_type]    
    
event_type: {WINDOW_OPEN | WINDOW_CLOSE}    

tag_definition:
    tag_name type_name [COMMENT 'string_value'] AS expr
```

### 触发方式

- 定时触发：通过系统时间的固定间隔来驱动，以建流当天系统时间的零点作为基准时间点，然后根据间隔来确定下次触发的时间点，可以通过指定时间偏移来改变基准时间点。
- 滑动触发：对触发表的写入数据按照事件时间的固定间隔来驱动的触发。可以有 INTERVAL 窗口，也可以没有。
- 会话窗口触发：对触发表的写入数据按照会话窗口的方式进行窗口划分，当窗口启动和（或）关闭时进行触发。
- 状态窗口触发：对触发表的写入数据按照状态窗口的方式进行窗口划分，当窗口启动和（或）关闭时进行触发。
- 事件窗口触发：对触发表的写入数据按照事件窗口的方式进行窗口划分，当窗口启动和（或）关闭时进行的触发。
- 计数窗口触发：对触发表的写入数据按照计数窗口的方式进行窗口划分，当窗口启动和（或）关闭时进行的触发。支持列的触发，当指定的列有数据写入时才触发。

### 触发动作

触发后可以根据需要执行不同的动作，比如发送[事件通知](#流式计算的通知机制)、[执行计算](#流式计算的计算任务)或者两者同时进行。

- 只通知不计算：可以通过 `WebSocket` 方式向外部应用发送事件通知。
- 只计算不通知：可以执行任意一个查询并保存结果到流计算的输出表中。
- 既通知又计算：可以执行任意一个查询，同时发送计算结果或事件通知给外部应用。

### 触发表与分组
通常意义来说，一个流计算只对应一个计算，比如根据一个子表触发和产生一个计算，结果保存到一张表中。根据 TDengine **一个设备一张表**的设计理念，如果需要对所有设备分别计算，那就需要为每个子表创建一个流计算，这会造成使用的不便和处理效率的降低。为了解决这个问题，TDengine 的流计算支持触发分组，分组是流计算的最小执行单元，从逻辑上可以认为每个分组对应一个单独的流计算，每个分组对应一个输出表和单独的事件通知。

**总结来说，一个流计算输出表（子表或普通表）的个数与触发表的分组个数相同，未指定分组时只产生一个输出表（普通表）。**

### 计算任务

计算任务是流在事件触发后执行的计算动作，可以是**任意类型的查询语句**，既可以对触发表进行计算，也可以对其他库表进行计算。计算时可能需要使用触发时的关联信息，这些信息在 SQL 语句中以占位符的形式出现，在每次计算时会被作为常量替换到 SQL 语句中。包括：
- _tprev_ts：上一次触发的事件时间
- _tcurrent_ts：本次触发的事件时间
- _tnext_ts：下一次触发的事件时间
- _twstart：本次触发窗口的起始时间戳
- _twend：本次触发窗口的结束时间戳
- _twduration：本次触发窗口的持续时间
- _twrownum：本次触发窗口的记录条数
- _tprev_localtime：上一次触发时刻的系统时间
- _tnext_localtime：下一次触发时刻的系统时间
- _tgrpid：触发分组的 ID 值
- _tlocaltim：本次触发时刻的系统时间|
- %%n：触发分组列的引用，n 为分组列的下标
- %%tbname：触发表每个分组表名的引用，可作为查询表名使用（FROM %%tbname）
- %%trows：触发表每个分组的触发数据集（满足本次触发的数据集）的引用

### 通知机制
事件通知是流在事件触发后可选的执行动作，支持通过 `WebSocket` 协议发送事件通知到应用。用户可以指定需要通知的事件，以及用于接收通知消息的目标地址。通知内容可以包含计算结果，也可以在没有计算结果时只通知事件相关信息。

## 流式计算的示例
#### 计数窗口触发
- 表 tb1 每写入 1 行数据时，计算表 tb2 在同一时刻前 5 分钟内 col1 的平均值，计算结果写入表 tb3。
```SQL
CREATE stream sm1 count_window(1) FROM tb1 
  INTO tb3 AS
    SELECT _twstart, avg(col1) FROM tb2 
    WHERE _c0 >= _twend - 5m AND _c0 <= _twend;
```

- 表 tb1 每写入 10 行大于 0 的 col1 列数据时，计算这 10 条数据 col1 列的平均值，计算结果不需要保存，需要通知到 `ws://localhost:8080/notify`。
```SQL
CREATE stream sm2 count_window(10, 1, col1) FROM tb1 
  OPTIONS(CALC_ONTIFY_ONLY | PRE_FILTER(col1 > 0)) 
  NOTIFY("ws://localhost:8080/notify") ON (WINDOW_CLOSE) 
  AS 
    SELECT avg(col1) FROM %%trows;
```

#### 滑动触发
- 超级表 stb1 的每个子表在每 5 分钟的时间窗口结束后，计算这 5 分钟的 col1 的平均值，每个子表的计算结果分别写入超级表 stb2 的不同子表中。
```SQL
CREATE stream sm1 INTERVAL(5m) SLIDING(5m) FROM stb1 PARTITION BY tbname 
  INTO stb2 AS 
    SELECT _twstart, avg(col1) FROM %%tbname 
    WHERE _c0 >= _twstart AND _c0 <= _twend;
```

> 上面 SQL 中的 `from %%tbname where _c0 >= _twstart and _c0 <= _twend` 与 `from %%trows` 的含义是不完全相同的。前者表示计算使用触发分组对应的表中在触发窗口时间段内的数据，窗口内的数据在计算时与 `%%trows` 相比较是有可能有变化的，后者则表示只使用触发时读取到的窗口数据。

- 超级表 stb1 的每个子表从最早的数据开始，在每 5 分钟的时间窗口结束后或从窗口启动 1 分钟后窗口仍然未关闭时，计算窗口内的 col1 的平均值，每个子表的计算结果分别写入超级表 stb2 的不同子表中。
```SQL
CREATE stream sm2 INTERVAL(5m) SLIDING(5m) FROM stb1 PARTITION BY tbname 
  OPTIONS(MAX_DELAY(1m) | FILL_HISTORY_FIRST) 
  INTO stb2 AS 
    SELECT _twstart, avg(col1) FROM %%tbname WHERE _c0 >= _twstart AND _c0 <= _twend;
```

- 计算电表电流的每分钟平均值，并在窗口打开、关闭时向两个通知地址发送通知，计算历史数据时不发送通知，不允许在通知发送失败时丢弃通知。

```sql
CREATE STREAM avg_stream INTERVAL(1m) SLIDING(1m) FROM meters 
  NOTIFY ('ws://localhost:8080/notify', 'wss://192.168.1.1:8080/notify?key=foo') ON ('WINDOW_OPEN', 'WINDOW_CLOSE') NOTIFY_OPTIONS(NOTIFY_HISTORY | ON_FAILURE_PAUSE)
  INTO avg_stb
    AS SELECT _twstart, _twend, AVG(current) FROM %%trows;
```

#### 定时触发
- 每过 1 小时计算表 tb1 中总的数据量，计算结果写入表 tb2 (毫秒库)。
```SQL
CREATE stream sm1 PERIOD(1h) 
  INTO tb2 AS
    SELECT cast(_tlocaltime/1000000 AS TIMESTAMP), count(*) FROM tb1;
```

- 每过 1 小时通知 `ws://localhost:8080/notify` 当前系统时间。
```SQL
CREATE stream sm1 PERIOD(1h) 
  NOTIFY("ws://localhost:8080/notify");
```

## 流式计算的其他特性
### 高可用
流式计算从架构上支持流的存算分离，在部署时要求系统中必须部署 snode，除数据读取外所有流计算功能都只在 snode 上运行。
- snode 是负责运行流计算计算任务的节点，在一个集群中可以部署 1 或多个（至少 1 个），每个 snode 都有多个执行线程。
- snode 部署在单独的 dnode 上时，可以保证资源隔离，不会对写入、查询等业务造成太大干扰。
- 为了保证流计算的高可用，可在一个集群的多个物理节点中部署多个 snode：
  - 流计算在多个 snode 间进行负载均衡。
  - 每两个 snode 间互为存储副本，负责存储流的状态和进度等信息。

### 重新计算
TDengine 支持使用 `WATERMARK` 来解决一定程度的乱序、更新、删除场景带来的问题。`WATERMARK` 是用户可以指定的基于事件时间的时长，它代表的是事件时间在流计算中的进展，体现了用户对于乱序数据的容忍程度。`当前处理的最新事件时间 - WATERMARK 指定的固定间隔` 即为当前水位线，只有写入数据的事件时间早于当前水位线才会进入触发判断，只有窗口或其他触发的时间条件早于当前水位线才会启动触发。

对于超出 `WATERMARK` 的乱序、更新、删除场景，TDengine 流计算使用重新计算的方式来保证最终结果的正确性，重新计算意味着对于乱序、更新和删除的数据覆盖区间重新进行触发和运算。为了保证这种方式的有效性，用户需要确保其计算语句和数据源表是与处理时间无关的，也就是说同一个触发即使执行多次其结果依然是有效的。

重新计算可以分为自动重新计算与手动重新计算，如果用户不需要自动重新计算，可以通过选项关闭。
