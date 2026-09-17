---
sidebar_label: 部署与设计
title: 部署与设计
description: 流式计算部署、建流设计与维护建议
---

重构后的流式计算提供了更大的功能灵活性，取消了很多使用限制。在提升可用性的同时，也对使用流式计算提出了更高的要求。部署与高可用细节也可参见 [运维与限制](./02-instructions.md)。

## 部署方面

- 在单独的 dnode 中部署 snode，降低流式计算对数据库读写的影响，该 dnode 不部署 vnode、mnode、qnode。
- 在集群内部署多个 snode 以保证高可用。
- 在建流前先预先完成多个 snode 的创建，以便更好的达到负载均衡。
- 流式计算负载高时，可以通过增加 snode 来实现负载均衡。

## 配置方面

- 根据部署方式和负载情况，合理配置流相关的线程个数，线程数越大可能占用的 CPU 资源越高，反之则越低。
- 根据部署方式合理配置流最大的缓存大小，负载越大，并发流越多，单独部署时都可以调大配置。

## 建流前设计

用户在建流前需要依次明确下列主要功能检查点，明确后可以按照如下应对方式进行使用：

- **根据业务特点选择触发方式**：如需每写入一条或多条数据进行处理则选择计数触发，如需满足窗口条件则选择窗口触发，如需根据事件时间定时运算则选择滑动触发，如需根据处理时间定时运算则选择定时触发。
- **根据业务计算的时效性要求选择窗口触发的可选项**：窗口触发除了要选择窗口类型外，还需要选择是开窗触发、关窗触发还是两者都触发，除此之外还可以选择在窗口未关闭时是否需要及时计算（`MAX_DELAY(delay_time)`）。
- **根据事件来源选择触发表**：事件触发的来源表选做触发表，对于定时触发可以没有触发表，但是如果需要分组输出或使用定时期间触发表数据集（`%%trows`），则必须指定触发表。
- **确保数据来源表与触发表的时序关系**：如果数据来源表与触发表不相同，那么需要确保在触发表事件触发时数据来源表中数据已经可用，否则将可能影响计算结果的正确性。
- **根据业务最终计算的需求决定分组**：以超级表计算为例，如果业务最终的需求是全局聚合则不需要分组，如果需要某些表聚合则选择按标签分组，如果需要单个子表的计算结果则可以选择按子表分组。
- **根据流式计算结果的应用方式选择分组输出子表**：
  - 每个分组都可以有自己独立的输出子表，如果分组过多可能导致结果表过多，因此可以根据后续流式计算结果的应用方式和系统资源限制，选择是否每个分组需要单独的输出子表。
  - 多个分组可合并输出到一个子表时，可以设定这些分组使用相同的输出表名（`[OUTPUT_SUBTABLE(tbname_expr)]`），结合输出表的复合主键设计，即可实现分组结果合并。
- **用最优的数据写入方式**：每个分组的数据都能顺序写入是流式计算最佳的写入方式，如果存在大量的乱序写入、更新写入、数据删除操作，将可能造成大量的重算处理，因此如果有条件能够保证写入顺序将可以有效提升流式计算的计算效能。
- **确认写入数据的乱序情况**:根据每个子表的写入乱序情况确定是否需要指定 `WATERMARK` 以及确定合适的 `WATERMARK` 时长。
- **确认乱序数据写入对流式计算的影响**：
  - 如果存在乱序写入数据，需要确认这些乱序写入的数据的计算结果的影响，对于业务非常注重计算或通知的时效性、触发表乱序数据不影响计算结果等场景，可以指定 `STREAM_OPTIONS(IGNORE_DISORDER)` 忽略这些乱序数据。
  - 如果存在严重的过去时间的乱序写入数据（写入数据的事件时间与当前已经处理的事件时间相差太大），且这些数据不影响计算结果或时效性已经丧失，可以通过 `STREAM_OPTIONS(EXPIRED_TIME(exp_time))` 指定其为过期数据且忽略处理。
- **确认重算对流式计算结果的有效性**：乱序、更新、删除场景主要是通过重算方式来解决，如果重算结果不具有幂等性或有效性，则会影响结果的正确性，需可结合业务特点进行判断。
- **确认删除数据对流式计算的影响**：如果存在数据删除，且需要根据删除的数据重新计算结果时，可以通过 `STREAM_OPTIONS(DELETE_RECALC)` 指定。计数窗口中只有滑动步长为 1 的窗口（例如 `COUNT_WINDOW(1)` 和 `COUNT_WINDOW(n, 1)`）支持该选项。
- **确认历史数据是否需要计算和计算方式**：
  - 在建流前，数据库中已经写入的数据需要进行计算，需要根据业务特点和处理逻辑进一步确认是优先计算历史数据还是实时数据，例如 `COUNT_WINDOW` 触发需优先历史数据计算，否则可能造成窗口无法衔接。
  - 如果优先历史数据计算，可以指定 `STREAM_OPTIONS(FILL_HISTORY_FIRST)`，否则指定 `STREAM_OPTIONS(FILL_HISTORY)`。
- **确认业务对流式计算实时性要求程度**：如果业务对流式计算通知或计算的实时性要求很高，可以指定 `STREAM_OPTIONS(LOW_LATENCY_CALC)`，在这种模式下会对计算资源有更高的要求。
- **确认使用流式计算的用途**：如果只需要一个事件触发通知，而不需要做计算，那可以使用只通知不计算模式（即不指定计算语句）。
- **确认计算后结果的应用方式**：如果只需要结果通知而不需要保存，可以使用只通知不保存选项（`STREAM_OPTIONS(CALC_NOTIFY_ONLY)`）。
- **确认结果写入的可靠性**：运算过程中如果计算结果的主键为 NULL 值，则对应的计算结果会被丢弃。
  - 如果查询语句也包含分组子句，且分组结果写入到同一个子表，则不同分组产生的相同时间戳记录会出现数据覆盖。
  - 如果同一个分组多次触发计算产生的主键时间戳相同，那么它们会互相覆盖。
  - 如果同一个分组多次触发（重算）产生的主键时间戳不相同，那么它们就不会被更新。

## 流的维护

在流的状态展示中（查询表 `information_schema.ins_streams`），会列举流的一些具体的状态信息，例如：流的实时计算是否能保持进度一致，流的重算次数多少（比例），流的错误信息等等，需要用户或运维人员关注展示的信息，判断流式计算业务是否正常并据此进行分析优化处理。

## 建流示例

### 按条件发送通知 {#notify-where}

`NOTIFY ... WHERE` 可以根据计算结果决定哪些结果行进入通知，同时保留完整的输出表结果。以下两个示例分别根据平均电流和原始数据的质量标记筛选通知，语法和过滤范围见 [流式计算的通知机制](./01-syntax.md#流式计算的通知机制)。

执行前需有可用的 snode，并在 `ws://localhost:8080/notify` 启动 WebSocket 接收服务；其中 `localhost` 指运行 taosd 的主机。以下示例假设客户端和服务端时区均为 `Asia/Shanghai`（东八区），数据库精度为毫秒。先创建空表和两个流，等待流就绪后再写入示例数据。

```sql
CREATE DATABASE notify_demo VGROUPS 1 PRECISION 'ms' KEEP 3650;
USE notify_demo;
CREATE TABLE measurements (ts TIMESTAMP, current DOUBLE, quality INT);
```

**示例一：按计算结果过滤通知**

每个 10 秒窗口关闭时计算平均电流，只有平均值大于 20 时才发送结果通知：

```sql
CREATE STREAM avg_alarm
  INTERVAL(10s) SLIDING(10s) FROM measurements
  NOTIFY('ws://localhost:8080/notify')
    ON(WINDOW_CLOSE) WHERE avg_current > 20
  INTO avg_results
  AS SELECT _twstart AS window_start,
            AVG(current) AS avg_current
     FROM %%trows;
```

`avg_current` 是计算查询的输出别名。平均值不超过 20 的结果仍然写入 `avg_results`，只是不会出现在结果通知中。

**示例二：根据原始列值过滤通知**

假设 `quality = 0` 表示异常，要求窗口内只要有一条异常记录就通知，同时仍对窗口内全部记录计算平均电流。可以在计算结果中增加异常行数 `bad_rows`，再根据它筛选通知：

```sql
CREATE STREAM quality_alarm
  INTERVAL(10s) SLIDING(10s) FROM measurements
  NOTIFY('ws://localhost:8080/notify')
    ON(WINDOW_CLOSE) WHERE bad_rows > 0
  INTO quality_results
  AS SELECT _twstart AS window_start,
            AVG(current) AS avg_current,
            SUM(CASE WHEN quality = 0 THEN 1 ELSE 0 END) AS bad_rows
     FROM %%trows;
```

这里判断的是“窗口内至少有一条异常记录”，不等同于“最后一条记录异常”或“全部记录异常”。`bad_rows` 是显式输出的结果列，会同时保存在 `quality_results` 和匹配的通知结果中。如果计算查询未输出 `quality`，直接使用 `NOTIFY ... WHERE quality = 0` 会被拒绝。

**写入数据并核对结果**

查询任务状态，确认上述两个流的 `Trigger` 任务均为 `Running` 后，再写入数据：

```sql
SELECT stream_name, status FROM information_schema.ins_stream_tasks
WHERE type = 'Trigger';

INSERT INTO measurements VALUES
    ('2025-12-09 00:00:00', 10, 0)
    ('2025-12-09 00:00:01', 20, 1)
    ('2025-12-09 00:00:10', 30, 1)
    ('2025-12-09 00:00:11', 50, 1)
    ('2025-12-09 00:00:20', 0, 1);
```

最后一行使事件时间推进到 `00:00:20`，关闭前两个 10 秒窗口；它自身属于第三个尚未关闭的窗口。前两个窗口的计算和通知情况如下：

| window_start | avg_current | bad_rows | avg_alarm 结果通知 | quality_alarm 结果通知 |
| --- | --- | --- | --- | --- |
| 2025-12-09 00:00:00.000 | 15 | 1 | 不发送 | 发送 |
| 2025-12-09 00:00:10.000 | 40 | 0 | 发送 | 不发送 |

两个输出表均保存这两个窗口的计算结果，其中 `bad_rows` 仅由 `quality_alarm` 输出。可以查询输出表，并与 WebSocket 接收到的两个流的通知分别对照：

```sql
SELECT * FROM avg_results ORDER BY window_start;
SELECT * FROM quality_results ORDER BY window_start;
```

第一个窗口的 `avg_current = 15`，说明 `quality = 1` 的记录仍参与了平均值计算。`NOTIFY ... WHERE` 不等同于 `PRE_FILTER` 或计算查询中的 `WHERE`：后两者会分别过滤触发数据或计算输入，而通知条件只筛选结果通知。

### 计数窗口触发

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
  STREAM_OPTIONS(CALC_NOTIFY_ONLY | PRE_FILTER(col1 > 0)) 
  NOTIFY("ws://localhost:8080/notify") ON (WINDOW_CLOSE) 
  AS 
    SELECT avg(col1) FROM %%trows;
```

### 事件窗口触发

- 当环境温度超过 80 度持续超过 10 分钟时，计算环境温度的平均值。

```SQL
CREATE STREAM `idmp`.`ana_temp` EVENT_WINDOW(start with `环境温度` > 80 end with `环境温度` <= 80 ) TRUE_FOR(10m) FROM `idmp`.`vt_气象传感器02_471544` 
  STREAM_OPTIONS( IGNORE_DISORDER)
  INTO `idmp`.`ana_temp` 
  AS 
    SELECT _twstart+0s as output_timestamp, avg(`环境温度`) as `平均环境温度` FROM idmp.`vt_气象传感器02_471544` where ts >= _twstart and ts <= _twend;
```

### 滑动触发

- 超级表 stb1 的每个子表在每 5 分钟的时间窗口结束后，计算这 5 分钟的 col1 的平均值，每个子表的计算结果分别写入超级表 stb2 的不同子表中。

```SQL
CREATE stream sm1 INTERVAL(5m) SLIDING(5m) FROM stb1 PARTITION BY tbname 
  INTO stb2 
  AS 
    SELECT _twstart, avg(col1) FROM %%tbname 
    WHERE _c0 >= _twstart AND _c0 <= _twend;
```

> 上面 SQL 中的 `from %%tbname where _c0 >= _twstart and _c0 <= _twend` 与 `from %%trows` 的含义是不完全相同的。前者表示计算使用触发分组对应的表中在触发窗口时间段内的数据，窗口内的数据在计算时与 `%%trows` 相比较是有可能有变化的，后者则表示只使用触发时读取到的窗口数据。

- 超级表 stb1 的每个子表从最早的数据开始，在每 5 分钟的时间窗口结束后或从窗口启动 1 分钟后窗口仍然未关闭时，计算窗口内的 col1 的平均值，每个子表的计算结果分别写入超级表 stb2 的不同子表中。

```SQL
CREATE stream sm2 INTERVAL(5m) SLIDING(5m) FROM stb1 PARTITION BY tbname 
  STREAM_OPTIONS(MAX_DELAY(1m) | FILL_HISTORY_FIRST) 
  INTO stb2 
  AS 
    SELECT _twstart, avg(col1) FROM %%tbname WHERE _c0 >= _twstart AND _c0 <= _twend;
```

- 计算电表电流的每分钟平均值，并在窗口打开、关闭时向两个通知地址发送通知，计算历史数据时也发送通知（`NOTIFY_HISTORY`）。

```sql
CREATE STREAM avg_stream INTERVAL(1m) SLIDING(1m) FROM meters 
  NOTIFY ('ws://localhost:8080/notify', 'wss://192.168.1.1:8080/notify?key=foo') ON (WINDOW_OPEN | WINDOW_CLOSE) NOTIFY_OPTIONS(NOTIFY_HISTORY)
  INTO avg_stb
  AS 
    SELECT _twstart, _twend, AVG(current) FROM %%trows;
```

- `meters` 超级表的 `location` 标签使用 `.` 表示位置层级。每小时计算各层级节点的平均电流。若某个子表的 `location = 'California.SanFrancisco.Soma'`，该子表的数据会同时参与 `California`、`California.SanFrancisco` 和 `California.SanFrancisco.Soma` 三个分组。

```SQL
CREATE STREAM rollup_avg_current
  INTERVAL(1h) SLIDING(1h)
  FROM meters ROLLUP BY location
  INTO rollup_avg
  OUTPUT_SUBTABLE(concat(%%1, '_avg'))
  TAGS (
    location VARCHAR(256) AS %%1,
    node_name VARCHAR(64) AS %%rollup_tag
  )
  AS
    SELECT _twstart, avg(current), _trollup_tbcount
    FROM %%trows;
```

### 定时触发

- 每过 1 小时计算表 tb1 中总的数据量，计算结果写入表 tb2 (毫秒库)。

```SQL
CREATE stream sm1 PERIOD(1h) 
  INTO tb2 
  AS
    SELECT cast(_tlocaltime/1000000 AS TIMESTAMP), count(*) FROM tb1;
```

- 每过 1 小时通知 `ws://localhost:8080/notify` 当前系统时间。

```SQL
CREATE stream sm1 PERIOD(1h) 
  NOTIFY("ws://localhost:8080/notify");
```

- 每过 1 天计算智能电表 meters 中各子表的耗电量和，计算结果写入降采样超级表 meters_1d 中并把各子表 TAG 值原样带过来。

```SQL
CREATE stream stream_consumer_energy 
  PERIOD(1d) 
  FROM meters PARTITION BY tbname, groupid, location
  INTO meters_1d (ts, sum_power)
     TAGS (groupid INT AS groupid , location VARCHAR(24) AS location)
  AS 
     SELECT cast(_tlocaltime/1000000 AS timestamp) ,sum(current*voltage) AS sum_power
          FROM meters
          WHERE ts >= cast(_tprev_localtime/1000000 AS timestamp) AND ts <= cast(_tlocaltime/1000000 AS timestamp);
```

- 每周一 00:00:00 计算上周的设备运行汇总，计算结果写入 weekly_summary 表。

```SQL
CREATE STREAM weekly_device_summary
  PERIOD(1w)
  FROM meters PARTITION BY location
  INTO weekly_summary
  AS
    SELECT _wstart AS week_start,
           location,
           AVG(current) AS avg_current,
           MAX(voltage) AS max_voltage,
           COUNT(*) AS record_count
    FROM meters
    INTERVAL(1w)
    PARTITION BY location;
```

- 每月 1 日 00:00:00 计算上月的能耗账单，计算结果写入 monthly_bill 表。

```SQL
CREATE STREAM monthly_energy_bill
  PERIOD(1n)
  FROM meters PARTITION BY location, groupId
  INTO monthly_bill
  AS
    SELECT _wstart AS month_start,
           location,
           groupId,
           SUM(current * voltage) AS total_energy
    FROM meters
    INTERVAL(1n)
    PARTITION BY location, groupId;
```

- 每月 15 日 00:00:00 计算半月结算报表（使用 offset 参数）。

```SQL
CREATE STREAM mid_month_settlement
  PERIOD(1n, 14d)
  FROM meters PARTITION BY location
  INTO mid_month_settlement_table
  AS
    SELECT _wstart AS period_start,
           location,
           SUM(current * voltage) AS total_energy
    FROM meters
    INTERVAL(1n)
    PARTITION BY location;
```

- 每年 1 月 1 日 00:00:00 归档上一年的全量数据。

```SQL
CREATE STREAM yearly_archive
  PERIOD(1y)
  FROM meters PARTITION BY location, groupId
  INTO yearly_archive_table
  AS
    SELECT _wstart AS year_start,
           location,
           groupId,
           AVG(current) AS avg_current,
           SUM(current * voltage) AS total_energy
    FROM meters
    INTERVAL(1y)
    PARTITION BY location, groupId;
```
