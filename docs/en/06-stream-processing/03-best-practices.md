---
sidebar_label: Deployment and Design
title: Deployment and Design
description: Stream processing deployment, stream design, and maintenance suggestions
---

## Best Practices

The redesigned stream processing engine offers greater flexibility and removes many previous limitations. While availability has been improved, it also introduces higher requirements for proper use.

### Deployment

- Deploy snodes on dedicated dnodes to minimize the impact of stream processing on database reads and writes. These dnodes should not host vnodes, mnodes, or qnodes.
- Deploy multiple snodes within the cluster to ensure high availability.
- Create multiple snodes in advance before defining streams to achieve better load balancing.
- When stream processing workloads are heavy, scale out by adding additional snodes to balance the load.

### Configuration

- Configure the number of stream-related threads according to the deployment method and workload. A higher thread count increases CPU resource consumption, while a lower thread count reduces it.
- Configure the maximum stream buffer size based on the deployment method. For heavier workloads or when running many concurrent streams, increase the buffer size—especially when snodes are deployed on dedicated nodes.

### Designing Streams

Before creating a stream, users should carefully review the following key checkpoints. Once clarified, streams can be designed and used accordingly:

- Choose a trigger type based on business characteristics: If you need to process after each one or more data records are written, choose a count trigger. If you need to process when window conditions are met, choose a window trigger. If you need periodic computation based on event time, choose a sliding trigger. If you need periodic computation based on processing time, choose a periodic trigger.
- Choose window trigger options based on timeliness requirements: In addition to selecting the window type, decide whether to trigger on window open, window close, or both. You may also choose whether to compute promptly before the window closes using MAX_DELAY(delay_time).
- Select the trigger table based on event sources: The event source table is used as the trigger table. For periodic triggers, no trigger table is required. However, if grouped outputs or the trigger table dataset (%%trows) during the scheduled interval are needed, a trigger table must be specified.
- Ensure time sequence consistency between source and trigger tables: If the source table and the trigger table are different, ensure that the source table contains valid data when the trigger table event is triggered. Otherwise, computation accuracy may be affected.
- Decide grouping based on final business requirements: For computations on a supertable, no grouping is required for global aggregation. Use tag-based grouping if aggregation by certain tags is needed. Use child-table grouping if results are required at the individual table level.
- Choose grouping and output subtables based on how stream results will be used:
  - Each group can have its own independent output subtable. However, if there are too many groups this may result in too many result tables. Depending on how the results will be used and on system resource limits, you can decide whether each group really needs its own output subtable.
  - If multiple groups can be merged into a single subtable, you can configure those groups to use the same output table name ([OUTPUT_SUBTABLE(tbname_expr)]). Combined with a composite primary key design in the output table, this allows group results to be merged.
- Use the optimal data writing method: The best ingestion pattern for stream processing is sequential writes per group. If there are a large number of out-of-order writes, update writes, or data deletions, this can trigger extensive recalculation. If sequential writing can be guaranteed, computation efficiency will improve significantly.
- Check the degree of out-of-order writes: Based on the out-of-order behavior of each child table, determine whether a WATERMARK is needed and what an appropriate WATERMARK duration should be.
- Assess the impact of out-of-order data on stream processing:
  - If out-of-order data is present, confirm whether it affects the correctness of results. In scenarios where timeliness is more important, or where trigger table disorder does not affect computation results, you can use STREAM_OPTIONS(IGNORE_DISORDER) to ignore the out-of-order data.
  - If there are severe out-of-order records from far in the past (where the event time is much earlier than the current processed event time) and these records do not affect correctness, or their timeliness has already been lost, you can mark them as expired and ignore them with STREAM_OPTIONS(EXPIRED_TIME(exp_time)).
- Verify the validity of recalculation for stream results: Out-of-order, update, and delete scenarios are mainly addressed through recalculation. If recalculation is not idempotent or produces invalid results, correctness may be affected. This should be judged based on business requirements.
- Check the impact of deletions on stream processing: If deletions occur and results need to be recomputed based on deleted data, use STREAM_OPTIONS(DELETE_RECALC). For count windows, only windows whose sliding step is 1, such as `COUNT_WINDOW(1)` and `COUNT_WINDOW(n, 1)`, support this option.
- Confirm whether historical data needs to be computed and how:
  - If data already exists in the database before the stream is created, it may need to be computed. Depending on business requirements and logic, confirm whether historical data should be prioritized over real-time data. For example, COUNT_WINDOW triggers should prioritize historical data, otherwise windows may not align properly.
  - To prioritize historical data, specify STREAM_OPTIONS(FILL_HISTORY_FIRST). Otherwise, specify STREAM_OPTIONS(FILL_HISTORY).
- Confirm the level of real-time requirements for stream processing: If your business requires very high timeliness for notifications or computations, you can specify STREAM_OPTIONS(LOW_LATENCY_CALC). This mode consumes more computing resources.
- Clarify the purpose of stream processing: If you only need an event trigger notification without computation, you can use the notification-only mode (i.e., no computation statement specified).
- Confirm how computed results will be used: If only result notifications are required and results do not need to be saved, use the notify-only option (STREAM_OPTIONS(CALC_NOTIFY_ONLY)).
- Verify the reliability of result writes: If the primary key of a computed result is NULL, that result will be discarded.
  - If the query statement includes a grouping clause and results from different groups are written into the same subtable, records with identical timestamps may overwrite each other.
  - If multiple triggers from the same group produce results with the same primary key timestamp, they will overwrite each other.
  - If multiple triggers (including recalculations) from the same group produce results with different primary key timestamps, they will not overwrite each other.

### Stream Maintenance

In the stream status display (query the table information_schema.ins_streams), several detailed status indicators are listed. Examples include whether real-time computations are keeping up with progress, how many recalculations (and what ratio) have occurred, and any error messages. Users and administrators should monitor this information to determine whether stream processing is functioning normally, and use it as a basis for analysis and optimization.

### Stream Creation Example

#### Count Window Trigger

- Each time one row is written into table tb1, compute the average value of column col1 in table tb2 over the past 5 minutes up to that moment, and write the result into table tb3.

```SQL
CREATE stream sm1 count_window(1) FROM tb1 
  INTO tb3 AS
    SELECT _twstart, avg(col1) FROM tb2 
    WHERE _c0 >= _twend - 5m AND _c0 <= _twend;
```

- Each time 10 rows are written into table tb1 where column col1 is greater than 0, compute the average of column col1 for those 10 rows. The result does not need to be saved but must be sent as a notification to ws://localhost:8080/notify.

```SQL
CREATE stream sm2 count_window(10, 1, col1) FROM tb1 
  STREAM_OPTIONS(CALC_ONTIFY_ONLY | PRE_FILTER(col1 > 0)) 
  NOTIFY("ws://localhost:8080/notify") ON (WINDOW_CLOSE) 
  AS 
    SELECT avg(col1) FROM %%trows;
```

#### Event Window Trigger

- When the ambient temperature exceeds 80° and remains above that threshold for more than 10 minutes, compute the average ambient temperature.

```SQL
CREATE STREAM `idmp`.`ana_temp` EVENT_WINDOW(start with `temp` > 80 end with `temp` <= 80 ) TRUE_FOR(10m) FROM `idmp`.`vt_envsens02_471544` 
  STREAM_OPTIONS( IGNORE_DISORDER)
  INTO `idmp`.`ana_temp` 
  AS 
    SELECT _twstart+0s as output_timestamp, avg(`temp`) as `avgtemp` FROM idmp.`vt_engsens02_471544` where ts >= _twstart and ts <= _twend;
```

#### Sliding Trigger

- For each subtable of supertable stb1, at the end of every 5-minute time window, compute the average of column col1 over that interval. The results for each subtable are written separately into different subtables of supertable stb2.

```SQL
CREATE stream sm1 INTERVAL(5m) SLIDING(5m) FROM stb1 PARTITION BY tbname 
  INTO stb2 
  AS 
    SELECT _twstart, avg(col1) FROM %%tbname 
    WHERE _c0 >= _twstart AND _c0 <= _twend;
```

In the SQL above, `FROM %%tbname WHERE _c0 >= _twstart AND _c0 <= _twend` and `FROM %%trows` are not equivalent. The former means the computation uses data from the trigger group’s corresponding table within the window’s time range; those in-window rows may differ from what `%%trows` saw at trigger time. The latter means the computation uses only the window data captured at the moment of triggering.

- For each subtable of supertable stb1, starting from the earliest data, compute the average of col1 for each 5-minute time window either when the window closes or when 1 minute has elapsed since the window opened and it is still not closed. Write each subtable’s result to a separate subtable under supertable stb2.

```SQL
CREATE stream sm2 INTERVAL(5m) SLIDING(5m) FROM stb1 PARTITION BY tbname 
  STREAM_OPTIONS(MAX_DELAY(1m) | FILL_HISTORY_FIRST) 
  INTO stb2 
  AS 
    SELECT _twstart, avg(col1) FROM %%tbname WHERE _c0 >= _twstart AND _c0 <= _twend;
```

- Compute the per-minute average of the meter current, and send notifications to two target addresses when the window opens and closes. Do not send notifications during historical computation, and do not allow notifications to be dropped on delivery failure (pause and retry until delivered).

```sql
CREATE STREAM avg_stream INTERVAL(1m) SLIDING(1m) FROM meters 
  NOTIFY ('ws://localhost:8080/notify', 'wss://192.168.1.1:8080/notify?key=foo') ON ('WINDOW_OPEN', 'WINDOW_CLOSE') NOTIFY_OPTIONS(NOTIFY_HISTORY | ON_FAILURE_PAUSE)
  INTO avg_stb
  AS 
    SELECT _twstart, _twend, AVG(current) FROM %%trows;
```

- The `location` tag of the `meters` supertable uses `.` to represent a location hierarchy. Compute the average current for each hierarchy node every hour. If a child table has `location = 'California.SanFrancisco.Soma'`, its data participates in three groups: `California`, `California.SanFrancisco`, and `California.SanFrancisco.Soma`.

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

#### Scheduled Trigger

- Every hour, compute the total number of rows in table tb1 and write the result to table tb2 (in a millisecond-precision database).

```SQL
CREATE stream sm1 PERIOD(1h) 
  INTO tb2 
  AS
    SELECT cast(_tlocaltime/1000000 AS TIMESTAMP), count(*) FROM tb1;
```

- Every hour, send a notification with the current system time to `ws://localhost:8080/notify`.

```SQL
CREATE stream sm1 PERIOD(1h) 
  NOTIFY("ws://localhost:8080/notify");
```

- Calculate the sum of power consumption for each subtable in the smart meter supertable meters every day, write the calculation results into the downsampled supertable meters_1d, and carry over the TAG values from each subtable.

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

- Every Monday at 00:00:00, compute the weekly device operation summary for the previous week and write the results to the weekly_summary table.

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

- On the 1st of each month at 00:00:00, compute the energy consumption bill for the previous month and write the results to the monthly_bill table.

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

- On the 15th of each month at 00:00:00, compute the mid-month settlement report (using the offset parameter).

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

- On January 1st at 00:00:00 each year, archive the full data from the previous year.

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
