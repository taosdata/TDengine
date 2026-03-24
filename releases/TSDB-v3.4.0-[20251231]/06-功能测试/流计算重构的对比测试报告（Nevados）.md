# 流计算重构的对比测试报告（Nevados）

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2025-10-13 | - | 0.1 | 关胜亮 | 编制文档目录结构和内容框架 |
| 2025-10-22 | - | 1.0 | 李珲 | 补充测试结果 |
|  |  |  |  |  |

## 2. 测试目的

本报告旨在对比 TSDB 在重构前后的两个版本（3.3.6.6 vs 3.3.8.x） 上，流计算任务在执行过程中的资源消耗（包括CPU、内存） 与计算性能表现。测试通过复现客户 Nevados 的真实业务场景，模拟多个流计算任务并发执行时，考察新版本在流计算引擎方面的优化效果。

## 3. 测试结论

根据目前已观察到的测试结果趋势，v3.3.8.x 版本相较于 v3.3.6.6，在流计算功能的资源效率方面实现了显著提升。新版本有效降低了 CPU 和内存的资源消耗，同时保持了流计算任务的准确性和低延迟，证实了其优化措施的有效性。具体表现如下：
1. 资源消耗方面：CPU 平均使用率从 x% 降低至 y%，降幅约 z%；内存平均占用从 x GB 减少至 y GB，减少约 z%。
2. 计算延迟方面：流计算任务的平均处理延迟由 x 秒缩短至 y 秒，响应速度提升约 z%。
3. 计算正确性方面：所有流计算任务输出结果均准确无误，与预期结果一致。
后续推进计划
1. 第一阶段：在 Nevados 预生产环境中部署 v3.3.8.x 版本，持续观察系统稳定性与性能表现一周。
2. 第二阶段：如预生产环境验证无误，和客户沟通后，升级至正式环境。

## 4. 测试方法

### 4.1 测试环境

| 配置项 | 规格 |
| --- | --- |
| 服务器配置 | 物理机：8 CPU Cores, 24 GB RAM |
| 磁盘存储 | 700G |
| 操作系统 | CentOS Linux release 7.9.2009 |
| Taosd IP 地址 | 192.168.3.62 |
| taosBenchmark IP地址 | 192.168.3.61 |
| TDengine 版本 | Version A：v3.3.6.6 Version B：v3.3.8.1 |
| 客户端工具 | taosBenchmark |

### 4.2 测试数据与模型

#### 4.2.1 数据模型​

基于 Nevados 业务逻辑
1. 数据库：
```sql {wrap}
CREATE DATABASE `prod` BUFFER 1024 CACHESIZE 10 CACHEMODEL 'last_row' COMP 2 DURATION 50d WAL_FSYNC_PERIOD 3000 MAXROWS 4096 MINROWS 10 STT_TRIGGER 1 KEEP 3650d,3650d,3650d PAGES 256 PAGESIZE 4 PRECISION 'ms' REPLICA 1 WAL_LEVEL 1 VGROUPS 2 SINGLE_STABLE 0 TABLE_PREFIX 0 TABLE_SUFFIX 0 TSDB_PAGESIZE 4 WAL_RETENTION_PERIOD 345600 WAL_RETENTION_SIZE 20971520 KEEP_TIME_OFFSET 0 ENCRYPT_ALGORITHM 'none' SS_CHUNKPAGES 131072 SS_KEEPLOCAL 525600m SS_COMPACT 1 COMPACT_INTERVAL 0d COMPACT_TIME_RANGE 0d,0d COMPACT_TIME_OFFSET 0h
```

1. 超级表（使用 taosBenchmark 创建）：
```sql {wrap}
CREATE STABLE `windspeeds` (`ts` TIMESTAMP, `speed` DOUBLE, `direction` DOUBLE) TAGS (`id` NCHAR(8), `site` NCHAR(8));

CREATE STABLE `snowdepths` (`ts` TIMESTAMP, `distance_raw` DOUBLE, `distance_corrected` DOUBLE, `depth` DOUBLE, `delta_60m` DOUBLE, `temp` DOUBLE, `state` VARCHAR(16), `quality` DOUBLE) TAGS (`id` NCHAR(16), `site` NCHAR(8));

CREATE STABLE `trackers` (`ts` TIMESTAMP, `reg_system_status14` BOOL, `reg_move_enable14` BOOL, `reg_move_enable02` BOOL, `reg_pack7_mv` DOUBLE, `reg_temp_status05` BOOL, `reg_system_status02` BOOL, `reg_temp_status13` BOOL, `reg_battery_status07` BOOL, `reg_temp_status08` BOOL, `reg_system_status15` BOOL, `reg_motor_ma` DOUBLE, `reg_temp_status15` BOOL, `reg_pack5_mv` DOUBLE, `reg_system_status13` BOOL, `reg_battery_status02` BOOL, `reg_temp_status04` BOOL, `reg_move_enable08` BOOL, `reg_move_pitch` DOUBLE, `reg_system_status03` BOOL, `reg_battery_status12` BOOL, `reg_system_status04` BOOL, `reg_temp_status03` BOOL, `reg_battery_status01` BOOL, `reg_pack4_mv` DOUBLE, `reg_move_enable09` BOOL, `reg_temp_status00` BOOL, `reg_move_enable10` BOOL, `reg_panel_mv` DOUBLE, `reg_move_enable13` BOOL, `reg_temp_status02` BOOL, `reg_system_status00` BOOL, `reg_system_status07` BOOL, `reg_roll` DOUBLE, `reg_battery_mv` DOUBLE, `reg_temp_status12` BOOL, `reg_battery_status10` BOOL, `reg_battery_status15` BOOL, `reg_temp_status07` BOOL, `reg_pack1_mv` DOUBLE, `reg_system_status09` BOOL, `reg_battery_status06` BOOL, `reg_move_enable00` BOOL, `reg_system_status12` BOOL, `reg_temp_therm2` DOUBLE, `reg_temp_status10` BOOL, `reg_motor_temp` DOUBLE, `reg_pack3_mv` DOUBLE, `reg_battery_negative_peak` DOUBLE, `reg_move_enable04` BOOL, `xbee_signal` DOUBLE, `reg_temp_status06` BOOL, `reg_battery_status09` BOOL, `reg_pack6_mv` DOUBLE, `reg_temp_status11` BOOL, `reg_move_enable01` BOOL, `reg_battery_status08` BOOL, `reg_move_enable05` BOOL, `reg_system_status10` BOOL, `reg_pack2_mv` DOUBLE, `reg_move_enable15` BOOL, `reg_firmware_rev` DOUBLE, `reg_battery_status13` BOOL, `reg_temp_therm1` DOUBLE, `reg_move_enable11` BOOL, `reg_temp_status14` BOOL, `reg_system_status06` BOOL, `reg_pitch` DOUBLE, `reg_move_enable03` BOOL, `reg_battery_status14` BOOL, `reg_system_status08` BOOL, `reg_battery_status05` BOOL, `reg_battery_status04` BOOL, `reg_battery_status03` BOOL, `reg_battery_status00` BOOL, `reg_battery_positive_peak` DOUBLE, `reg_system_status05` BOOL, `reg_battery_status11` BOOL, `reg_system_status01` BOOL, `reg_battery_ma` DOUBLE, `is_online` BOOL, `mode` VARCHAR(32), `reg_pack8_mv` DOUBLE, `reg_move_enable06` BOOL, `reg_temp_status09` BOOL, `reg_move_enable07` BOOL, `reg_temp_status01` BOOL, `reg_move_enable12` BOOL, `reg_system_status11` BOOL, `reg_battery_rested_mv` DOUBLE, `reg_motor_last_move_avg_ma` DOUBLE, `reg_battery_discharge_net` DOUBLE, `reg_panel_last_charge_mv` DOUBLE, `reg_serial_number` VARCHAR(4), `reg_motor_last_move_peak_ma` DOUBLE, `reg_panel_last_charge_ma` DOUBLE, `reg_day_seconds` DOUBLE, `reg_motor_last_move_min_mv` DOUBLE, `reg_motor_last_move_start_pitch` DOUBLE, `reg_motor_last_move_count` DOUBLE) TAGS (`site` NCHAR(8), `tracker` NCHAR(16), `zone` NCHAR(4));
```

1. 子表：

| 超级表名 | 子表数量 |
| --- | --- |
| windspeeds | **103** |
| snowdepths | **74** |
| trackers | **28851** |

#### 4.2.2 数据模拟​

1. 种子数据​：使用一个子表的一天真实数据作为样本。
2. 写入工具​：使用 `taosBenchmark`的 `sample`模式，模拟持续的数据写入，尽可能复现生产环境的写入压力和模式 。
3. 写入频率：

| 超级表名 | 每张子表的写入频率 |
| --- | --- |
| windspeeds | **30 秒** |
| snowdepths | **30 秒** |
| trackers | **5 分钟** |

### 4.3 流计算任务

本次测试创建了 10 个 流计算任务，涵盖不同类型的窗口聚合计算，以全面评估性能。

#### 4.3.1 流计算任务A-windspeeds_hourly 

1. 逻辑描述及差异说明
2. v3.3.6.6 建流语句
```sql {wrap}
create stream windspeeds_hourly fill_history 1 into prod.windspeeds_hourly as select _wend as window_hourly, site, id, max(speed) as windspeed_hourly_maximum from prod.windspeeds where ts >= '2025-05-07' partition by site, id interval(1h); 
```

1. v3.3.8.x 建流语句
```sql {wrap}
create stream if not exists windspeeds_hourly interval(1h) sliding(1h) from prod.windspeeds partition by site, id stream_options(watermark(1s) | fill_history('2025-05-07 00:00:00.000')) into windspeeds_hourly OUTPUT_SUBTABLE(CONCAT('windspeeds_hourly_', cast(site as varchar), cast(id as varchar))) tags(tag_site nchar(8) as site, tag_id nchar(8) as id) as select _twend as window_hourly, %%1 as site, %%2 as id, max(speed) as windspeed_hourly_maximum from prod.windspeeds where _c0 >= _twstart and _c0 < _twend and site=%%1 and id=%%2;
```

#### 4.3.2 流计算任务B-windspeeds_daily 

1. 逻辑描述及差异说明
2. v3.3.6.6 建流语句
```sql {wrap}
create stream windspeeds_daily fill_history 1 into prod.windspeeds_daily as select _wend as window_daily, site, id, max(windspeed_hourly_maximum) as windspeed_daily_maximum from prod.windspeeds_hourly partition by site, id interval(1d, 5h);
```

1. v3.3.8.x 建流语句
```sql {wrap}
create stream if not exists windspeeds_daily interval(1d, 5h) sliding(1d) from windspeeds_hourly partition by tag_site, tag_id stream_options(watermark(1s) | fill_history) into windspeeds_daily OUTPUT_SUBTABLE(CONCAT('windspeeds_daily_', cast(tag_site as varchar), cast(tag_id as varchar))) tags(tag_site nchar(8) as tag_site, tag_id nchar(8) as tag_id) as select _twend as window_hourly, %%1 as site, %%2 as id, max(windspeed_hourly_maximum) as windspeed_daily_maximum from prod.windspeeds_hourly where _c0 >= _twstart and _c0 < _twend and site=%%1 and id=%%2;
```

#### 4.3.3 流计算任务C-snowdepths_hourly 

1. 逻辑描述及差异说明
2. v3.3.6.6 建流语句
```sql {wrap}
create stream snowdepths_hourly fill_history 1 into prod.snowdepths_hourly as select _wend as window_hourly, site, id, max(depth) as snowdepth_hourly_maximum from prod.snowdepths where _ts >= '2024-01-01' partition by site, id interval(1h);
```

1. v3.3.8.x 建流语句
```sql {wrap}
create stream if not exists snowdepths_hourly interval(1h) sliding(1h) from prod.snowdepths partition by site, id stream_options(watermark(1s) | fill_history('2024-01-01')) into snowdepths_hourly OUTPUT_SUBTABLE(CONCAT('snowdepths_hourly_', cast(site as varchar), cast(id as varchar))) tags(tag_site nchar(8) as site, tag_id nchar(16) as id) as select _twend as window_hourly, %%1 as site, %%2 as id, max(depth) as snowdepth_hourly_maximum from prod.snowdepths where _c0 >= _twstart and _c0 < _twend and site=%%1 and id=%%2; 
```

#### 4.3.4 流计算任务D-snowdepths_daily 

1. 逻辑描述及差异说明
2. v3.3.6.6 建流语句
```sql {wrap}
create stream snowdepths_daily fill_history 1 into prod.snowdepths_daily as select _wend as window_daily, site, id, max(snowdepth_hourly_maximum) as snowdepth_daily_maximum from prod.snowdepths_hourly partition by site, id interval(1d, 5h);
```

1. v3.3.8.x 建流语句
```sql {wrap}
create stream if not exists snowdepths_daily interval(1d, 5h) sliding(1d) from prod.snowdepths_hourly partition by tag_site, tag_id stream_options(watermark(1s) | fill_history) into snowdepths_daily OUTPUT_SUBTABLE(CONCAT('snowdepths_daily_', cast(tag_site as varchar), cast(tag_id as varchar))) tags(tag_site nchar(8) as tag_site, tag_id nchar(16) as tag_id) as select _twend as window_daily, %%1 as site, %%2 as id, max(snowdepth_hourly_maximum) as snowdepth_daily_maximum from prod.snowdepths_hourly where _c0 >= _twstart and _c0 < _twend and site=%%1 and id=%%2;   
```

#### 4.3.5 流计算任务E-off_target_trackers 

1. 逻辑描述及差异说明
2. v3.3.6.6 建流语句
```sql {wrap}
create stream off_target_trackers ignore expired 0 ignore update 0 into prod.off_target_trackers as select _wend as _ts, site, tracker, last(reg_pitch) as off_target_pitch, last(mode) as mode from prod.trackers where _ts >= '2024-04-23' and _ts < now() + 1h and abs(reg_pitch-reg_move_pitch) > 2 partition by site, tracker interval(15m) sliding(5m);
```

1. v3.3.8.x 建流语句
```sql {wrap}
create stream if not exists off_target_trackers interval(15m) sliding(5m) from trackers partition by site, tracker stream_options(watermark(1s) | IGNORE_DISORDER) into off_target_trackers OUTPUT_SUBTABLE(CONCAT('off_target_trackers_', cast(site as varchar), cast(tracker as varchar))) tags(tag_site nchar(8) as site, tag_tracker nchar(16) as tracker) as select _twend as _ts, %%1 as site, %%2 as tracker, last(reg_pitch) as off_target_pitch, last(mode) as mode from prod.trackers where _c0 >= _twstart and _c0 < _twend and abs(reg_pitch-reg_move_pitch) > 2 and _c0 < cast((_tlocaltime/1000000 + 1h) as timestamp) and site=%%1 and tracker=%%2;
```

#### 4.3.6 流计算任务F-trackers_motor_current_state_window 

1. 逻辑描述及差异说明
2. v3.3.6.6 建流语句
```sql {wrap}
create stream trackers_motor_current_state_window into prod.trackers_motor_current_state_window as select _ts, site, tracker, max(`reg_motor_last_move_peak_mA` / 1000) as max_motor_current from prod.trackers where _ts >= '2024-09-22' and _ts < now() + 1h and `reg_motor_last_move_peak_mA` > 0 partition by tbname/*, site, tracker */ state_window(cast(reg_motor_last_move_count as int));
```

1. v3.3.8.x 建流语句
```sql {wrap}
create stream if not exists trackers_motor_current_state_window
state_window(cast(reg_motor_last_move_count as int))
from prod.trackers partition by tbname/*, site, tracker */ 
stream_options(watermark(1s) | fill_history('2024-09-22'))
into trackers_motor_current_state_window OUTPUT_SUBTABLE(CONCAT('trackers_state_window_', tbname))
as select _c0, site, tracker, 
max(reg_motor_last_move_peak_mA / 1000) as max_motor_current from %%tbname where _c0 >= _twstart and _c0 <= _twend  and _c0 < cast((_tlocaltime/1000000 + 1h) as timestamp) and reg_motor_last_move_peak_mA > 0; 
```

#### 4.3.7 流计算任务G-**kpi_db_test **

1. 逻辑描述及差异说明
2. v3.3.6.6 建流语句
```sql {wrap}
create stream kpi_db_test trigger window_close watermark 10m fill_history 1 ignore update 1 into kpi_db_test as select _wend as window_end, case when last(_ts) is not null then 1 else 0 end as db_online from trackers where _ts >= '2024-10-04T00:00:00.000Z' interval(1h) sliding(1h);
```

1. v3.3.8.x 建流语句
```sql {wrap}
create stream if not exists kpi_db_test interval(1h) sliding(1h) from prod.trackers stream_options(watermark(1s) | IGNORE_DISORDER | fill_history('2024-10-04T00:00:00.000Z')) into kpi_db_test as select _twend as window_end, case when last(_c0) is not null then 1 else 0 end as db_online from prod.trackers where _c0 >= _twstart and _c0 < _twend;
```

#### 4.3.8 流计算任务H-**kpi_trackers_test **

1. 逻辑描述及差异说明
2. v3.3.6.6 建流语句
```sql {wrap}
create stream if not exists kpi_trackers_test trigger window_close watermark 10m fill_history 1 ignore update 1 into kpi_trackers_test as select _wend as window_end, site, zone, tracker, case when ((min(abs(reg_pitch - reg_move_pitch)) <= 2) or (min(reg_temp_therm2) < -10) or (max(reg_temp_therm2) > 60) or (last(reg_system_status14) = true)) then 1 else 0 end as tracker_on_target, case when last(reg_pitch) is not null then 1 else 0 end as tracker_online from trackers where _ts >= '2024-10-04T00:00:00.000Z' partition by tbname interval(1h) sliding(1h);
```

1. v3.3.8.x 建流语句
```sql {wrap}
create stream if not exists kpi_trackers_test interval(1h) sliding(1h) from prod.trackers partition by tbname stream_options(watermark(10m) | IGNORE_DISORDER | fill_history('2024-10-04T00:00:00.000Z')) into kpi_trackers_test OUTPUT_SUBTABLE(CONCAT('kpi_trackers_test_', tbname)) as select _twend as window_end, site, zone, tracker, case when ((min(abs(reg_pitch - reg_move_pitch)) <= 2) or (min(reg_temp_therm2) < -10) or (max(reg_temp_therm2) > 60) or (last(reg_system_status14) = true)) then 1 else 0 end as tracker_on_target, case when last(reg_pitch) is not null then 1 else 0 end as tracker_online from %%tbname where _c0 >= _twstart and _c0 < _twend;
```

#### 4.3.9 流计算任务I-**kpi_zones_test **

1. 逻辑描述及差异说明
2. v3.3.6.6 建流语句
```sql {wrap}
create stream kpi_zones_test trigger window_close watermark 10m fill_history 1 ignore update 1 into kpi_zones_test as select _wend as window_end, site, zone, case when last(_ts) is not null then 1 else 0 end as zone_online from trackers where _ts >= '2024-10-04T10:00:00.000Z' partition by site, zone interval(1h) sliding(1h);
```

1. v3.3.8.x 建流语句
```sql {wrap}
create stream if not exists kpi_zones_test interval(1h) sliding(1h) from prod.trackers partition by site, zone stream_options(watermark(10m) | IGNORE_DISORDER | fill_history('2024-10-04T10:00:00.000Z')) into kpi_zones_test OUTPUT_SUBTABLE(CONCAT('kpi_zones_test_', cast(site as varchar),  cast(zone as varchar))) tags(tag_site nchar(8) as site, tag_zone nchar(4) as zone) as select _twend as window_end, %%1 as site, %%2 as zone, case when last(_c0) is not null then 1 else 0 end as zone_online from prod.trackers where _c0 >= _twstart and _c0 < _twend and site=%%1 and zone=%%2;
```

#### 4.3.10 流计算任务J-**kpi_sites_test **

1. 逻辑描述及差异说明
2. v3.3.6.6 建流语句
```sql {wrap}
create stream kpi_sites_test trigger window_close watermark 10m fill_history 1 ignore update 1 into  kpi_sites_test as select _wend as window_end, site, case when last(_ts) is not null then 1 else 0 end as site_online from  trackers where _ts >= '2024-10-04T00:00:00.000Z' partition by site interval(1h) sliding(1h);
```

1. v3.3.8.x 建流语句
```sql {wrap}
create stream if not exists kpi_sites_test interval(1h) sliding(1h) from trackers partition by site stream_options(watermark(10m) | IGNORE_DISORDER | fill_history('2024-10-04T00:00:00.000Z')) into kpi_sites_test OUTPUT_SUBTABLE(CONCAT('kpi_sites_test_', cast(site as varchar))) tags(tag_site nchar(8) as site) as select _twend as window_end, %%1 as site, case when last(_c0) is not null then 1 else 0 end as site_online from prod.trackers where _c0 >= _twstart and _c0 < _twend and site=%%1;
```

### 4.4 测试流程

1. 环境部署​：在纯净的测试环境中分别部署 TDengine v3.3.6.6 和 v3.3.8.0。
2. 元数据准备​：创建数据库、超级表、子表。
3. 流计算创建​：执行预先准备好的 `CREATE STREAM` 语句，创建所有流计算任务 。
4. 数据写入与负载模拟​：启动 `taosBenchmark`模拟数据写入。
5. 监控与数据收集​：测试持续运行 `[例如：2小时]`。在此期间，使用 `taosKeeper`或其他监控工具，持续收集并记录以下指标：
  - 系统整体及 TDengine 进程的 CPU 使用率。
  - 系统整体及 TDengine 进程的内存占用。
  - 流计算相关状态（如 `SHOW STREAMS;`查看计算进度 ）。
1. 结果验证​：检查流计算输出表的计算结果，确保其准确性。

## 5. 测试结果

### 5.1 流计算任务 A-windspeeds_hourly 

| 观测指标 | v3.3.6.6 | v3.3.8.x | 指标对比 |
| --- | --- | --- | --- |
| CPU 平均使用率（%） |  |  | (C2-B2)/B2 |
| CPU 使用率峰值（%） |  |  | (C3-B3)/B3 |
| CPU 使用值 p99（%） |  |  | (C4-B4)/B4 |
| 内存平均占用（GB） |  |  | (C5-B5)/B5 |
| 内存占用峰值（GB） |  |  | (C6-B6)/B6 |
| 计算结果延迟(ms) |  |  | (C7-B7)/B7 |
| 计算结果准确性 | - | - | 一致 |


### 5.2 流计算任务 B-windspeeds_daily 

| 观测指标 | v3.3.6.6 | v3.3.8.x | 指标对比 |
| --- | --- | --- | --- |
| CPU 平均使用率（%） |  |  | (C2-B2)/B2 |
| CPU 使用率峰值（%） |  |  | (C3-B3)/B3 |
| CPU 使用值 p99（%） |  |  | (C4-B4)/B4 |
| 内存平均占用（GB） |  |  | (C5-B5)/B5 |
| 内存占用峰值（GB） |  |  | (C6-B6)/B6 |
| 计算结果延迟(ms) |  |  | (C7-B7)/B7 |
| 计算结果准确性 | - | - | 一致 |

### 5.3 流计算任务 C-snowdepths_hourly 

| 观测指标 | v3.3.6.6 | v3.3.8.x | 指标对比 |
| --- | --- | --- | --- |
| CPU 平均使用率（%） |  |  | (C2-B2)/B2 |
| CPU 使用率峰值（%） |  |  | (C3-B3)/B3 |
| CPU 使用值 p99（%） |  |  | (C4-B4)/B4 |
| 内存平均占用（GB） |  |  | (C5-B5)/B5 |
| 内存占用峰值（GB） |  |  | (C6-B6)/B6 |
| 计算结果延迟(ms) |  |  | (C7-B7)/B7 |
| 计算结果准确性 | - | - | 一致 |


### 5.4 流计算任务 D-snowdepths_daily 

### 5.5 流计算任务 E-off_target_trackers 

v3.3.6.6: 开始:2025-10-24 11:41:15.596719, 结束:2025-10-24 15:41:26.257148
v3.3.8.x: 开始:2025-10-23 09:03:00.197774, 结束:2025-10-23 13:03:10.562998
| 观测指标 | v3.3.6.6 | v3.3.8.x | 指标对比 |
| --- | --- | --- | --- |
| CPU 平均使用率（%） | 1.43 | 10.99 | (C2-B2)/B2 |
| CPU 使用率峰值（%） | 67.9 | 86.03 | (C3-B3)/B3 |
| CPU 使用值 p99（%） | 26.82 |  | (C4-B5)/B5 |
| 内存平均占用（GB） | 2.28 | 2.97 | (C5-B6)/B6 |
| 内存占用峰值（GB） | 2.84 | 2.87 | (C6-B6)/B6 |
| 计算结果延迟(ms) | 1374.35 | 23123.69 | (C7-B7)/B7 |
| 计算结果准确性 | - | - | 一致 |


### 5.6 流计算任务 F-trackers_motor_current_state_window 

v3.3.6.6: 开始:2025-10-23 13:16:37.946739, 结束:2025-10-23 17:16:48.442292
v3.3.8.x: 开始:2025-10-24 15:53:46.781299, 结束:2025-10-24 19:53:57.931774
| 观测指标 | v3.3.6.6 | v3.3.8.x | 指标对比 |
| --- | --- | --- | --- |
| CPU 平均使用率（%） | 1.06 | 2 | (C2-B2)/B2 |
| CPU 使用率峰值（%） | 46.01 | 67.81 | (C3-B3)/B3 |
| CPU 使用值 p99（%） | 13.26 |  | (C4-B4)/B4 |
| 内存平均占用（GB） | 2.72 | 2.87 | (C5-B5)/B5 |
| 内存占用峰值（GB） | 2.92 | 2.96 | (C6-B6)/B6 |
| 计算结果延迟(ms) | 1029.76 | 6496.76 | (C7-B7)/B7 |
| 计算结果准确性 | - | - | 一致 |


### 5.7 流计算任务G-**kpi_db_test **

| 观测指标 | v3.3.6.6 | v3.3.8.x | 指标对比 |
| --- | --- | --- | --- |
| CPU 平均使用率（%） |  |  | (C2-B2)/B2 |
| CPU 使用率峰值（%） |  |  | (C3-B3)/B3 |
| CPU 使用值 p99（%） |  |  | (C4-B4)/B4 |
| 内存平均占用（GB） |  |  | (C5-B5)/B5 |
| 内存占用峰值（GB） |  |  | (C6-B6)/B6 |
| 计算结果延迟(ms) |  |  | (C7-B7)/B7 |
| 计算结果准确性 | - | - | 一致 |


### 5.8 流计算任务H-**kpi_trackers_test **

| 观测指标 | v3.3.6.6 | v3.3.8.x | 指标对比 |
| --- | --- | --- | --- |
| CPU 平均使用率（%） |  |  | (C2-B2)/B2 |
| CPU 使用率峰值（%） |  |  | (C3-B3)/B3 |
| CPU 使用值 p99（%） |  |  | (C4-B4)/B4 |
| 内存平均占用（GB） |  |  | (C5-B5)/B5 |
| 内存占用峰值（GB） |  |  | (C6-B6)/B6 |
| 计算结果延迟(ms) |  |  | (C7-B7)/B7 |
| 计算结果准确性 | - | - | 一致 |

### 5.9 流计算任务I-**kpi_zones_test **

| 观测指标 | v3.3.6.6 | v3.3.8.x | 指标对比 |
| --- | --- | --- | --- |
| CPU 平均使用率（%） |  |  | (C2-B2)/B2 |
| CPU 使用率峰值（%） |  |  | (C3-B3)/B3 |
| CPU 使用值 p99（%） |  |  | (C4-B4)/B4 |
| 内存平均占用（GB） |  |  | (C5-B5)/B5 |
| 内存占用峰值（GB） |  |  | (C6-B6)/B6 |
| 计算结果延迟(ms) |  |  | (C7-B7)/B7 |
| 计算结果准确性 | - | - | 一致 |


### 5.10 流计算任务J-**kpi_sites_test **

| 观测指标 | v3.3.6.6 | v3.3.8.x | 指标对比 |
| --- | --- | --- | --- |
| CPU 平均使用率（%） |  |  | (C2-B2)/B2 |
| CPU 使用率峰值（%） |  |  | (C3-B3)/B3 |
| CPU 使用值 p99（%） |  |  | (C4-B4)/B4 |
| 内存平均占用（GB） |  |  | (C5-B5)/B5 |
| 内存占用峰值（GB） |  |  | (C6-B6)/B6 |
| 计算结果延迟(ms) |  |  | (C7-B7)/B7 |
| 计算结果准确性 | - | - | 一致 |

### 5.11 混合场景

3.3.6.6: 开始时间: 2025-10-23 15:01:33.881922, 结束时间: 2025-10-24 03:02:04.872105
3.3.8.x: 
运行时间：2025.10.21 23:00 ~ 2025.10.22  9:00
cpu/mem 取值和计算SQL：
```python {wrap}
select avg(cpu_engine) as cpu_engine_avg, PERCENTILE(cpu_engine,99) as cpu_engine_p99, PERCENTILE(cpu_engine,99.9) as cpu_engine_p999, max(cpu_engine) as cpu_engine_max, avg(mem_engine) as mem_engine_avg, PERCENTILE(mem_engine,99) as mem_engine_p99, PERCENTILE(mem_engine,99.9) as mem_engine_p999, max(mem_engine) as mem_engine_max from log.dinfo_1_cluster_4781730678926514525 where _ts >= "2025-10-21 22:41:48" and _ts <= "2025-10-22 08:42:12";
```


| 观测指标 |  | v3.3.6.6 | v3.3.8.x | 指标对比 |
| --- | --- | --- | --- | --- |
| CPU 平均使用率（%） |  | 2.11 | 11.58 | (D2-C2)/C2 |
| CPU 使用率峰值（%） |  | 93.7 | 88.1 | (D3-C3)/C3 |
| CPU 使用值 p99（%） |  | 49.72 | 81.85 | (D4-C4)/C4 |
| 内存平均占用（GB） |  | 2.89 | 3.03 | (D5-C5)/C5 |
| 内存占用峰值（GB） |  | 3.29 | 3.71 | (D6-C6)/C6 |
| 计算结果延迟 | 任务 A（ms） | 237.52 | 136.04 | (D7-C7)/C7 |
|  | 任务 B（ms） | 559 | 294.71 | (D8-C8)/C8 |
|  | 任务 C（ms） | 236.06 | 104.26 | (D9-C9)/C9 |
|  | 任务 D（ms） | 522 | 254.07 | (D10-C10)/C10 |
|  | 任务 E（ms） | 1319.8 | 24096.23 | (D11-C11)/C11 |
|  | 任务 F（ms） | 942.49 | 24769.3 | (D12-C12)/C12 |
|  | 任务 G（ms） | 519.72 | 1763.2 | (D13-C13)/C13 |
|  | 任务 H（ms） | 1667.91 | 19244.99 | (D14-C14)/C14 |
|  | 任务 I（ms） | 528.07 | 2925.39 | (D15-C15)/C15 |
|  | 任务 J（ms） | 526.71 | 502.89 | (D16-C16)/C16 |


## 6. 纳入发版前流程

为确保流计算性能持续优化且避免迭代中出现回退，本对比测试流程将纳入发版前验证体系。具体实施步骤如下：
1. 环境准备与初始化​：执行 `environment_build.sh`脚本，自动构建测试环境。
2. 自动化测试执行​：执行 `run_performance_test.sh`脚本，自动加载测试数据，并执行预设的流计算任务与查询负载。
3. 结果校验与报告生成​：执行 `check_and_report.sh`脚本，自动采集性能数据，比对关键指标（如 CPU /内存消耗、计算延迟），并输出是否通过的结论性报告。
