# Nevados 新旧版本的流计算对比测试报告

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2025-10-13 | - | 0.1 | 关胜亮 | 编制文档目录结构和内容框架 |
| 2025-11-23 | - | 0.2 | 李珲 | 补充测试结果 |
| 2025-11-24 | - | 0.3 | 邝金清 | 补充测试结果 |
| 2025-11-24 | 2025-11-24 | 1.0 | 关胜亮 | 发布 |

## 2. 测试目的

本报告旨在对比 TSDB 在重构前后的两个版本（3.3.6.28 vs 3.3.8.4） 上，流计算任务在执行过程中的资源消耗（包括 CPU、内存） 与计算性能表现。在客户 Nevados 的真实业务场景中，部署多个流计算任务并发执行时，考察新版本在流计算引擎方面的优化效果。

## 3. 测试结论

根据目前已观察到的测试结果趋势，v3.3.8.4 版本相较于 v3.3.6.28，在流计算功能的资源效率方面实现了显著提升。新版本有效降低了 CPU 和内存的资源消耗，同时保持了流计算任务的准确性和低延迟，证实了其优化措施的有效性。具体表现如下：
1. 资源消耗方面：CPU 平均使用率从 321.73% 降低至 30.29%，降幅约 90.59%；内存平均占用从 8.65 GB 减少至 1.49 GB，减少约 82.77%。
2. 计算延迟方面：流计算任务的平均处理延迟由 1 小时(通过 last 结果预估)缩短至 5 分钟以内(通过 Debug 日志确定)，响应速度提升 92%。
3. 计算正确性方面：所有流计算任务输出结果均准确无误，与预期结果一致。
推进计划
1. 第一阶段：在 Nevados 预生产环境中部署 v3.3.8.4 版本，持续观察系统稳定性与性能表现一周（已完成）
2. 第二阶段：将客户的 3.3.8.4 版本升级至正式环境（客户确认中）

## 4. 测试方法

### 4.1 测试环境

| 环境 | 配置规格 |  |
| --- | --- | --- |
| 生产环境(V3.3.6.28.202510230320) | 7.5 核， 22 GB | cpu: 7500m，memory: 22Gi |
| 测试环境(V3.3.8.4.202511180636) | 4 核， 16 GB | cpu: "4"， memory: 16Gi |

### 4.2 测试数据与模型

#### 4.2.1 数据模型​

基于 Nevados 实际业务
1. 数据库：
```sql {wrap}
生产环境的数据库：
taos> show create database prod\G;
*************************** 1.row ***************************
       Database: prod
Create Database: CREATE DATABASE `prod` BUFFER 96 CACHESIZE 10 CACHEMODEL 'last_row' COMP 2 DURATION 50d WAL_FSYNC_PERIOD 3000 MAXROWS 4096 MINROWS 100 STT_TRIGGER 8 KEEP 3650d,3650d,3650d PAGES 256 PAGESIZE 4 PRECISION 'ms' REPLICA 1 WAL_LEVEL 1 VGROUPS 2 SINGLE_STABLE 0 TABLE_PREFIX 0 TABLE_SUFFIX 0 TSDB_PAGESIZE 4 WAL_RETENTION_PERIOD 345600 WAL_RETENTION_SIZE 2097152 KEEP_TIME_OFFSET 0 ENCRYPT_ALGORITHM 'none' S3_CHUNKPAGES 262144 S3_KEEPLOCAL 5256000m S3_COMPACT 0 COMPACT_INTERVAL 0d COMPACT_TIME_RANGE 0d,0d COMPACT_TIME_OFFSET 0h

测试环境的数据库：
taos> show create database prod\G;
*************************** 1.row ***************************
       Database: prod
Create Database: CREATE DATABASE `prod` BUFFER 256 CACHESIZE 1 CACHEMODEL 'none' COMP 2 DURATION 50d WAL_FSYNC_PERIOD 3000 MAXROWS 4096 MINROWS 100 STT_TRIGGER 2 KEEP 3650d,3650d,3650d PAGES 256 PAGESIZE 4 PRECISION 'ms' REPLICA 1 WAL_LEVEL 1 VGROUPS 2 SINGLE_STABLE 0 TABLE_PREFIX 0 TABLE_SUFFIX 0 TSDB_PAGESIZE 4 WAL_RETENTION_PERIOD 3600 WAL_RETENTION_SIZE 0 KEEP_TIME_OFFSET 0 ENCRYPT_ALGORITHM 'none' SS_CHUNKPAGES 131072 SS_KEEPLOCAL 525600m SS_COMPACT 1 COMPACT_INTERVAL 0d COMPACT_TIME_RANGE 0d,0d COMPACT_TIME_OFFSET 0h
Query OK, 1 row(s) in set (0.000840s)
```

区别：

| 数据库参数 | 生产环境(V3.3.6.28.202510230320) | 测试环境(V3.3.8.4.202511180636) |
| --- | --- | --- |
| BUFFER | 96 | 256 |
| STT_TRIGGER | 8 | 2 |
| WAL_RETENTION_PERIOD | 345600 | 3600 |

1. 超级表
```sql {wrap}
CREATE STABLE `windspeeds` (`ts` TIMESTAMP, `speed` DOUBLE, `direction` DOUBLE) TAGS (`id` NCHAR(8), `site` NCHAR(8));

CREATE STABLE `snowdepths` (`ts` TIMESTAMP, `distance_raw` DOUBLE, `distance_corrected` DOUBLE, `depth` DOUBLE, `delta_60m` DOUBLE, `temp` DOUBLE, `state` VARCHAR(16), `quality` DOUBLE) TAGS (`id` NCHAR(16), `site` NCHAR(8));

CREATE STABLE `trackers` (`ts` TIMESTAMP, `reg_system_status14` BOOL, `reg_move_enable14` BOOL, `reg_move_enable02` BOOL, `reg_pack7_mv` DOUBLE, `reg_temp_status05` BOOL, `reg_system_status02` BOOL, `reg_temp_status13` BOOL, `reg_battery_status07` BOOL, `reg_temp_status08` BOOL, `reg_system_status15` BOOL, `reg_motor_ma` DOUBLE, `reg_temp_status15` BOOL, `reg_pack5_mv` DOUBLE, `reg_system_status13` BOOL, `reg_battery_status02` BOOL, `reg_temp_status04` BOOL, `reg_move_enable08` BOOL, `reg_move_pitch` DOUBLE, `reg_system_status03` BOOL, `reg_battery_status12` BOOL, `reg_system_status04` BOOL, `reg_temp_status03` BOOL, `reg_battery_status01` BOOL, `reg_pack4_mv` DOUBLE, `reg_move_enable09` BOOL, `reg_temp_status00` BOOL, `reg_move_enable10` BOOL, `reg_panel_mv` DOUBLE, `reg_move_enable13` BOOL, `reg_temp_status02` BOOL, `reg_system_status00` BOOL, `reg_system_status07` BOOL, `reg_roll` DOUBLE, `reg_battery_mv` DOUBLE, `reg_temp_status12` BOOL, `reg_battery_status10` BOOL, `reg_battery_status15` BOOL, `reg_temp_status07` BOOL, `reg_pack1_mv` DOUBLE, `reg_system_status09` BOOL, `reg_battery_status06` BOOL, `reg_move_enable00` BOOL, `reg_system_status12` BOOL, `reg_temp_therm2` DOUBLE, `reg_temp_status10` BOOL, `reg_motor_temp` DOUBLE, `reg_pack3_mv` DOUBLE, `reg_battery_negative_peak` DOUBLE, `reg_move_enable04` BOOL, `xbee_signal` DOUBLE, `reg_temp_status06` BOOL, `reg_battery_status09` BOOL, `reg_pack6_mv` DOUBLE, `reg_temp_status11` BOOL, `reg_move_enable01` BOOL, `reg_battery_status08` BOOL, `reg_move_enable05` BOOL, `reg_system_status10` BOOL, `reg_pack2_mv` DOUBLE, `reg_move_enable15` BOOL, `reg_firmware_rev` DOUBLE, `reg_battery_status13` BOOL, `reg_temp_therm1` DOUBLE, `reg_move_enable11` BOOL, `reg_temp_status14` BOOL, `reg_system_status06` BOOL, `reg_pitch` DOUBLE, `reg_move_enable03` BOOL, `reg_battery_status14` BOOL, `reg_system_status08` BOOL, `reg_battery_status05` BOOL, `reg_battery_status04` BOOL, `reg_battery_status03` BOOL, `reg_battery_status00` BOOL, `reg_battery_positive_peak` DOUBLE, `reg_system_status05` BOOL, `reg_battery_status11` BOOL, `reg_system_status01` BOOL, `reg_battery_ma` DOUBLE, `is_online` BOOL, `mode` VARCHAR(32), `reg_pack8_mv` DOUBLE, `reg_move_enable06` BOOL, `reg_temp_status09` BOOL, `reg_move_enable07` BOOL, `reg_temp_status01` BOOL, `reg_move_enable12` BOOL, `reg_system_status11` BOOL, `reg_battery_rested_mv` DOUBLE, `reg_motor_last_move_avg_ma` DOUBLE, `reg_battery_discharge_net` DOUBLE, `reg_panel_last_charge_mv` DOUBLE, `reg_serial_number` VARCHAR(4), `reg_motor_last_move_peak_ma` DOUBLE, `reg_panel_last_charge_ma` DOUBLE, `reg_day_seconds` DOUBLE, `reg_motor_last_move_min_mv` DOUBLE, `reg_motor_last_move_start_pitch` DOUBLE, `reg_motor_last_move_count` DOUBLE) TAGS (`site` NCHAR(8), `tracker` NCHAR(16), `zone` NCHAR(4));
```

1. 子表：

| 超级表名 | 子表数量 |
| --- | --- |
| windspeeds | **117** |
| snowdepths | **87** |
| trackers | **34311** |

#### 4.2.2 数据来源

生产环境(V3.3.6.28.202510230320)： 客户的实际业务数据；
测试环境(V3.3.8.4.202511180636)： 使用 taosx 工具实时同步生产环境的数据；

| 超级表名 | 每张子表的写入频率 | 备注 |
| --- | --- | --- |
| windspeeds | **30 秒** |  |
| snowdepths | **20 秒/40秒** | 20秒一条，40秒一条，循环 |
| trackers | **10 分钟** |  |

### 4.3 流计算任务

生产环境(V3.3.6.28.202510230320)： 客户实际部署了6个流计算任务
测试环境(V3.3.8.4.202511180636)： 我们部署了 10个流计算任务

|  |
|  |
| 生产环境(V3.3.6.28.202510230320) | 测试环境(V3.3.8.4.202511180636) |
| windspeeds_hourly | 是 | 是 | interval(1h) sliding(1h) |
| windspeeds_daily | 是 | 是 | interval(1d, 5h) sliding(1d) |
| snowdepths_hourly | 是 | 是 | interval(1h) sliding(1h) |
| snowdepths_daily | 是 | 是 | interval(1d, 5h) sliding(1d) |
| off_target_trackers | 是 | 是 | interval(15m) sliding(5m) |
| trackers_motor_current_state_window | 是 | 是 | state_window(cast(reg_motor_last_move_count as int)) |
| kpi_zones_test | 否 | 是 | interval(1h) sliding(1h) |
| kpi_db_test | 否 | 是 | interval(1h) sliding(1h) |
| kpi_sites_test | 否 | 是 | interval(1h) sliding(1h) |
| kpi_trackers_test | 否 | 是 | interval(1h) sliding(1h) |

#### 4.3.1 流计算任务A-windspeeds_hourly 

1. v3.3.6.28 建流语句
```sql {wrap}
create stream windspeeds_hourly fill_history 1 into prod.windspeeds_hourly as select _wend as window_hourly, site, id, max(speed) as windspeed_hourly_maximum from prod.windspeeds where ts >= '2025-05-07' partition by site, id interval(1h); 
```

1. v3.3.8.4 建流语句
```sql {wrap}
create stream if not exists windspeeds_hourly interval(1h) sliding(1h) from prod.windspeeds partition by site, id stream_options(watermark(1s) | fill_history('2025-05-07 00:00:00.000')) into windspeeds_hourly OUTPUT_SUBTABLE(CONCAT('windspeeds_hourly_', cast(site as varchar), cast(id as varchar))) tags(tag_site nchar(8) as site, tag_id nchar(8) as id) as select _twend as window_hourly, %%1 as site, %%2 as id, max(speed) as windspeed_hourly_maximum from prod.windspeeds where _c0 >= _twstart and _c0 < _twend and site=%%1 and id=%%2;
```

#### 4.3.2 流计算任务B-windspeeds_daily 

1. v3.3.6.28 建流语句
```sql {wrap}
create stream windspeeds_daily fill_history 1 into prod.windspeeds_daily as select _wend as window_daily, site, id, max(windspeed_hourly_maximum) as windspeed_daily_maximum from prod.windspeeds_hourly partition by site, id interval(1d, 5h);
```

1. v3.3.8.4 建流语句
```sql {wrap}
create stream if not exists windspeeds_daily interval(1d, 5h) sliding(1d) from windspeeds_hourly partition by tag_site, tag_id stream_options(watermark(1s) | fill_history) into windspeeds_daily OUTPUT_SUBTABLE(CONCAT('windspeeds_daily_', cast(tag_site as varchar), cast(tag_id as varchar))) tags(tag_site nchar(8) as tag_site, tag_id nchar(8) as tag_id) as select _twend as window_hourly, %%1 as site, %%2 as id, max(windspeed_hourly_maximum) as windspeed_daily_maximum from prod.windspeeds_hourly where _c0 >= _twstart and _c0 < _twend and site=%%1 and id=%%2;
```

#### 4.3.3 流计算任务C-snowdepths_hourly 

1. v3.3.6.28 建流语句
```sql {wrap}
create stream snowdepths_hourly fill_history 1 into prod.snowdepths_hourly as select _wend as window_hourly, site, id, max(depth) as snowdepth_hourly_maximum from prod.snowdepths where _ts >= '2024-01-01' partition by site, id interval(1h);
```

1. v3.3.8.4 建流语句
```sql {wrap}
create stream if not exists snowdepths_hourly interval(1h) sliding(1h) from prod.snowdepths partition by site, id stream_options(watermark(1s) | fill_history('2024-01-01')) into snowdepths_hourly OUTPUT_SUBTABLE(CONCAT('snowdepths_hourly_', cast(site as varchar), cast(id as varchar))) tags(tag_site nchar(8) as site, tag_id nchar(16) as id) as select _twend as window_hourly, %%1 as site, %%2 as id, max(depth) as snowdepth_hourly_maximum from prod.snowdepths where _c0 >= _twstart and _c0 < _twend and site=%%1 and id=%%2; 
```

#### 4.3.4 流计算任务D-snowdepths_daily 

1. v3.3.6.28 建流语句
```sql {wrap}
create stream snowdepths_daily fill_history 1 into prod.snowdepths_daily as select _wend as window_daily, site, id, max(snowdepth_hourly_maximum) as snowdepth_daily_maximum from prod.snowdepths_hourly partition by site, id interval(1d, 5h);
```

1. v3.3.8.4 建流语句
```sql {wrap}
create stream if not exists snowdepths_daily interval(1d, 5h) sliding(1d) from prod.snowdepths_hourly partition by tag_site, tag_id stream_options(watermark(1s) | fill_history) into snowdepths_daily OUTPUT_SUBTABLE(CONCAT('snowdepths_daily_', cast(tag_site as varchar), cast(tag_id as varchar))) tags(tag_site nchar(8) as tag_site, tag_id nchar(16) as tag_id) as select _twend as window_daily, %%1 as site, %%2 as id, max(snowdepth_hourly_maximum) as snowdepth_daily_maximum from prod.snowdepths_hourly where _c0 >= _twstart and _c0 < _twend and site=%%1 and id=%%2;   
```

#### 4.3.5 流计算任务E-off_target_trackers 

1. v3.3.6.28 建流语句
```sql {wrap}
create stream off_target_trackers ignore expired 0 ignore update 0 into prod.off_target_trackers as select _wend as _ts, site, tracker, last(reg_pitch) as off_target_pitch, last(mode) as mode from prod.trackers where _ts >= '2024-04-23' and _ts < now() + 1h and abs(reg_pitch-reg_move_pitch) > 2 partition by site, tracker interval(15m) sliding(5m);
```

1. v3.3.8.4 建流语句
```sql {wrap}
create stream if not exists off_target_trackers interval(15m) sliding(5m) from trackers partition by site, tracker stream_options(watermark(1s) | IGNORE_DISORDER) into off_target_trackers OUTPUT_SUBTABLE(CONCAT('off_target_trackers_', cast(site as varchar), cast(tracker as varchar))) tags(tag_site nchar(8) as site, tag_tracker nchar(16) as tracker) as select _twend as _ts, %%1 as site, %%2 as tracker, last(reg_pitch) as off_target_pitch, last(mode) as mode from prod.trackers where _c0 >= _twstart and _c0 < _twend and abs(reg_pitch-reg_move_pitch) > 2 and _c0 < cast((_tlocaltime/1000000 + 1h) as timestamp) and site=%%1 and tracker=%%2;
```

#### 4.3.6 流计算任务F-trackers_motor_current_state_window 

1. v3.3.6.28 建流语句
```sql {wrap}
create stream trackers_motor_current_state_window into prod.trackers_motor_current_state_window as select _ts, site, tracker, max(`reg_motor_last_move_peak_mA` / 1000) as max_motor_current from prod.trackers where _ts >= '2024-09-22' and _ts < now() + 1h and `reg_motor_last_move_peak_mA` > 0 partition by tbname/*, site, tracker */ state_window(cast(reg_motor_last_move_count as int));
```

1. v3.3.8.4 建流语句
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

1. v3.3.6.28 建流语句
```sql {wrap}
create stream kpi_db_test trigger window_close watermark 10m fill_history 1 ignore update 1 into kpi_db_test as select _wend as window_end, case when last(_ts) is not null then 1 else 0 end as db_online from trackers where _ts >= '2024-10-04T00:00:00.000Z' interval(1h) sliding(1h);
```

1. v3.3.8.4 建流语句
```sql {wrap}
create stream if not exists kpi_db_test interval(1h) sliding(1h) from prod.trackers stream_options(watermark(1s) | IGNORE_DISORDER | fill_history('2024-10-04T00:00:00.000Z')) into kpi_db_test as select _twend as window_end, case when last(_c0) is not null then 1 else 0 end as db_online from prod.trackers where _c0 >= _twstart and _c0 < _twend;
```

#### 4.3.8 流计算任务H-**kpi_trackers_test **

1. v3.3.6.28 建流语句
```sql {wrap}
create stream if not exists kpi_trackers_test trigger window_close watermark 10m fill_history 1 ignore update 1 into kpi_trackers_test as select _wend as window_end, site, zone, tracker, case when ((min(abs(reg_pitch - reg_move_pitch)) <= 2) or (min(reg_temp_therm2) < -10) or (max(reg_temp_therm2) > 60) or (last(reg_system_status14) = true)) then 1 else 0 end as tracker_on_target, case when last(reg_pitch) is not null then 1 else 0 end as tracker_online from trackers where _ts >= '2024-10-04T00:00:00.000Z' partition by tbname interval(1h) sliding(1h);
```

1. v3.3.8.4 建流语句
```sql {wrap}
create stream if not exists kpi_trackers_test interval(1h) sliding(1h) from prod.trackers partition by tbname stream_options(watermark(10m) | IGNORE_DISORDER | fill_history('2024-10-04T00:00:00.000Z')) into kpi_trackers_test OUTPUT_SUBTABLE(CONCAT('kpi_trackers_test_', tbname)) as select _twend as window_end, site, zone, tracker, case when ((min(abs(reg_pitch - reg_move_pitch)) <= 2) or (min(reg_temp_therm2) < -10) or (max(reg_temp_therm2) > 60) or (last(reg_system_status14) = true)) then 1 else 0 end as tracker_on_target, case when last(reg_pitch) is not null then 1 else 0 end as tracker_online from %%tbname where _c0 >= _twstart and _c0 < _twend;
```

#### 4.3.9 流计算任务I-**kpi_zones_test **

1. v3.3.6.28 建流语句
```sql {wrap}
create stream kpi_zones_test trigger window_close watermark 10m fill_history 1 ignore update 1 into kpi_zones_test as select _wend as window_end, site, zone, case when last(_ts) is not null then 1 else 0 end as zone_online from trackers where _ts >= '2024-10-04T10:00:00.000Z' partition by site, zone interval(1h) sliding(1h);
```

1. v3.3.8.4 建流语句
```sql {wrap}
create stream if not exists kpi_zones_test interval(1h) sliding(1h) from prod.trackers partition by site, zone stream_options(watermark(10m) | IGNORE_DISORDER | fill_history('2024-10-04T10:00:00.000Z')) into kpi_zones_test OUTPUT_SUBTABLE(CONCAT('kpi_zones_test_', cast(site as varchar),  cast(zone as varchar))) tags(tag_site nchar(8) as site, tag_zone nchar(4) as zone) as select _twend as window_end, %%1 as site, %%2 as zone, case when last(_c0) is not null then 1 else 0 end as zone_online from prod.trackers where _c0 >= _twstart and _c0 < _twend and site=%%1 and zone=%%2;
```

#### 4.3.10 流计算任务J-**kpi_sites_test **

1. v3.3.6.28 建流语句
```sql {wrap}
create stream kpi_sites_test trigger window_close watermark 10m fill_history 1 ignore update 1 into  kpi_sites_test as select _wend as window_end, site, case when last(_ts) is not null then 1 else 0 end as site_online from  trackers where _ts >= '2024-10-04T00:00:00.000Z' partition by site interval(1h) sliding(1h);
```

1. v3.3.8.4 建流语句
```sql {wrap}
create stream if not exists kpi_sites_test interval(1h) sliding(1h) from trackers partition by site stream_options(watermark(10m) | IGNORE_DISORDER | fill_history('2024-10-04T00:00:00.000Z')) into kpi_sites_test OUTPUT_SUBTABLE(CONCAT('kpi_sites_test_', cast(site as varchar))) tags(tag_site nchar(8) as site) as select _twend as window_end, %%1 as site, case when last(_c0) is not null then 1 else 0 end as site_online from prod.trackers where _c0 >= _twstart and _c0 < _twend and site=%%1;
```

### 4.4 测试流程

两个环境都已经在稳定运行中了，在此基础上，通过脚本采集一段时间（10个小时）中 taosd 进程的cpu 和内存占用的数据，然后计算出结果。

## 5. 测试结果

1. CPU 和内存的占用
| 观测指标 | V3.3.6.28.202510230320 | V3.3.8.4.202511180636 | 指标对比 |
| --- | --- | --- | --- |
| CPU 平均使用率（%） | 321.73 | 30.29 | (C2-B2)/B2 |
| CPU 使用率峰值（%） | 766.7 | 526.7 | (C3-B3)/B3 |
| CPU 使用值 p99（%） | 633.3 | 420 | (C4-B4)/B4 |
| CPU 使用值 p90（%） | 420 | 100 | (C5-B5)/B5 |
| 内存平均占用（GB） | 8.65 | 1.49 | (C6-B6)/B6 |
| 内存占用峰值（GB） | 10.34 | 1.55 | (C7-B7)/B7 |
| 内存占用p99（GB） | 9.75 | 1.55 | (C8-B8)/B8 |
| 内存占用p90（GB） | 9.24 | 1.53 | (C9-B9)/B9 |
| 内存占用p50（GB） | 8.7 | 1.49 | (C10-B10)/B10 |


|  |
|  |
|  |
|  | 数据源last_row(_c0) | 流结果last_row(_c0) | 备注 | 数据源last_row(_c0) | 流结果last_row(_c0) | 备注 |  |
| windspeeds_hourly | 2025-11-20 06:16:46.386 | 2025-11-20 05:00:00.000 | 延迟了1个计算窗口 | 2025-11-20 06:26:25.246 | 2025-11-20 06:00:00.000 | 正常 | interval(1h) sliding(1h) |
| windspeeds_daily | 2025-11-20 05:00:00.000 | 2025-11-19 05:00:00.000 | 延迟了1个计算窗口 | 2025-11-20 06:00:00.000 | 2025-11-20 05:00:00.000 | 正常 | interval(1d, 5h) sliding(1d) |
| snowdepths_hourly | 2025-11-20 06:23:25.094 | 2025-11-20 05:00:00.000 | 延迟了1个计算窗口 | 2025-11-20 06:28:24.068 | 2025-11-20 06:00:00.000 | 正常 | interval(1h) sliding(1h) |
| snowdepths_daily | 2025-11-20 05:00:00.000 | 2025-11-19 05:00:00.000 | 正常 | 2025-11-20 06:00:00.000 | 2025-11-20 05:00:00.000 | 正常 | interval(1d, 5h) sliding(1d) |
| 2025-11-20 06:30:44.805 | 2025-11-18 20:35:00.000 | 延迟太多窗口。 | 2025-11-20 06:29:37.895 | 2025-11-20 06:25:00.000 | 正常 |
| 2025-11-21 03:49:11.889 | 2025-11-19 20:55:00.000 | 延迟太多窗口。 | 2025-11-21 03:50:33.981 | 2025-11-21 03:50:00.000 | 正常 |
| trackers_motor_current_state_window | 2025-11-20 06:31:51.268 | 2025-11-19 14:55:58.079 | 无法估计延迟了多少个窗口 | 2025-11-20 06:31:52.639 | 2025-11-20 04:26:11.135 | 这里并不是延迟，而是用户数据在很长时间段内状态值保持不变，最后一个窗口没有关闭，所以没有新的计算结果。 | state_window(cast(reg_motor_last_move_count as int)) |
| kpi_zones_test | - | - | 没有部署 | 2025-11-20 06:34:27.275 | 2025-11-20 06:00:00.000 | 正常 | interval(1h) sliding(1h) |
| kpi_db_test | - | - | 没有部署 | 2025-11-20 06:34:27.275 | 2025-11-20 06:00:00.000 | 正常 | interval(1h) sliding(1h) |
| kpi_sites_test | - | - | 没有部署 | 2025-11-20 06:34:27.275 | 2025-11-20 06:00:00.000 | 正常 | interval(1h) sliding(1h) |
| kpi_trackers_test | - | - | 没有部署 | 2025-11-20 06:34:27.275 | 2025-11-20 06:00:00.000 | 正常 | interval(1h) sliding(1h) |

## 6. 纳入发版前流程

为确保流计算性能持续优化且避免迭代中出现回退，本对比测试流程将纳入发版前验证体系。具体实施步骤如下：
1. 环境准备与初始化​：执行 `environment_build.sh`脚本，自动构建测试环境。
2. 自动化测试执行​：执行 `run_performance_test.sh`脚本，自动加载测试数据，并执行预设的流计算任务与查询负载。
3. 结果校验与报告生成​：执行 `check_and_report.sh`脚本，自动采集性能数据，比对关键指标（如 CPU /内存消耗、计算延迟），并输出是否通过的结论性报告。
