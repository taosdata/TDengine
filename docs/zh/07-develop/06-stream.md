---
sidebar_label: 流式计算
description: "TDengine 流式计算将数据的写入、预处理、复杂分析、实时计算、报警触发等功能融为一体，是一个能够降低用户部署成本、存储成本和运维成本的计算引擎。"
title: 流式计算
---

在时序数据的处理中，经常要对原始数据进行清洗、预处理，再使用时序数据库进行长久的储存。用户通常需要在时序数据库之外再搭建 Kafka、Flink、Spark 等流计算处理引擎，增加了用户的开发成本和维护成本。
使用 TDengine 3.0 的流式计算引擎能够最大限度的减少对这些额外中间件的依赖，真正将数据的写入、预处理、长期存储、复杂分析、实时计算、实时报警触发等功能融为一体，并且，所有这些任务只需要使用 SQL 完成，极大降低了用户的学习成本、使用成本。

## 流式计算的创建

```sql
CREATE STREAM [IF NOT EXISTS] stream_name [stream_options] INTO stb_name AS subquery
stream_options: {
 TRIGGER    [AT_ONCE | WINDOW_CLOSE | MAX_DELAY time]
 WATERMARK   time
 IGNORE EXPIRED
}
```

详细的语法规则参考 [流式计算](../../taos-sql/stream)

## 示例一

企业电表的数据经常都是成百上千亿条的，那么想要将这些分散、凌乱的数据清洗或转换都需要比较长的时间，很难做到高效性和实时性，以下例子中，通过流计算可以将过去 12 小时电表电压大于 220V 的数据清洗掉，然后以小时为窗口整合并计算出每个窗口中电流的最大值，并将结果输出到指定的数据表中。

### 创建 DB 和原始数据表

首先准备数据，完成建库、建一张超级表和多张子表操作

```sql
drop database if exists stream_db;
create database stream_db;

create stable stream_db.meters (ts timestamp, current float, voltage int) TAGS (location varchar(64), groupId int);

create table stream_db.d1001 using stream_db.meters tags("beijing", 1);
create table stream_db.d1002 using stream_db.meters tags("guangzhou", 2);
create table stream_db.d1003 using stream_db.meters tags("shanghai", 3);
```

### 创建流

```sql
create stream stream1 into stream_db.stream1_output_stb as select _wstart as start, _wend as end, max(current) as max_current from stream_db.meters where voltage <= 220 and ts > now - 12h interval (1h);
```

### 写入数据
```sql
insert into stream_db.d1001 values(now-14h, 10.3, 210);
insert into stream_db.d1001 values(now-13h, 13.5, 216);
insert into stream_db.d1001 values(now-12h, 12.5, 219);
insert into stream_db.d1002 values(now-11h, 14.7, 221);
insert into stream_db.d1002 values(now-10h, 10.5, 218);
insert into stream_db.d1002 values(now-9h, 11.2, 220);
insert into stream_db.d1003 values(now-8h, 11.5, 217);
insert into stream_db.d1003 values(now-7h, 12.3, 227);
insert into stream_db.d1003 values(now-6h, 12.3, 215);
```

### 查询以观查结果
```sql
taos> select * from stream_db.stream1_output_stb;
          start          |           end           |     max_current      |       group_id        |
===================================================================================================
 2022-08-09 14:00:00.000 | 2022-08-09 15:00:00.000 |             10.50000 |                     0 |
 2022-08-09 15:00:00.000 | 2022-08-09 16:00:00.000 |             11.20000 |                     0 |
 2022-08-09 16:00:00.000 | 2022-08-09 17:00:00.000 |             11.50000 |                     0 |
 2022-08-09 18:00:00.000 | 2022-08-09 19:00:00.000 |             12.30000 |                     0 |
Query OK, 4 rows in database (0.012033s)
```

## 示例二
某运营商平台要采集机房所有服务器的系统资源指标，包含 cpu、内存、网络延迟等，采集后需要对数据进行四舍五入运算，将地域和服务器名以下划线拼接，然后将结果按时间排序并以服务器名分组输出到新的数据表中。

### 创建 DB 和原始数据表
首先准备数据，完成建库、建一张超级表和多张子表操作

```sql
drop database if exists stream_db;
create database stream_db;

create stable stream_db.idc (ts timestamp, cpu float, mem float, latency float) TAGS (location varchar(64), groupId int);

create table stream_db.server01 using stream_db.idc tags("beijing", 1);
create table stream_db.server02 using stream_db.idc tags("shanghai", 2);
create table stream_db.server03 using stream_db.idc tags("beijing", 2);
create table stream_db.server04 using stream_db.idc tags("tianjin", 3);
create table stream_db.server05 using stream_db.idc tags("shanghai", 1);
```

### 创建流

```sql
create stream stream2 into stream_db.stream2_output_stb as select ts, concat_ws("_", location, tbname) as server_location, round(cpu) as cpu, round(mem) as mem, round(latency) as latency from stream_db.idc partition by tbname order by ts;
```

### 写入数据
```sql
insert into stream_db.server01 values(now-14h, 50.9, 654.8, 23.11);
insert into stream_db.server01 values(now-13h, 13.5, 221.2, 11.22);
insert into stream_db.server02 values(now-12h, 154.7, 218.3, 22.33);
insert into stream_db.server02 values(now-11h, 120.5, 111.5, 5.55);
insert into stream_db.server03 values(now-10h, 101.5, 125.6, 5.99);
insert into stream_db.server03 values(now-9h, 12.3, 165.6, 6.02);
insert into stream_db.server04 values(now-8h, 160.9, 120.7, 43.51);
insert into stream_db.server04 values(now-7h, 240.9, 520.7, 54.55);
insert into stream_db.server05 values(now-6h, 190.9, 320.7, 55.43);
insert into stream_db.server05 values(now-5h, 110.9, 600.7, 35.54);
```
### 查询以观查结果
```sql
taos> select ts, server_location, cpu, mem, latency from stream_db.stream2_output_stb;
           ts            |        server_location         |         cpu          |         mem          |       latency        |
================================================================================================================================
 2022-08-09 21:24:56.785 | beijing_server01               |             51.00000 |            655.00000 |             23.00000 |
 2022-08-09 22:24:56.795 | beijing_server01               |             14.00000 |            221.00000 |             11.00000 |
 2022-08-09 23:24:56.806 | shanghai_server02              |            155.00000 |            218.00000 |             22.00000 |
 2022-08-10 00:24:56.815 | shanghai_server02              |            121.00000 |            112.00000 |              6.00000 |
 2022-08-10 01:24:56.826 | beijing_server03               |            102.00000 |            126.00000 |              6.00000 |
 2022-08-10 02:24:56.838 | beijing_server03               |             12.00000 |            166.00000 |              6.00000 |
 2022-08-10 03:24:56.846 | tianjin_server04               |            161.00000 |            121.00000 |             44.00000 |
 2022-08-10 04:24:56.853 | tianjin_server04               |            241.00000 |            521.00000 |             55.00000 |
 2022-08-10 05:24:56.866 | shanghai_server05              |            191.00000 |            321.00000 |             55.00000 |
 2022-08-10 06:24:57.301 | shanghai_server05              |            111.00000 |            601.00000 |             36.00000 |
Query OK, 10 rows in database (0.022950s)
```

