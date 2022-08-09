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
查询所有电表中电压等于 220V 的数据，对过滤出的电表电流数据进行四舍五入运算，同时将主键时间戳列转换为 bigint 类型，并对采集的数据按表名分组。

### 创建 DB 和原始数据表
首先准备数据，完成建库、建一张超级表和多张子表操作

```sql
drop database if exists stream_db;
create database stream_db;

create stable stream_db.meters (ts timestamp, current float, voltage int) TAGS (location varchar(64), groupId int);

create table stream_db.d1001 using stream_db.meters tags("beijing", 1);
create table stream_db.d1002 using stream_db.meters tags("shanghai", 2);
create table stream_db.d1003 using stream_db.meters tags("beijing", 2);
create table stream_db.d1004 using stream_db.meters tags("tianjin", 3);
create table stream_db.d1005 using stream_db.meters tags("shanghai", 1);
```

### 创建流

```sql
create stream stream2 into stream_db.stream2_output_stb as select ts,cast(ts as bigint),round(current),location from stream_db.meters where voltage=220 partition by tbname;
```

### 写入数据
```sql
insert into stream_db.d1001 values(now-14h, 10.3, 210);
insert into stream_db.d1001 values(now-13h, 13.5, 220);
insert into stream_db.d1002 values(now-12h, 14.7, 218);
insert into stream_db.d1002 values(now-11h, 10.5, 220);
insert into stream_db.d1003 values(now-10h, 11.5, 220);
insert into stream_db.d1003 values(now-9h, 12.3, 215);
insert into stream_db.d1004 values(now-8h, 11.5, 220);
insert into stream_db.d1004 values(now-7h, 15.3, 217);
insert into stream_db.d1005 values(now-6h, 16.5, 216);
insert into stream_db.d1005 values(now-5h, 12.3, 220);
```
### 查询以观查结果
```sql
taos> select * from stream_db.stream2_output_stb;
           ts            |  cast(ts as bigint)   |    round(current)    |            location            |       group_id        |
==================================================================================================================================
 2022-08-08 09:29:55.557 |         1659922195557 |             12.00000 | beijing                        |   7226905450883977166 |
 2022-08-08 11:29:55.570 |         1659929395570 |             12.00000 | tianjin                        |   7226905450884501455 |
 2022-08-08 08:29:55.549 |         1659918595549 |             11.00000 | shanghai                       |   7226905450883452877 |
 2022-08-08 06:29:55.534 |         1659911395534 |             14.00000 | beijing                        |   7226905450882928588 |
 2022-08-08 14:29:56.175 |         1659940196175 |             12.00000 | shanghai                       |   7226905450895708112 |
Query OK, 5 rows in database (0.015235s)
```

## 示例三
...待续

