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
DROP DATABASE IF EXISTS power;
CREATE DATABASE power;
USE power;

CREATE STABLE meters (ts timestamp, current float, voltage int, phase float) TAGS (location binary(64), groupId int);

CREATE TABLE d1001 USING meters TAGS ("California.SanFrancisco", 2);
CREATE TABLE d1002 USING meters TAGS ("California.SanFrancisco", 3);
CREATE TABLE d1003 USING meters TAGS ("California.LosAngeles", 2);
CREATE TABLE d1004 USING meters TAGS ("California.LosAngeles", 3);
```

### 创建流

```sql
create stream current_stream into current_stream_output_stb as select _wstart as start, _wend as end, max(current) as max_current from meters where voltage <= 220 and ts > now - 12h interval (1h);
```

### 写入数据
```sql
insert into d1001 values(now-13h, 10.30000, 219, 0.31000);
insert into d1001 values(now-11h, 12.60000, 218, 0.33000);
insert into d1001 values(now-10h, 12.30000, 221, 0.31000);
insert into d1002 values(now-9h, 10.30000, 218, 0.25000);
insert into d1003 values(now-8h, 11.80000, 221, 0.28000);
insert into d1003 values(now-7h, 13.40000, 223, 0.29000);
insert into d1004 values(now-6h, 10.80000, 223, 0.29000);
insert into d1004 values(now-5h, 11.50000, 221, 0.35000);
```

### 查询以观查结果

```sql
taos> select start, end, max_current from current_stream_output_stb;
          start          |           end           |     max_current      |
===========================================================================
 2022-08-12 04:00:00.000 | 2022-08-12 05:00:00.000 |             12.60000 |
 2022-08-12 06:00:00.000 | 2022-08-12 07:00:00.000 |             10.30000 |
Query OK, 2 rows in database (0.009580s)
```

## 示例二

依然以示例一中的数据为基础，我们已经采集到了每个智能电表的电流和电压数据，现在要求出功率，并将地域和电表名以符号 "." 拼接，然后以电表名称分组输出到新的数据表中。

### 创建 DB 和原始数据表

参考示例一 [创建 DB 和原始数据表](#创建-db-和原始数据表)

### 创建流

```sql
create stream power_stream into power_stream_output_stb as select ts, concat_ws(".", location, tbname) as meter_location, current*voltage as meter_power from meters partition by tbname;
```

### 写入数据

参考示例一 [写入数据](#写入数据)

### 查询以观查结果
```sql
taos> select ts, meter_location, meter_power from power_stream_output_stb;
           ts            |         meter_location         |        meter_power        |
=======================================================================================
 2022-08-12 07:44:47.817 | California.SanFrancisco.d1002  |            2245.400041580 |
 2022-08-12 08:44:47.826 | California.LosAngeles.d1003    |            2607.800042152 |
 2022-08-12 09:44:47.833 | California.LosAngeles.d1003    |            2988.199914932 |
 2022-08-12 03:44:47.791 | California.SanFrancisco.d1001  |            2255.700041771 |
 2022-08-12 05:44:47.800 | California.SanFrancisco.d1001  |            2746.800083160 |
 2022-08-12 06:44:47.809 | California.SanFrancisco.d1001  |            2718.300042152 |
 2022-08-12 10:44:47.840 | California.LosAngeles.d1004    |            2408.400042534 |
 2022-08-12 11:44:48.379 | California.LosAngeles.d1004    |            2541.500000000 |
Query OK, 8 rows in database (0.014788s)
```