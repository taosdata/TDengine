---
sidebar_label: 数据写入
title: 数据写入与更新
description: 使用 SQL 快速体验时序数据的写入、更新和删除
toc_max_heading_level: 4
---

本章继续使用前一章的智能电表模型，在 shell 中快速体验时序数据的写入、更新和删除。你将看到如何一次写入一条或多条数据、一次写入多表，以及自动建表、更新和删除等常用操作。

## 前提条件

请先确认已经完成前一章的操作：

1. TDengine 服务已经启动，可以通过 shell 连接。
2. 已经了解 `power` 数据库、`meters` 超级表和 `d1001`、`d1002` 等子表的基本模型。

如果你还没有创建这些对象，可以直接在 shell 中执行下面的 SQL。

```sql
CREATE DATABASE IF NOT EXISTS power PRECISION 'ms' KEEP 3650 DURATION 10 BUFFER 16;

USE power;

CREATE STABLE IF NOT EXISTS meters (
    ts timestamp,
    current float,
    voltage int,
    phase float
) TAGS (
    location varchar(64),
    group_id int
);

CREATE TABLE IF NOT EXISTS d1001
USING meters TAGS ("California.SanFrancisco", 2);

CREATE TABLE IF NOT EXISTS d1002
USING meters TAGS ("California.SanFrancisco", 3);

CREATE TABLE IF NOT EXISTS d1003
USING meters TAGS ("California.LosAngeles", 2);

CREATE TABLE IF NOT EXISTS d1004
USING meters TAGS ("California.LosAngeles", 3);
```

## 写入

在 shell 中可以使用 `INSERT` 语句写入时序数据。

### 一次写入一条

执行下面的 SQL，向子表 `d1001` 写入一条数据：电流 10.3A，电压 219V，相位 0.31。

```sql
INSERT INTO d1001 (ts, current, voltage, phase) VALUES ("2018-10-03 14:38:05", 10.3, 219, 0.31);
```

如果 `VALUES` 中包含表的所有列，也可以省略字段列表，效果相同。

```sql
INSERT INTO d1001 VALUES ("2018-10-03 14:38:05", 10.3, 219, 0.31);
```

时间戳列也可以直接使用数据库精度的时间戳数值。

```sql
INSERT INTO d1001 VALUES (1538548685000, 10.3, 219, 0.31);
```

以上三种写法效果完全相同。

### 一次写入多条

假设 `d1001` 每 10 秒采集一次，每 30 秒上报一次，可以在一条 `INSERT` 语句中一次写入 3 条数据。

```sql
INSERT INTO d1001 VALUES
 ("2018-10-03 14:38:05", 10.2, 220, 0.23),
 ("2018-10-03 14:38:15", 12.6, 218, 0.33),
 ("2018-10-03 14:38:25", 12.3, 221, 0.31);
```

### 一次写入多表

也可以在一条语句中同时向 `d1001`、`d1002`、`d1003` 写入数据。下面的 SQL 一共写入 9 条记录。

```sql
INSERT INTO d1001 VALUES
    ("2018-10-03 14:38:05", 10.2, 220, 0.23),
    ("2018-10-03 14:38:15", 12.6, 218, 0.33),
    ("2018-10-03 14:38:25", 12.3, 221, 0.31)
d1002 VALUES
    ("2018-10-03 14:38:04", 10.2, 220, 0.23),
    ("2018-10-03 14:38:14", 10.3, 218, 0.25),
    ("2018-10-03 14:38:24", 10.1, 220, 0.22)
d1003 VALUES
    ("2018-10-03 14:38:06", 11.5, 221, 0.35),
    ("2018-10-03 14:38:16", 10.4, 220, 0.36),
    ("2018-10-03 14:38:26", 10.3, 220, 0.33);
```

### 指定列写入

只写入部分列时，未出现的列会自动填充为 `NULL`。时间戳列必须存在，且值不能为空。下面的 SQL 向 `d1004` 写入电压和相位，电流为 `NULL`。

```sql
INSERT INTO d1004 (ts, voltage, phase) VALUES ("2018-10-04 14:38:06", 223, 0.29);
```

### 写入时自动建表

使用带 `USING` 关键字的 `INSERT` 语句时，如果子表不存在，会先自动建表再写入；如果已存在，则直接写入。也可以只指定部分标签列，未指定的标签为 `NULL`。

```sql
INSERT INTO d1005
USING meters (location)
TAGS ("beijing.chaoyang")
VALUES ("2018-10-04 14:38:07", 10.15, 217, 0.33);
```

自动建表也支持一次向多张表写入。下面的 SQL 一共写入 9 条数据。

```sql
INSERT INTO d1001 USING meters TAGS ("California.SanFrancisco", 2) VALUES
    ("2018-10-03 14:38:05", 10.2, 220, 0.23),
    ("2018-10-03 14:38:15", 12.6, 218, 0.33),
    ("2018-10-03 14:38:25", 12.3, 221, 0.31)
d1002 USING meters TAGS ("California.SanFrancisco", 3) VALUES
    ("2018-10-03 14:38:04", 10.2, 220, 0.23),
    ("2018-10-03 14:38:14", 10.3, 218, 0.25),
    ("2018-10-03 14:38:24", 10.1, 220, 0.22)
d1003 USING meters TAGS ("California.LosAngeles", 2) VALUES
    ("2018-10-03 14:38:06", 11.5, 221, 0.35),
    ("2018-10-03 14:38:16", 10.4, 220, 0.36),
    ("2018-10-03 14:38:26", 10.3, 220, 0.33);
```

### 通过超级表写入

也可以直接向超级表写入。超级表本身不存数据，写入会落到对应的子表。下面的 SQL 通过 `tbname` 指定写入 `d1001`。

```sql
INSERT INTO meters (tbname, ts, current, voltage, phase, location, group_id)
VALUES ("d1001", "2018-10-03 14:38:05", 10.2, 220, 0.23, "California.SanFrancisco", 2);
```

### 通过虚拟表写入

注意：虚拟表和虚拟超级表是动态生成的，本身不存储数据，不支持写入。

### 零代码写入

除了在 shell 中手写 SQL，你还可以通过 Telegraf、Prometheus、EMQX、StatsD、collectd、HiveMQ 等第三方工具导入数据。TDengine TSDB Enterprise 还提供 MQTT、OPC、AVEVA PI System、Wonderware、Kafka、MySQL、Oracle 等连接器，配置后无须编写代码即可写入。快速体验请参见 [零代码数据写入](./10-no-code-ingestion.md)。

## 更新

写入相同时间戳的数据时，新值会替换旧值。下面的 SQL 把 `d1001` 在 `2018-10-03 14:38:05` 的电流更新为 `22`。

```sql
INSERT INTO d1001 (ts, current) VALUES ("2018-10-03 14:38:05", 22);
```

## 删除

可以根据时间戳删除异常数据。下面的 SQL 删除超级表 `meters` 中早于 `2021-10-01 10:40:00.100` 的数据。

```sql
DELETE FROM meters WHERE ts < '2021-10-01 10:40:00.100';
```

数据删除后不可恢复。建议先用 `SELECT` 加上相同的 `WHERE` 条件确认待删除内容，再执行 `DELETE`。

## 查看压缩率

写入完成后，可以查看数据库压缩率和磁盘占用。

```sql
SELECT * FROM INFORMATION_SCHEMA.INS_DISK_USAGE WHERE db_name = 'power';
```

也可以查看单表的压缩率分布。

```sql
SHOW TABLE DISTRIBUTED d1001;
```

更多磁盘占用和分布说明，请参见 [查看数据库的磁盘空间占用](../05-tdengine-sql/02-ddl/01-database.md#查看数据库的磁盘空间占用) 和 [SHOW TABLE DISTRIBUTED](../05-tdengine-sql/09-system-info/03-show.md#show-table-distributed)。

更多写入语法、删除规则和压缩配置，请继续阅读 [数据写入](../05-tdengine-sql/03-data-write/01-insert.md) 章节。
