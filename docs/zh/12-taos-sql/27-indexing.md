---
sidebar_label: 索引
title: 索引
description: 索引功能的使用细节
---

TDengine 从 3.0.0.0 版本开始引入了索引功能，支持 SMA 索引和 tag 索引。

## 创建索引

```sql

CREATE INDEX index_name ON tb_name index_option

CREATE SMA INDEX index_name ON tb_name index_option

index_option:
    FUNCTION(functions) INTERVAL(interval_val [, interval_offset]) [SLIDING(sliding_val)] [WATERMARK(watermark_val)] [MAX_DELAY(max_delay_val)]

functions:
    function [, function] ...
```
### tag 索引

 [tag 索引](../tag-index)

### SMA 索引

对指定列按 INTERVAL 子句定义的时间窗口创建进行预聚合计算，预聚合计算类型由 functions_string 指定。SMA 索引能提升指定时间段的聚合查询的性能。目前，限制一个超级表只能创建一个 SMA INDEX。

- 支持的函数包括 MAX、MIN 和 SUM。
- WATERMARK: 最小单位毫秒，取值范围 [0ms, 900000ms]，默认值为 5 秒，只可用于超级表。
- MAX_DELAY: 最小单位毫秒，取值范围 [1ms, 900000ms]，默认值为 interval 的值(但不能超过最大值)，只可用于超级表。注：不建议 MAX_DELAY 设置太小，否则会过于频繁的推送结果，影响存储和查询性能，如无特殊需求，取默认值即可。

```sql
DROP DATABASE IF EXISTS d0;
CREATE DATABASE d0;
USE d0;
CREATE TABLE IF NOT EXISTS st1 (ts timestamp, c1 int, c2 float, c3 double) TAGS (t1 int unsigned);
CREATE TABLE ct1 USING st1 TAGS(1000);
CREATE TABLE ct2 USING st1 TAGS(2000);
INSERT INTO ct1 VALUES(now+0s, 10, 2.0, 3.0);
INSERT INTO ct1 VALUES(now+1s, 11, 2.1, 3.1)(now+2s, 12, 2.2, 3.2)(now+3s, 13, 2.3, 3.3);
CREATE SMA INDEX sma_index_name1 ON st1 FUNCTION(max(c1),max(c2),min(c1)) INTERVAL(5m,10s) SLIDING(5m) WATERMARK 5s MAX_DELAY 1m;
-- 从 SMA 索引查询
ALTER LOCAL 'querySmaOptimize' '1';
SELECT max(c2),min(c1) FROM st1 INTERVAL(5m,10s) SLIDING(5m);
SELECT _wstart,_wend,_wduration,max(c2),min(c1) FROM st1 INTERVAL(5m,10s) SLIDING(5m);
-- 从原始数据查询
ALTER LOCAL 'querySmaOptimize' '0'; 
```

## 删除索引

```sql
DROP INDEX index_name;
```

## 查看索引

````sql
SHOW INDEXES FROM tbl_name [FROM db_name];
SHOW INDEXES FROM [db_name.]tbl_name;
````

显示在所指定的数据库或表上已创建的索引。
