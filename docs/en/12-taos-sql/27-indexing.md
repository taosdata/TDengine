---
title: Indexing
sidebar_label: Indexing
description: This document describes the SQL statements related to indexing in TDengine.
---

TDengine supports SMA and tag indexing.

## Create an Index

```sql
CREATE INDEX index_name ON tb_name (col_name [, col_name] ...)

CREATE SMA INDEX index_name ON tb_name index_option

index_option:
    FUNCTION(functions) INTERVAL(interval_val [, interval_offset]) [SLIDING(sliding_val)] [WATERMARK(watermark_val)] [MAX_DELAY(max_delay_val)]

functions:
    function [, function] ...
```
### tag Indexing

  [tag index](../tag-index)

### SMA Indexing

Performs pre-aggregation on the specified column over the time window defined by the INTERVAL clause. The type is specified in functions_string. SMA indexing improves aggregate query performance for the specified time period. One supertable can only contain one SMA index.

- The max, min, and sum functions are supported.
- WATERMARK: Enter a value between 0ms and 900000ms. The most precise unit supported is milliseconds. The default value is 5 seconds. This option can be used only on supertables.
- MAX_DELAY: Enter a value between 1ms and 900000ms. The most precise unit supported is milliseconds. The default value is the value of interval provided that it does not exceed 900000ms. This option can be used only on supertables. Note: Retain the default value if possible. Configuring a small MAX_DELAY may cause results to be frequently pushed, affecting storage and query performance.

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
-- query from SMA Index
ALTER LOCAL 'querySmaOptimize' '1';
SELECT max(c2),min(c1) FROM st1 INTERVAL(5m,10s) SLIDING(5m);
SELECT _wstart,_wend,_wduration,max(c2),min(c1) FROM st1 INTERVAL(5m,10s) SLIDING(5m);
-- query from raw data
ALTER LOCAL 'querySmaOptimize' '0';
```

## Delete an Index

```sql
DROP INDEX index_name;
```

## View Indices

````sql
SHOW INDEXES FROM tbl_name [FROM db_name];
SHOW INDEXES FROM [db_name.]tbl_name ;
````

Shows indices that have been created for the specified database or table.
