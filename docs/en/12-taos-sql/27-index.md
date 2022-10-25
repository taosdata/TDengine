---
sidebar_label: Index
title: Using Indices
---

TDengine supports SMA and FULLTEXT indexing.

## Create an Index

```sql
CREATE FULLTEXT INDEX index_name ON tb_name (col_name [, col_name] ...)

CREATE SMA INDEX index_name ON tb_name index_option

index_option:
    FUNCTION(functions) INTERVAL(interval_val [, interval_offset]) [SLIDING(sliding_val)] [WATERMARK(watermark_val)] [MAX_DELAY(max_delay_val)]

functions:
    function [, function] ...
```

### SMA Indexing

Performs pre-aggregation on the specified column over the time window defined by the INTERVAL clause. The type is specified in functions_string. SMA indexing improves aggregate query performance for the specified time period. One supertable can only contain one SMA index.

- The max, min, and sum functions are supported.
- WATERMARK: Enter a value between 0ms and 900000ms. The most precise unit supported is milliseconds. The default value is 5 seconds. This option can be used only on supertables.
- MAX_DELAY: Enter a value between 1ms and 900000ms. The most precise unit supported is milliseconds. The default value is the value of interval provided that it does not exceed 900000ms. This option can be used only on supertables. Note: Retain the default value if possible. Configuring a small MAX_DELAY may cause results to be frequently pushed, affecting storage and query performance.

### FULLTEXT Indexing

Creates a text index for the specified column. FULLTEXT indexing improves performance for queries with text filtering. The index_option syntax is not supported for FULLTEXT indexing. FULLTEXT indexing is supported for JSON tag columns only. Multiple columns cannot be indexed together. However, separate indices can be created for each column.

## Delete an Index

```sql
DROP INDEX index_name;
```

## View Indices

````sql
```sql
SHOW INDEXES FROM tbl_name [FROM db_name];
````

Shows indices that have been created for the specified database or table.
