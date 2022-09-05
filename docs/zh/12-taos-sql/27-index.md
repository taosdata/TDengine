---
sidebar_label: 索引
title: 索引
description: 索引功能的使用细节
---

TDengine 从 3.0.0.0 版本开始引入了索引功能，支持 SMA 索引和 FULLTEXT 索引。

## 创建索引

```sql
CREATE FULLTEXT INDEX index_name ON tb_name (col_name [, col_name] ...)

CREATE SMA INDEX index_name ON tb_name index_option

index_option:
    FUNCTION(functions) INTERVAL(interval_val [, interval_offset]) [SLIDING(sliding_val)] [WATERMARK(watermark_val)] [MAX_DELAY(max_delay_val)]

functions:
    function [, function] ...
```

### SMA 索引

对指定列按 INTERVAL 子句定义的时间窗口创建进行预聚合计算，预聚合计算类型由 functions_string 指定。SMA 索引能提升指定时间段的聚合查询的性能。目前，限制一个超级表只能创建一个 SMA INDEX。

- 支持的函数包括 MAX、MIN 和 SUM。
- WATERMARK: 最小单位毫秒，取值范围 [0ms, 900000ms]，默认值为 5 秒，只可用于超级表。
- MAX_DELAY: 最小单位毫秒，取值范围 [1ms, 900000ms]，默认值为 interval 的值(但不能超过最大值)，只可用于超级表。注：不建议 MAX_DELAY 设置太小，否则会过于频繁的推送结果，影响存储和查询性能，如无特殊需求，取默认值即可。

### FULLTEXT 索引

对指定列建立文本索引，可以提升含有文本过滤的查询的性能。FULLTEXT 索引不支持 index_option 语法。现阶段只支持对 JSON 类型的标签列创建 FULLTEXT 索引。不支持多列联合索引，但可以为每个列分布创建 FULLTEXT 索引。

## 删除索引

```sql
DROP INDEX index_name;
```

## 查看索引

````sql
```sql
SHOW INDEXES FROM tbl_name [FROM db_name];
````

显示在所指定的数据库或表上已创建的索引。
