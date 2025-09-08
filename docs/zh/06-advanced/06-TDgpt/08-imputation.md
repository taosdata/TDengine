---
title: "缺失数据补值"
sidebar_label: "缺失数据补值"
description: 缺失数据补值
---

TDengine TSDB 提供了新的数据自动补齐的功能。针对缺失的时序数据点位，能够自动缺失的数据。
要求时序数据必须是严格的等间隔，一般意义上由于设备上报的数据并不能完全确保严格等距，建议针对基础数据进行窗口聚合和时间戳列
进行时间对齐在进行补值查询操作。

补值操作要求。
例如：

```SQL
--- 使用自动不全函数针对缺失的窗口聚集函数结果进行补齐。
SELECT imputation(col_val)
FROM (SELECT _wstart, min(val) col_val FROM foo INTERVAL(1s))

---- 以下是针对对齐数据时间戳以后对基础缺失数据进行补齐
SELECT imputation(col_val)
FROM (SELECT timetruncate(val) from )
```


### 语法

```SQL
IMPUTATION(column_name, option_expr)

option_expr: {"
algo=expr1
[,wncheck=1|0]
[,expr2]
"}
```

1. `column_name`：进行时序数据补齐的输入数据列，当前只支持单列，且只能是数值类型，不能是字符类型（例如：`NCHAR`、`VARCHAR`、`VARBINARY`等类型）。
2. `options`：字符串。其中使用 K=V 调用异常检测算法及与算法相关的参数。采用逗号分隔的 K=V 字符串表示，其中的字符串不需要使用单引号、双引号、或转义号等符号，不能使用中文及其他宽字符。例如：`algo=moment` 表示进行补齐操作调用的时序模型是`moment`，无其他参数输入。
3. 自动补齐的结果可以作为外层查询的子查询输入，在 `SELECT` 子句中使用的聚合函数或标量函数与其他类型的窗口查询相同。
4. 输入数据默认进行白噪声检查，如果输入数据是白噪声，将返回错误。

### 参数说明

| 参数      | 含义                     | 默认值 |
| ------- | ---------------------- | --- |
| algo    | 自动补齐调用的算法              | moment |
| wncheck | 对输入数据列是否进行白噪声检查，取值为 0 或 1 | 1   |

1. 当前只支持使用 `moment` 时序模型提供自动补齐服务。
   
### 示例

```SQL
--- 使用 moment 时序模型提供自动补齐服务，针对 i32 列进行补齐操作
SELECT imputation(i32)
FROM foo;

```