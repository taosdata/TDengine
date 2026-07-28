---
title: 异常检测
sidebar_label: 异常检测
description: 时序数据异常检测模型
---

import ad from '../assets/anomaly-detection.png';

TDengine 中定义了异常（状态）窗口来提供异常检测服务。异常窗口可以视为一种特殊的**事件窗口（Event Window）**，即异常检测算法确定的连续异常时间序列数据所在的时间窗口。与普通事件窗口区别在于，时间窗口的起始时间和结束时间均由分析算法识别确定，不通过用户给定的表达式进行判定。因此，在查询的窗口子句中使用 `ANOMALY_WINDOW` 即可调用时序数据异常检测服务，同时窗口伪列（`_WSTART`、`_WEND`、`_WDURATION`）也能够像其他时间窗口一样用于描述异常窗口的起始时间（`_WSTART`）、结束时间（`_WEND`）、持续时间（`_WDURATION`）。例如：

```sql
-- 使用异常检测算法 IQR 对输入列 col_val 进行异常检测。同时输出异常窗口的起始时间、结束时间、以及异常窗口内 col 列的和。
SELECT _wstart, _wend, SUM(col)
FROM foo
ANOMALY_WINDOW(col_val, "algo=iqr");
```

如下图所示，Anode 将返回时序数据异常窗口 **[10:51:30, 10:53:40]**

<figure style={{textAlign: "center"}}>
<img src={ad} alt="异常检测"/>
</figure>

在此基础上，用户可以针对异常窗口内的时序数据进行查询聚合、变换处理等操作。

### 语法

```sql
ANOMALY_WINDOW(column_expr [, column_expr ...] [, option_expr])

option_expr: {"
algo=expr1
[,wncheck=1|0]
[,expr2]
"}
```

1. `column_expr`：进行时序数据异常检测的输入数据列。只能是数值类型，不能是字符类型（例如：`NCHAR`、`VARCHAR`、`VARBINARY` 等）；支持结果为数值类型的表达式，不支持 tag 和非数值结果。自 `v3.4.1.0` 起支持多列输入；对于只能处理单列的模型，只识别第一列，多余的列将自动忽略。
2. `option_expr`：字符串。其中使用 `K=V` 调用异常检测算法及与算法相关的参数。采用逗号分隔的 `K=V` 字符串表示，其中的字符串不需要使用单引号、双引号或转义号等符号，不能使用中文及其他宽字符。例如：`algo=ksigma,k=2` 表示进行异常检测的算法是 ksigma，该算法接受的输入参数是 2。
3. 异常检测的结果可以作为外层查询的子查询输入，在 `SELECT` 子句中使用的聚合函数或标量函数与其他类型的窗口查询相同。
4. 输入数据默认进行白噪声检查，如果输入数据是白噪声，将不会有任何（异常）窗口信息返回。

### 参数说明

| 参数 | 含义 | 默认值 |
| --- | --- | --- |
| `algo` | 异常检测调用的算法 | iqr |
| `wncheck` | 对输入数据列是否进行白噪声检查，取值为 0 或 1 | 1 |

### 示例

```sql
-- 使用 iqr 算法进行异常检测，检测列 i32 列。
SELECT _wstart, _wend, SUM(i32)
FROM foo
ANOMALY_WINDOW(i32, "algo=iqr");

-- 使用 ksigma 算法进行异常检测，输入参数 k 值为 2，检测列 i32 列
SELECT _wstart, _wend, SUM(i32)
FROM foo
ANOMALY_WINDOW(i32, "algo=ksigma,k=2");

taos> SELECT _wstart, _wend, count(*) FROM foo ANOMALY_WINDOW(i32);
         _wstart         |          _wend          |   count(*)    |
====================================================================
 2020-01-01 00:00:16.000 | 2020-01-01 00:00:17.000 |             2 |
Query OK, 1 row(s) in set (0.028946s)


-- 使用 ksigma 算法进行异常检测，输入参数 k 值为 2，检测列 i32 列；i8 列会被自动忽略。
-- 在 `v3.4.1.0` 之前的版本会报告错误，自 `v3.4.1.0` 起自动忽略 i8 列
SELECT _wstart, _wend, SUM(i32)
FROM foo
ANOMALY_WINDOW(i32, i8, "algo=ksigma,k=2");
```

### 内置异常检测算法

分析平台内置了若干异常检测模型，分为 3 个类别，分别是 [基于统计学的算法](./01-statistics-approach.md)、[基于数据密度的算法](./02-data-density.md)、以及 [基于机器学习的算法](./03-machine-learning.md)。在不指定异常检测使用的方法的情况下，默认调用 IQR 进行异常检测。可用算法以 `SHOW ANODES FULL` 实际返回为准，详见 [SHOW 命令](../../../05-tdengine-sql/09-system-info/03-show.md#show-anodes)。

### 异常检测算法有效性比较工具
