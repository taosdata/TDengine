---
title: 异常检测算法
description: 异常检测算法
---

import ad from '../pic/anomaly-detection.png';

时序数据异常检测，在TDengine 查询处理中以异常窗口的形式服务。因此，可以将异常检测获得的窗口视为一种特殊的**事件窗口**，区别在于异常窗口的触发条件和结束条件不是用户指定，而是检测算法自动识别。因此，可以应用在事件窗口上的函数均可应用在异常窗口中。由于异常检测结果是一个时间窗口，因此调用异常检测的方式也与使用事件窗口的方式相同，在 `WHERE` 子句中使用 `ANOMALY_WINDOW` 关键词即可调用时序数据异常检测服务，同时窗口伪列（`_WSTART`, `_WEND`, `_WDURATION`）也能够像其他窗口函数一样使用。例如：

```SQL
SELECT _wstart, _wend, SUM(i32) 
FROM foo
ANOMALY_WINDOW(i32, "algo=iqr");
```

如下图所示，Anode 将返回时序数据异常窗口 $[10:51:30, 10:53:40]$ 

<img src={ad} width="760" alt="异常检测" />

在此基础上，用户可以针对异常窗口内的时序数据进行查询聚合、变换处理等操作。

### 语法

```SQL
ANOMALY_WINDOW(column_name, option_expr)

option_expr: {"
algo=expr1
[,wncheck=1|0]
[,expr2]
"}
```

1. `column_name`：进行时序数据异常检测的输入数据列，当前只支持单列，且只能是数值类型，不能是字符类型（例如：`NCHAR` `VARCHAR` `VARBINARY`等类型），**不支持函数表达式**。
2. `options`：字符串。其中使用 K=V 调用异常检测算法及与算法相关的参数。采用逗号分隔的 K=V 字符串表示，其中的字符串不需要使用单引号、双引号、或转义号等符号，不能使用中文及其他宽字符。例如：`algo=ksigma,k=2` 表示进行异常检测的算法是 ksigma，该算法接受的输入参数是 2。
3. 异常检测的结果可以作为外层查询的子查询输入，在 `SELECT` 子句中使用的聚合函数或标量函数与其他类型的窗口查询相同。
4. 输入数据默认进行白噪声检查，如果输入数据是白噪声，将不会有任何（异常）窗口信息返回。

**参数说明**
|参数|含义|默认值|
|---|---|---|
|algo|异常检测调用的算法|iqr|
|wncheck|对输入数据列是否进行白噪声检查|取值为 0 或者 1，默认值为 1，表示进行白噪声检查|

异常检测的返回结果以窗口形式呈现，因此窗口查询相关的伪列在这种场景下仍然可用。可用的伪列如下：
1. `_WSTART`： 异常窗口开始时间戳
2. `_WEND`：异常窗口结束时间戳
3. `_WDURATION`：异常窗口持续时间

**示例**
```SQL
--- 使用 iqr 算法进行异常检测，检测列 i32 列。
SELECT _wstart, _wend, SUM(i32) 
FROM ai.atb
ANOMALY_WINDOW(i32, "algo=iqr");

--- 使用 ksigma 算法进行异常检测，输入参数 k 值为 2，检测列 i32 列
SELECT _wstart, _wend, SUM(i32) 
FROM ai.atb
ANOMALY_WINDOW(i32, "algo=ksigma,k=2");
```

```
taos> SELECT _wstart, _wend, count(*) FROM ai.atb ANOMAYL_WINDOW(i32);
         _wstart         |          _wend          |   count(*)    |
====================================================================
 2020-01-01 00:00:16.000 | 2020-01-01 00:00:17.000 |             2 |
Query OK, 1 row(s) in set (0.028946s)
```


**可用异常检测算法**
- iqr
- ksigma
- grubbs
- lof
- shesd
- tac
