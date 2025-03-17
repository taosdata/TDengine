---
title: 异常检测算法
description: 异常检测算法
---

import ad from '../pic/anomaly-detection.png';
import ad_result from '../pic/ad-result.png';
import ad_result_figure from '../pic/ad-result-figure.png';

TDengine 中定义了异常（状态）窗口来提供异常检测服务。异常窗口可以视为一种特殊的**事件窗口（Event Window）**，即异常检测算法确定的连续异常时间序列数据所在的时间窗口。与普通事件窗口区别在于——时间窗口的起始时间和结束时间均是分析算法识别确定，不是用户给定的表达式进行判定。因此，在 `WHERE` 子句中使用 `ANOMALY_WINDOW` 关键词即可调用时序数据异常检测服务，同时窗口伪列（`_WSTART`, `_WEND`, `_WDURATION`）也能够像其他时间窗口一样用于描述异常窗口的起始时间(`_WSTART`)、结束时间(`_WEND`)、持续时间(`_WDURATION`)。例如：

```SQL
--- 使用异常检测算法 IQR 对输入列 col_val 进行异常检测。同时输出异常窗口的起始时间、结束时间、以及异常窗口内 col 列的和。
SELECT _wstart, _wend, SUM(col) 
FROM foo
ANOMALY_WINDOW(col_val, "algo=iqr");
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

### 参数说明
|参数|含义|默认值|
|---|---|---|
|algo|异常检测调用的算法|iqr|
|wncheck|对输入数据列是否进行白噪声检查，取值为0或1|1|


### 示例
```SQL
--- 使用 iqr 算法进行异常检测，检测列 i32 列。
SELECT _wstart, _wend, SUM(i32) 
FROM foo
ANOMALY_WINDOW(i32, "algo=iqr");

--- 使用 ksigma 算法进行异常检测，输入参数 k 值为 2，检测列 i32 列
SELECT _wstart, _wend, SUM(i32) 
FROM foo
ANOMALY_WINDOW(i32, "algo=ksigma,k=2");

taos> SELECT _wstart, _wend, count(*) FROM foo ANOMAYL_WINDOW(i32);
         _wstart         |          _wend          |   count(*)    |
====================================================================
 2020-01-01 00:00:16.000 | 2020-01-01 00:00:17.000 |             2 |
Query OK, 1 row(s) in set (0.028946s)
```


### 内置异常检测算法
分析平台内置了6个异常检查模型，分为3个类别，分别是[基于统计学的算法](./02-statistics-approach.md)、[基于数据密度的算法](./03-data-density.md)、以及[基于机器学习的算法](./04-machine-learning.md)。在不指定异常检测使用的方法的情况下，默认调用 IQR 进行异常检测。

### 异常检测算法有效性比较工具
TDgpt 提供自动化的工具对比不同数据集的不同算法监测有效性，针对异常检测算法提供查全率（recall）和查准率（precision）两个指标衡量不同算法的有效性。
通过在配置文件中(analysis.ini)设置以下的选项可以调用需要使用的异常检测算法，异常检测算法测试用数据的时间范围、是否生成标注结果的图片、调用的异常检测算法以及相应的参数。
调用异常检测算法比较之前，需要人工手动标注异常监测数据集的结果，即设置[anno_res]选项的数值，第几个数值是异常点，需要标注在数组中，如下测试集中，第 9 个点是异常点，我们就标注异常结果为 [9].

```bash
[ad]
# training data start time
start_time = 2021-01-01T01:01:01

# training data end time
end_time = 2021-01-01T01:01:11

# draw the results or not
gen_figure = true

# annotate the anomaly_detection result
anno_res = [9]

# algorithms list that is involved in the comparion
[ad.algos]
ksigma={"k": 2}
iqr={}
grubbs={}
lof={"algo":"auto", "n_neighbor": 3}
```

对比程序执行完成以后，会自动生成名称为`ad_result.xlsx` 的文件，第一个卡片是算法运行结果（如下图所示），分别包含了算法名称、执行调用参数、查全率、查准率、执行时间 5 个指标。

<img src={ad_result} width="760" alt="异常检测对比结果" />

如果设置了 `gen_figure` 为 `true`，比较程序会自动将每个参与比较的算法分析结果采用图片方式呈现出来（如下图所示为 ksigma 的异常检测结果标注）。

<img src={ad_result_figure} width="760" alt="异常检测标注图" />

