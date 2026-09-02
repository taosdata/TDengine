---
title: "CES"
sidebar_label: "CES"
---

本节说明 CES 算法模型的使用方法。

## 功能概述

CES：Complex Exponential Smoothing，即复数指数平滑模型。CES 使用复数形式的状态空间方程描述时间序列，可以同时处理水平、趋势和季节性变化。TDgpt 使用自动化的 CES 模型，根据输入时间序列选择合适的模型并预测后续数据。

CES 支持计算预测结果的置信区间范围。

### 参数

| 参数   | 说明                                                                                     | 必填项 |
| ------ | ---------------------------------------------------------------------------------------- | ------ |
| period | 输入时间序列每个周期包含的数据点个数。如果不设置该参数或设置为 0，将使用非季节性模型预测 | 选填   |
| model  | CES 模型类型，可选 `N`、`S`、`P`、`F` 或 `Z`，默认值为 `Z`，表示自动选择模型             | 选填   |

`N` 表示非季节模型，`S` 表示简单季节模型，`P` 表示部分季节模型，`F` 表示完整季节模型，`Z` 表示由算法自动选择模型。

输入数据只能包含有限数值。非季节性预测至少需要两个输入值；当 `period` 大于 0 时，输入数据至少需要包含两个完整周期。

### 示例及结果

针对 i32 列进行数据预测，输入列 i32 每 12 个点是一个周期，使用自动选择的 CES 模型进行预测。

```sql
FORECAST(i32, "algo=ces,period=12,model=Z")
```

完整的调用 SQL 语句如下：

```sql
SELECT _frowts, FORECAST(i32, "algo=ces,period=12,model=Z") from foo
```

```json5
{
"rows": rows,       // 返回结果的行数
"period": period,   // 返回结果的周期性，同输入
"algo": "ces",     // 返回结果使用的算法
"mse": mse,         // 拟合输入时间序列生成模型的均方误差 (MSE)
"res": res          // 列模式的预测结果及置信区间
}
```

### 参考文献

1. [AutoCES Model - StatsForecast](https://nixtlaverse.nixtla.io/statsforecast/docs/models/autoces.html)
2. [Complex Exponential Smoothing](https://doi.org/10.1002/nav.22074)
