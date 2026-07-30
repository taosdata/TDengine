---
title: "ETS"
sidebar_label: "ETS"
---

本节说明 ETS 算法模型的使用方法。

## 功能概述

ETS：Error、Trend、Seasonal，即误差、趋势和季节模型。ETS 使用状态空间模型分别描述时间序列中的误差、趋势和季节成分。TDgpt 使用自动化的 ETS 模型，根据输入参数和时间序列选择合适的成分组合并预测后续数据。

ETS 支持计算预测结果的置信区间范围。

### 参数

| 参数   | 说明                                                                                                                  | 必填项 |
| ------ | --------------------------------------------------------------------------------------------------------------------- | ------ |
| period | 输入时间序列每个周期包含的数据点个数。如果不设置该参数或设置为 0，将使用非季节性模型预测                              | 选填   |
| model  | 三个字符组成的 ETS 模型。三个字符依次表示误差、趋势和季节成分，默认值为 `ZZZ`，表示自动选择各个成分                   | 选填   |
| damped | 是否使用阻尼趋势，设置为 `1` 表示使用，设置为 `0` 表示不使用；不设置时由算法根据模型自动处理                           | 选填   |

模型字符中，`A` 表示加法成分，`M` 表示乘法成分，`N` 表示不包含该成分，`Z` 表示由算法自动选择。例如，`ANN` 表示加法误差、不包含趋势和季节成分。

输入数据只能包含有限数值。非季节性预测至少需要两个输入值；当 `period` 大于 0 时，输入数据至少需要包含两个完整周期。使用乘法成分时，输入数据必须为正数。

### 示例及结果

针对 i32 列进行数据预测，输入列 i32 每 12 个点是一个周期，由 ETS 自动选择误差、趋势和季节成分，并启用阻尼趋势。

```sql
FORECAST(i32, "algo=ets,period=12,model=ZZZ,damped=1")
```

完整的调用 SQL 语句如下：

```sql
SELECT _frowts, FORECAST(i32, "algo=ets,period=12,model=ZZZ,damped=1") from foo
```

```json5
{
"rows": rows,       // 返回结果的行数
"period": period,   // 返回结果的周期性，同输入
"algo": "ets",     // 返回结果使用的算法
"mse": mse,         // 拟合输入时间序列生成模型的均方误差 (MSE)
"res": res          // 列模式的预测结果及置信区间
}
```

### 参考文献

1. [AutoETS - StatsForecast](https://nixtlaverse.nixtla.io/statsforecast/src/core/models.html#autoets)
2. [Forecasting with Exponential Smoothing: The State Space Approach](https://robjhyndman.com/expsmooth/)
