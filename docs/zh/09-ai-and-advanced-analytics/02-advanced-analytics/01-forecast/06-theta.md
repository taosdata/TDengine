---
title: "Theta"
sidebar_label: "Theta"
---

本节说明 Theta 算法模型的使用方法。

## 功能概述

Theta 方法将输入时间序列转换为具有不同曲率的 Theta 线，分别进行预测后再组合预测结果。该模型适用于包含趋势的单变量时间序列，也可以在预测前对季节性数据进行分解。

Theta 支持计算预测结果的置信区间范围。

### 参数

| 参数               | 说明                                                                                             | 必填项 |
| ------------------ | ------------------------------------------------------------------------------------------------ | ------ |
| period             | 输入时间序列每个周期包含的数据点个数。如果不设置该参数或设置为 0，将使用非季节性模型预测         | 选填   |
| decomposition_type | 季节分解类型，可选 `additive`（加法）或 `multiplicative`（乘法），默认值为 `multiplicative` | 选填   |

当季节变化保持相对稳定时，可以使用 `additive`；当季节变化随时间序列水平成比例变化时，可以使用 `multiplicative`。乘法分解要求输入数据为正数。

输入数据只能包含有限数值。非季节性预测至少需要两个输入值；当 `period` 大于 0 时，输入数据至少需要包含两个完整周期。

### 示例及结果

针对 i32 列进行数据预测，输入列 i32 每 12 个点是一个周期，季节性采用乘法分解。

```sql
FORECAST(i32, "algo=theta,period=12,decomposition_type=multiplicative")
```

完整的调用 SQL 语句如下：

```sql
SELECT _frowts, FORECAST(i32, "algo=theta,period=12,decomposition_type=multiplicative") from foo
```

```json5
{
"rows": rows,       // 返回结果的行数
"period": period,   // 返回结果的周期性，同输入
"algo": "theta",   // 返回结果使用的算法
"mse": mse,         // 拟合输入时间序列生成模型的均方误差 (MSE)
"res": res          // 列模式的预测结果及置信区间
}
```

### 参考文献

1. [Standard Theta Model - StatsForecast](https://nixtlaverse.nixtla.io/statsforecast/docs/models/standardtheta.html)
2. [The Theta model: a decomposition approach to forecasting](https://doi.org/10.1016/S0169-2070(00)00066-2)
