---
title: "ARIMA"
sidebar_label: "ARIMA"
---

本节说明 ARIMA 算法模型的使用方法。

## 功能概述

ARIMA：Autoregressive Integrated Moving Average，即自回归移动平均模型，记作 ARIMA(p,d,q)，是统计模型中最常见的一种用来进行时间序列预测的模型。
ARIMA 模型是一种自回归模型，只需要自变量即可预测后续的值。ARIMA 模型要求时间序列**平稳**，或经过差分处理后平稳，如果是不平稳的数据，**无法**获得正确的结果。

> 平稳的时间序列：其性质不随观测时间的变化而变化。具有趋势或季节性的时间序列不是平稳时间序列——趋势和季节性使得时间序列在不同时段呈现不同性质。

以下参数可以动态输入，控制预测过程中生成合适的 ARIMA 模型。

- p= 自回归模型阶数
- d= 差分阶数
- q= 移动平均模型阶数

### 参数

分析平台中使用自动化的 ARIMA 模型进行计算，因此每次计算的时候会根据输入的数据自动拟合最合适的模型，然后根据该模型进行预测输出结果。

| 参数      | 说明                                                           | 必填项 |
| ------- | ------------------------------------------------------------ | --- |
| period  | 输入时间序列每个周期包含的数据点个数，如果不设置该参数或该参数设置为 0，将使用非季节性/周期性的 ARIMA 模型预测 | 选填  |
| start_p | 自回归模型阶数的起始值，0 开始的整数，不推荐大于 10                                 | 选填  |
| max_p   | 自回归模型阶数的结束值，0 开始的整数，不推荐大于 10                                 | 选填  |
| start_q | 移动平均模型阶数的起始值，0 开始的整数，不推荐大于 10                                | 选填  |
| max_q   | 移动平均模型阶数的结束值，0 开始的整数，不推荐大于 10                                | 选填  |
| d       | 差分阶数                                                         | 选填  |

`start_p`、`max_p` `start_q` `max_q` 四个参数约束了模型在多大的范围内去搜寻合适的最优解。相同输入数据的条件下，参数范围越大，消耗的资源越多，系统响应的时间越长。

### 示例及结果

针对 i32 列进行数据预测，输入列 i32 每 10 个点是一个周期；start_p 起始是 1，最大拟合是 5；start_q 是 1，最大值是 5，预测结果中返回 95% 置信区间范围边界。

```
FORECAST(i32, "algo=arima,alpha=95,period=10,start_p=1,max_p=5,start_q=1,max_q=5")
```

完整的调用 SQL 语句如下：

```SQL
SELECT _frowts, FORECAST(i32, "algo=arima,alpha=95,period=10,start_p=1,max_p=5,start_q=1,max_q=5") from foo
```

```json5
{
"rows": fc_rows,  // 返回结果的行数
"period": period, // 返回结果的周期性，同输入
"alpha": alpha,   // 返回结果的置信区间，同输入
"algo": "arima",  // 返回结果使用的算法
"mse": mse,       // 拟合输入时间序列时候生成模型的最小均方误差 (MSE)
"res": res        // 列模式的结果
}
```

### 参考文献

- https://en.wikipedia.org/wiki/Autoregressive_moving-average_model
- [https://baike.baidu.com/item/自回归滑动平均模型/5023931](https://baike.baidu.com/item/%E8%87%AA%E5%9B%9E%E5%BD%92%E6%BB%91%E5%8A%A8%E5%B9%B3%E5%9D%87%E6%A8%A1%E5%9E%8B/5023931)
