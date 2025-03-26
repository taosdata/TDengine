---
title: "MLP"
sidebar_label: "MLP"
---

本节说明 MLP 模型的使用方法。

## 功能概述

MLP（MutiLayers Perceptron，多层感知机）是一种典的神经网络模型，能够通过学习历史数据的非线性关系，
捕捉时间序列中的模式并进行未来值预测。它通过多层全连接网络进行特征提取和映射，
对输入的历史数据生成预测结果。由于不直接考虑趋势或季节性变化，通常需要结合数据预处理来提升效果，
适合解决非线性和复杂的时间序列问题。

完整的调用SQL语句如下：

```SQL
SELECT _frowts, FORECAST(i32, "algo=mlp") from foo
```

```json5
{
"rows": fc_rows,  // 返回结果的行数
"period": period, // 返回结果的周期性，同输入
"alpha": alpha,   // 返回结果的置信区间，同输入
"algo": "mlp",    // 返回结果使用的算法
"mse": mse,       // 拟合输入时间序列时候生成模型的最小均方误差(MSE)
"res": res        // 列模式的结果
}
```

### 参考文献
- [1]Rumelhart D E, Hinton G E, Williams R J. Learning representations by back-propagating errors[J]. nature, 1986, 323(6088): 533-536.
- [2]Rosenblatt F. The perceptron: a probabilistic model for information storage and organization in the brain[J]. Psychological review, 1958, 65(6): 386.
- [3]LeCun Y, Bottou L, Bengio Y, et al. Gradient-based learning applied to document recognition[J]. Proceedings of the IEEE, 1998, 86(11): 2278-2324.