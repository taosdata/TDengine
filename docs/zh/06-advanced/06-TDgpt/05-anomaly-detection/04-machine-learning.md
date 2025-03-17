---
title: "机器学习算法"
sidebar_label: "机器学习算法"
---

Autoencoder<sup>[1]</sup>: TDgpt 内置使用自编码器（Autoencoder）的异常检测算法，对周期性的时间序列数据具有较好的检测结果。使用该模型需要针对输入时序数据进行预训练，同时将训练完成的模型保存在到服务目录 `ad_autoencoder` 中，然后在 SQL 语句中指定调用该算法模型即可使用。

```SQL
--- 在 options 中增加 model 的名称，ad_autoencoder_foo， 针对 foo 数据集（表）训练的采用自编码器的异常检测模型进行异常检测
SELECT COUNT(*), _WSTART
FROM foo
ANOMALY_WINDOW(col1, 'algo=encoder, model=ad_autoencoder_foo');
```

后续添加机器（深度）学习异常检测算法
- Isolation Forest
- One-Class Support Vector Machines (SVM)
- Prophet

### 参考文献

1. https://en.wikipedia.org/wiki/Autoencoder
