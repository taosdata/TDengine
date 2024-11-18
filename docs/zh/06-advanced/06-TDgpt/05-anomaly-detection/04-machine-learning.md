---
title: "机器学习算法"
sidebar_label: "机器学习算法"
---

Autoencoder: TDgpt 内置使用自编码器（Autoencoder）的异常检测算法，对周期性的时间序列数据具有较好的检测结果。使用该模型需要针对输入时序数据进行预训练，同时将训练完成的模型保存在到服务目录 `ad_autoencoder` 中，然后在 SQL 语句中指定调用该算法模型即可使用。
