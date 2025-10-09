---
title: "时间序列相关分析"
sidebar_label: "时间序列相关分析"
description: 时间序列相关分析
---

TDengine TSDB 从 3.3.8.0 开始提供时间序列的相关分析功能。TDengine TSDB 提供了 `corr` 函数针对两个时间序列的皮尔森相关系数系数，
反应两个序列的线性相关性。关于 `corr` 函数使用的细节及相关内容请阅读 [SQL 参考手册](../../14-reference/03-taos-sql/22-function.md#corr)
相关部分。

> 说明：使用 `corr` 函数不需要部署 TDgpt。
