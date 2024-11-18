---
title: "检测算法"
sidebar_label: "检测算法"
---

本节介绍内置异常检测算法模型的定义和使用方法。

## 概述
分析平台内置了6个异常检查模型，分为3个类别，分别是基于统计学的模型、基于数据密度的模型、以及基于深度学习的模型。在不指定异常检测使用的方法的情况下，默认调用 iqr 进行异常检测。







### 参考文献
1. [https://en.wikipedia.org/wiki/68–95–99.7 rule](https://en.wikipedia.org/wiki/68%E2%80%9395%E2%80%9399.7_rule)
2. https://en.wikipedia.org/wiki/Interquartile_range
3. Adikaram, K. K. L. B.; Hussein, M. A.; Effenberger, M.; Becker, T. (2015-01-14). "Data Transformation Technique to Improve the Outlier Detection Power of Grubbs's Test for Data Expected to Follow Linear Relation". Journal of Applied Mathematics. 2015: 1–9. doi:10.1155/2015/708948.
4. Hochenbaum, O. S. Vallis, and A. Kejariwal. 2017. Automatic Anomaly Detection in the Cloud Via Statistical Learning. arXiv preprint arXiv:1704.07706 (2017).
5. Breunig, M. M.; Kriegel, H.-P.; Ng, R. T.; Sander, J. (2000). LOF: Identifying Density-based Local Outliers (PDF). Proceedings of the 2000 ACM SIGMOD International Conference on Management of Data. SIGMOD. pp. 93–104. doi:10.1145/335191.335388. ISBN 1-58113-217-4.

