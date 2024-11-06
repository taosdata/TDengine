---
title: "Anomaly-detection"
sidebar_label: "Anomaly-detection"
---

本节讲述 异常检测算法模型的使用方法。

## 概述
分析平台提供了 6 种异常检查模型，6 种异常检查模型分为 3 个类别，分别属于基于统计的异常检测模型、基于数据密度的检测模型、基于深度学习的异常检测模型。在不指定异常检测使用的方法的情况下，默认调用 iqr 的方法进行计算。


### 统计学异常检测方法

- k-sigma<sup>[1]</sup>: 即 ***68–95–99.7 rule*** 。***k***值默认为 3， 即序列均值的 3 倍标准差范围为边界，超过边界的是异常值。KSigma 要求数据整体上服从正态分布，如果一个点偏离均值 K 倍标准差，则该点被视为异常点.
  
|参数名称|说明|是否必选|默认值|
|---|---|---|---|
|k|标准差倍数|选填|3|


- IQR<sup>[2]</sup>：四分位距 (Interquartile range, IQR) 是一种衡量变异性的方法. 四分位数将一个按等级排序的数据集划分为四个相等的部分。即 Q1（第 1 个四分位数）、Q2（第 2 个四分位数）和 Q3（第 3 个四分位数）。IQR 定义为 Q3–Q1，位于 Q3+1.5 。无输入参数。

- Grubbs<sup>[3]</sup>: 又称为 Grubbs' test，即最大标准残差测试。Grubbs 通常用作检验最大值、最小值偏离均值的程度是否为异常，该单变量数据集遵循近似标准正态分布。非正态分布数据集不能使用该方法。无输入参数。

- SHESD<sup>[4]</sup>： 带有季节性的 ESD 检测算法。ESD 可以检测时间序列数据的多异常点。需要指定异常点比例的上界***k***，最差的情况是至多 49.9%。数据集的异常比例一般不超过 5%
  
|参数名称|说明|是否必选|默认值|
|---|---|---|---|
|k|异常点在输入数据集中占比，范围是$`1\le K \le 49.9`$ |选填|5|


### 基于数据密度的检测方法
LOF<sup>[5]</sup>: 局部离群因子（LOF，又叫局部异常因子）算法是 Breunig 于 2000 年提出的一种基于密度的局部离群点检测算法，该方法适用于不同类簇密度分散情况迥异的数据。根据数据点周围的数据密集情况，首先计算每个数据点的一个局部可达密度，然后通过局部可达密度进一步计算得到每个数据点的一个离群因子，该离群因子即标识了一个数据点的离群程度，因子值越大，表示离群程度越高，因子值越小，表示离群程度越低。最后，输出离群程度最大的 top(n) 个点。


### 基于深度学习的检测方法
使用自动编码器的异常检测模型。可以对具有周期性的数据具有较好的检测结果。但是使用该模型需要针对输入的时序数据进行训练，同时将训练完成的模型部署到服务目录中，才能够运行与使用。


### 参考文献
1. https://en.wikipedia.org/wiki/68%E2%80%9395%E2%80%9399.7_rule
2. https://en.wikipedia.org/wiki/Interquartile_range
3. Adikaram, K. K. L. B.; Hussein, M. A.; Effenberger, M.; Becker, T. (2015-01-14). "Data Transformation Technique to Improve the Outlier Detection Power of Grubbs's Test for Data Expected to Follow Linear Relation". Journal of Applied Mathematics. 2015: 1–9. doi:10.1155/2015/708948.
4. Hochenbaum, O. S. Vallis, and A. Kejariwal. 2017. Automatic Anomaly Detection in the Cloud Via Statistical Learning. arXiv preprint arXiv:1704.07706 (2017).
5. Breunig, M. M.; Kriegel, H.-P.; Ng, R. T.; Sander, J. (2000). LOF: Identifying Density-based Local Outliers (PDF). Proceedings of the 2000 ACM SIGMOD International Conference on Management of Data. SIGMOD. pp. 93–104. doi:10.1145/335191.335388. ISBN 1-58113-217-4.

