---
title: "检测算法"
sidebar_label: "检测算法"
---

### 统计学异常检测方法

- k-sigma<sup>[1]</sup>: 即 ***68–95–99.7 rule*** 。***k***值默认为 3，即序列均值的 3 倍标准差范围为边界，超过边界的是异常值。KSigma 要求数据整体上服从正态分布，如果一个点偏离均值 K 倍标准差，则该点被视为异常点.

|参数|说明|是否必选|默认值|
|---|---|---|---|
|k|标准差倍数|选填|3|


- IQR<sup>[2]</sup>：Interquartile range(IQR)，四分位距是一种衡量变异性的方法。四分位数将一个按等级排序的数据集划分为四个相等的部分。即 Q1（第 1 个四分位数）、Q2（第 2 个四分位数）和 Q3（第 3 个四分位数）。 $IQR=Q3-Q1$，对于 $v$, $Q1-(1.5 \times IQR) \le v \le Q3+(1.5 \times IQR)$ 是正常值，范围之外的是异常值。无输入参数。

- Grubbs<sup>[3]</sup>: Grubbs' test，即最大标准残差测试。Grubbs 通常用作检验最大值、最小值偏离均值的程度是否为异常，要求单变量数据集遵循近似标准正态分布。非正态分布数据集不能使用该方法。无输入参数。

- SHESD<sup>[4]</sup>： 带有季节性的 ESD 检测算法。ESD 可以检测时间序列数据的多异常点。需要指定异常点比例的上界***k***，最差的情况是至多 49.9%。数据集的异常比例一般不超过 5%

|参数|说明|是否必选|默认值|
|---|---|---|---|
|k|异常点在输入数据集中占比 $1 \le K \le 49.9$ |选填|5|

### 参考文献
1. [https://en.wikipedia.org/wiki/68–95–99.7 rule](https://en.wikipedia.org/wiki/68%E2%80%9395%E2%80%9399.7_rule)
2. https://en.wikipedia.org/wiki/Interquartile_range
3. Adikaram, K. K. L. B.; Hussein, M. A.; Effenberger, M.; Becker, T. (2015-01-14). "Data Transformation Technique to Improve the Outlier Detection Power of Grubbs's Test for Data Expected to Follow Linear Relation". Journal of Applied Mathematics. 2015: 1–9. doi:10.1155/2015/708948.
4. Hochenbaum, O. S. Vallis, and A. Kejariwal. 2017. Automatic Anomaly Detection in the Cloud Via Statistical Learning. arXiv preprint arXiv:1704.07706 (2017).
