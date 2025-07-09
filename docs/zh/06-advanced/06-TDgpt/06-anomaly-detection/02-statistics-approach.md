---
title: "统计学算法"
sidebar_label: "统计学算法"
---

- k-sigma<sup>[1]</sup>: 即 ***68–95–99.7 rule*** 。***k***值默认为 3，即序列均值的 3 倍标准差范围为边界，超过边界的是异常值。KSigma 要求数据整体上服从正态分布，如果一个点偏离均值 K 倍标准差，则该点被视为异常点。

| 参数  | 说明    | 是否必选 | 默认值 |
| --- | ----- | ---- | --- |
| k   | 标准差倍数 | 选填   | 3   |

```SQL
--- 指定调用的算法为ksigma，参数 k 为 2
SELECT _WSTART, COUNT(*)
FROM foo
ANOMALY_WINDOW(foo.i32, "algo=ksigma,k=2")
```

- IQR<sup>[2]</sup>：Interquartile range(IQR)，四分位距是一种衡量变异性的方法。四分位数将一个按等级排序的数据集划分为四个相等的部分。即 Q1（第 1 个四分位数）、Q2（第 2 个四分位数）和 Q3（第 3 个四分位数）。 $IQR=Q3-Q1$，对于 $v$，$Q1-(1.5 \times IQR) \le v \le Q3+(1.5 \times IQR)$ 是正常值，范围之外的是异常值。无输入参数。

```SQL
--- 指定调用的算法为 iqr，无参数
SELECT _WSTART, COUNT(*)
FROM foo
ANOMALY_WINDOW(foo.i32, "algo=iqr")
```

- Grubbs<sup>[3]</sup>: Grubbs' test，即最大标准残差测试。Grubbs 通常用作检验最大值、最小值偏离均值的程度是否为异常，要求单变量数据集遵循近似标准正态分布。非正态分布数据集不能使用该方法。无输入参数。

```SQL
--- 指定调用的算法为 grubbs，无参数
SELECT _WSTART, COUNT(*)
FROM foo
ANOMALY_WINDOW(foo.i32, "algo=grubbs")
```

- SHESD<sup>[4]</sup>：带有季节性的 ESD 检测算法。ESD 可以检测时间序列数据的多异常点。需要指定异常检测方向（'pos' / 'neg' / 'both'），异常值比例的上界***max_anoms***，最差的情况是至多 49.9%。数据集的异常比例一般不超过 5%

| 参数        | 说明                               | 是否必选 | 默认值    |
| --------- | -------------------------------- | ---- | ------ |
| direction | 异常检测方向类型 ('pos' / 'neg' / 'both') | 否    | "both" |
| max_anoms | 异常值比例 $0 < K \le 49.9$           | 否    | 0.05   |
| period    | 一个周期包含的数据点                       | 否    | 0      |

```SQL
--- 指定调用的算法为 shesd，参数 direction 为 both，异常值比例 5%
SELECT _WSTART, COUNT(*)
FROM foo
ANOMALY_WINDOW(foo.i32, "algo=shesd,direction=both,anoms=0.05")
```

后续待添加异常检测算法

- Gaussian Process Regression

基于变点检测的异常检测算法  

- CUSUM (Cumulative Sum Control Chart)
- PELT (Pruned Exact Linear Time)

### 参考文献

1. [https://en.wikipedia.org/wiki/68–95–99.7 rule](https://en.wikipedia.org/wiki/68%E2%80%9395%E2%80%9399.7_rule)
2. [https://en.wikipedia.org/wiki/Interquartile_range](https://en.wikipedia.org/wiki/Interquartile_range)
3. [Adikaram, K. K. L. B.; Hussein, M. A.; Effenberger, M.; Becker, T. (2015-01-14). "Data Transformation Technique to Improve the Outlier Detection Power of Grubbs's Test for Data Expected to Follow Linear Relation". Journal of Applied Mathematics. 2015: 1–9. doi:10.1155/2015/708948.](https://onlinelibrary.wiley.com/doi/epdf/10.1155/2015/708948)
4. [Hochenbaum, O. S. Vallis, and A. Kejariwal. 2017. Automatic Anomaly Detection in the Cloud Via Statistical Learning.](https://arxiv.org/abs/1704.07706).
