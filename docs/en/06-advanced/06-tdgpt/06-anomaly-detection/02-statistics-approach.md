---
title: Statistical Algorithms
sidebar_label: Statistical Algorithms
---

- k-sigma<sup>[1]</sup>, or ***68–95–99.7 rule***: The *k* value defines how many standard deviations indicate an anomaly. The default value is 3. The k-sigma algorithm require data to be in a regular distribution. Data points that lie outside of *k* standard deviations are considered anomalous.

|Parameter|Description|Required?|Default|
|---|---|---|---|
|k|Number of standard deviations|No|3|

```SQL
--- Use the k-sigma algorithm with a k value of 2
SELECT _WSTART, COUNT(*)
FROM foo
ANOMALY_WINDOW(foo.i32, "algo=ksigma,k=2")
```

- Interquartile range (IQR)<sup>[2]</sup>: IQR divides a rank-ordered dataset into even quartiles, Q1 through Q3. IQR=Q3-Q1, for *v*, Q1 - (1.5 x IQR) \<= v \<= Q3 + (1.5 x IQR) is normal. Data points outside this range are considered anomalous. This algorithm does not take any parameters.

```SQL
--- Use the IQR algorithm.
SELECT _WSTART, COUNT(*)
FROM foo
ANOMALY_WINDOW(foo.i32, "algo=iqr")
```

- Grubbs's test<sup>[3]</sup>, or maximum normalized residual test: Grubbs is used to test whether the deviation from mean of the maximum and minimum is anomalous. It requires a univariate data set in a close to normal distribution. Grubbs's test cannot be uses for datasets that are not normally distributed. This algorithm does not take any parameters.

```SQL
--- Use Grubbs's test.
SELECT _WSTART, COUNT(*)
FROM foo
ANOMALY_WINDOW(foo.i32, "algo=grubbs")
```

- Seasonal Hybrid ESD (S-H-ESD)<sup>[4]</sup>: Extreme Studentized Deviate (ESD) can identify multiple anomalies in time-series data. You define whether to detect positive anomalies (`pos`), negative anomalies (`neg`), or both (`both`). The maximum proportion of data that can be anomalous (`max_anoms`) is at worst 49.9% Typically, the proportion of anomalies in a dataset does not exceed 5%.

|Parameter|Description|Required?|Default|
|---|---|---|---|
|direction|Specify the direction of anomalies ('pos', 'neg', or 'both').|No|"both"|
|max_anoms|Specify maximum proportion of data that can be anomalous *k*, where 0 \< *k* \<= 49.9|No|0.05|
|period|The number of data points included in each period|No|0|

```SQL
--- Use the SHESD algorithm in both directions with a maximum 5% of the data being anomalous
SELECT _WSTART, COUNT(*)
FROM foo
ANOMALY_WINDOW(foo.i32, "algo=shesd,direction=both,anoms=0.05")
```

The following algorithms are in development:

- Gaussian Process Regression

Change point detection--based algorithms:  

- CUSUM (Cumulative Sum Control Chart)
- PELT (Pruned Exact Linear Time)

### References

1. [https://en.wikipedia.org/wiki/68–95–99.7 rule](https://en.wikipedia.org/wiki/68%E2%80%9395%E2%80%9399.7_rule)
2. [Interquartile range](https://en.wikipedia.org/wiki/Interquartile_range)
3. Adikaram, K. K. L. B.; Hussein, M. A.; Effenberger, M.; Becker, T. (2015-01-14). "Data Transformation Technique to Improve the Outlier Detection Power of Grubbs's Test for Data Expected to Follow Linear Relation". Journal of Applied Mathematics. 2015: 1–9. doi:10.1155/2015/708948.
4. Hochenbaum, O. S. Vallis, and A. Kejariwal. 2017. Automatic Anomaly Detection in the Cloud Via Statistical Learning. arXiv preprint arXiv:1704.07706 (2017).
