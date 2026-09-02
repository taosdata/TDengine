---
title: Data Density Algorithms
sidebar_label: Data Density Algorithms
---

### Data Density/Mining Algorithms

LOF is a density-based algorithm for determining local outliers proposed by Breunig et al. in 2000. It is suitable for data with varying cluster densities and diverse dispersion. First, the local reachability density of each data point is calculated based on the density of its neighborhood. The local reachability density is then used to assign an outlier factor to each data point. This outlier factor indicates how anomalous a data point is. A higher factor indicates more anomalous data. Finally, the top *k* outliers are output.

Starting in `v3.4.1.13`, LOF supports multivariate anomaly detection. You can pass multiple numeric columns as input features.

```sql
--- Use LOF.
SELECT count(*)
FROM foo
ANOMALY_WINDOW(foo.i32, "algo=lof")

--- Use LOF for multi-variate anomaly detection.
SELECT count(*)
FROM foo
ANOMALY_WINDOW(foo.i32, foo.i64, foo.f32, "algo=lof")
```

The following algorithms are in development:

- DBSCAN (Density-Based Spatial Clustering of Applications with Noise)
- K-Nearest Neighbors (KNN)

Third-party anomaly detection algorithms:

- PyOD: ECOD, HBOS, COPOD, IForest, and PCA are available in the default TDgpt runtime.

These algorithms do not require a pretrained model file. TDgpt fits the detector
on the input data at runtime and returns anomaly windows from the detected
outlier points. They support multi-variate input, so you can pass multiple
numeric columns as features.

|Algorithm|Description|Parameters|
| ------- | ---------- | -------- |
|ECOD|Empirical cumulative distribution based outlier detection.|`contamination`|
|HBOS|Histogram based outlier score.|`contamination`, `n_bins`|
|COPOD|Copula based outlier detection.|`contamination`|
|IForest|Isolation Forest based outlier detection.|`contamination`, `n_estimators`, `random_state`|
|PCA|Principal component analysis based outlier detection.|`contamination`, `n_components`, `standardization`|

|Parameter|Definition|Default|
| ------- | --------- | ------ |
|`contamination`|Expected proportion of outliers. Valid range is `(0, 0.5]`.|`0.1`|
|`n_bins`|Number of histogram bins used by HBOS. Must be at least `2`.|`10`|
|`n_estimators`|Number of isolation trees used by IForest. Must be greater than `0`.|`100`|
|`random_state`|Random seed used by IForest.|`42`|
|`n_components`|Number of principal components used by PCA. Must be greater than `0`.|All components|
|`standardization`|Whether PCA standardizes input features. Use `1` to enable or `0` to disable.|`1`|

```sql
--- Use PyOD ECOD for multi-variate anomaly detection.
SELECT count(*)
FROM foo
ANOMALY_WINDOW(foo.i32, foo.i64, "algo=ecod,contamination=0.1")

--- Use PyOD HBOS.
SELECT count(*)
FROM foo
ANOMALY_WINDOW(foo.i32, foo.i64, "algo=hbos,contamination=0.1,n_bins=10")

--- Use PyOD COPOD.
SELECT count(*)
FROM foo
ANOMALY_WINDOW(foo.i32, foo.i64, "algo=copod,contamination=0.1")

--- Use PyOD Isolation Forest.
SELECT count(*)
FROM foo
ANOMALY_WINDOW(foo.i32, foo.i64, "algo=iforest,contamination=0.1,n_estimators=100,random_state=42")

--- Use PyOD PCA.
SELECT count(*)
FROM foo
ANOMALY_WINDOW(foo.i32, foo.i64, "algo=pca,contamination=0.1,n_components=2")
```

### References

1. Breunig, M. M.; Kriegel, H.-P.; Ng, R. T.; Sander, J. (2000). LOF: Identifying Density-based Local Outliers (PDF). Proceedings of the 2000 ACM SIGMOD International Conference on Management of Data. SIGMOD. pp. 93–104. doi:10.1145/335191.335388. ISBN 1-58113-217-4.
