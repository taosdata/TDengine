---
title: Data Density Algorithms
sidebar_label: Data Density Algorithms
---

### Data Density/Mining Algorithms

LOF is a density-based algorithm for determining local outliers proposed by Breunig et al. in 2000. It is suitable for data with varying cluster densities and diverse dispersion. First, the local reachability density of each data point is calculated based on the density of its neighborhood. The local reachability density is then used to assign an outlier factor to each data point. This outlier factor indicates how anomalous a data point is. A higher factor indicates more anomalous data. Finally, the top *k* outliers are output.

```SQL
--- Use LOF.
SELECT count(*)
FROM foo
ANOMALY_WINDOW(foo.i32, "algo=lof")
```

The following algorithms are in development:

- DBSCAN (Density-Based Spatial Clustering of Applications with Noise)
- K-Nearest Neighbors (KNN)
- Principal Component Analysis (PCA)

Third-party anomaly detection algorithms:

- PyOD

### References

1. Breunig, M. M.; Kriegel, H.-P.; Ng, R. T.; Sander, J. (2000). LOF: Identifying Density-based Local Outliers (PDF). Proceedings of the 2000 ACM SIGMOD International Conference on Management of Data. SIGMOD. pp. 93–104. doi:10.1145/335191.335388. ISBN 1-58113-217-4.
