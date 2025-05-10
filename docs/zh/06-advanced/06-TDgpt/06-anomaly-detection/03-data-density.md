---
title: "数据密度算法"
sidebar_label: "数据密度算法"
---

### 基于数据密度/数据挖掘的检测算法
LOF<sup>[1]</sup>: Local Outlier Factor(LOF)，局部离群因子/局部异常因子，是 Breunig 在 2000 年提出的一种基于密度的局部离群点检测算法，该方法适用于不同类簇密度分散情况迥异的数据。根据数据点周围的数据密集情况，首先计算每个数据点的局部可达密度，然后通过局部可达密度进一步计算得到每个数据点的一个离群因子。该离群因子即标识了一个数据点的离群程度，因子值越大，表示离群程度越高，因子值越小，表示离群程度越低。最后，输出离群程度最大的 $topK$ 个点。

```SQL
--- 指定调用的算法为LOF，即可调用该算法
SELECT count(*)
FROM foo
ANOMALY_WINDOW(foo.i32, "algo=lof")
```

后续待添加基于数据挖掘检测算法
- DBSCAN (Density-Based Spatial Clustering of Applications with Noise)
- K-Nearest Neighbors (KNN)
- Principal Component Analysis (PCA)

第三方异常检测算法库
- PyOD

### 参考文献

1. Breunig, M. M.; Kriegel, H.-P.; Ng, R. T.; Sander, J. (2000). LOF: Identifying Density-based Local Outliers (PDF). Proceedings of the 2000 ACM SIGMOD International Conference on Management of Data. SIGMOD. pp. 93–104. doi:10.1145/335191.335388. ISBN 1-58113-217-4.
