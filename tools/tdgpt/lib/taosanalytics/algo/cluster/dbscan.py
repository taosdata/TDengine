# Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

"""DBSCAN clustering algorithm implementation."""

import numpy as np
from sklearn.cluster import DBSCAN
from sklearn.metrics import (
    calinski_harabasz_score,
    davies_bouldin_score,
    silhouette_score,
)

from lib.base import AbstractClusteringService
from lib.log import AppLogger


class _DBScanService(AbstractClusteringService):
    """DBSCAN (Density-Based Spatial Clustering) service using scikit-learn."""

    name = "dbscan"
    desc = "DBSCAN density-based clustering with automatic cluster detection"
    _builtins = True

    def __init__(self):
        super().__init__()
        self.eps = 0.5
        self.min_samples = 5
        self.metric = "euclidean"
        self.model = None

    def set_params(self, params: dict) -> None:
        """Set DBSCAN parameters.

        Args:
            params: dict with keys:
            - eps (float, default 0.5): neighborhood radius
            - min_samples (int, default 5): minimum samples in neighborhood to form core point
            - metric (str, default 'euclidean'): distance metric
        """
        super().set_params(params)

        if "eps" in params:
            self.eps = float(params["eps"])
            if self.eps <= 0:
                raise ValueError("eps must be > 0")

        if "min_samples" in params:
            self.min_samples = int(params["min_samples"])
            if self.min_samples < 1:
                raise ValueError("min_samples must be >= 1")

        if "metric" in params:
            self.metric = str(params["metric"])

    def execute(self) -> dict:
        """Execute DBSCAN clustering.

        Returns:
            dict with clustering results and metrics
        """
        if self.input_data is None or len(self.input_data) == 0:
            raise ValueError("input data is empty")

        try:
            X = np.asarray(self.input_data, dtype=float)
            if not np.isfinite(X).all():
                raise ValueError("input data contains NaN or infinite values")

            if X.ndim != 2:
                raise ValueError(
                    "input data must be 2-dimensional (n_samples, n_features)"
                )

            AppLogger.info(
                "Running DBSCAN with eps=%.4f, min_samples=%d",
                self.eps,
                self.min_samples,
            )
            self.model = DBSCAN(
                eps=self.eps, min_samples=self.min_samples, metric=self.metric
            )
            labels = self.model.fit_predict(X)

            n_clusters = len(set(labels)) - (1 if -1 in labels else 0)
            n_noise = list(labels).count(-1)

            AppLogger.info(
                "DBSCAN completed: %d clusters, %d noise points", n_clusters, n_noise
            )

            # Calculate evaluation metrics (excluding noise points for some metrics)
            metrics = self._calculate_metrics(X, labels, n_clusters)

            result = {
                "labels": labels.tolist(),
                "n_clusters": int(n_clusters),
                "n_noise": int(n_noise),
                "metrics": metrics,
            }

            # DBSCAN doesn't have cluster centers, but we can compute them from members
            centers = []
            for cluster_id in range(n_clusters):
                cluster_mask = labels == cluster_id
                if np.any(cluster_mask):
                    center = X[cluster_mask].mean(axis=0).tolist()
                    centers.append(center)
            result["centers"] = centers

            if self.ts_list is not None:
                result["ts_list"] = self.ts_list

            return result

        except Exception as e:
            AppLogger.error("DBSCAN execution failed: %s", str(e))
            raise

    def _calculate_metrics(
        self, X: np.ndarray, labels: np.ndarray, n_clusters: int
    ) -> dict:
        """Calculate clustering evaluation metrics.

        Args:
            X: feature matrix
            labels: cluster labels (-1 for noise)
            n_clusters: number of actual clusters

        Returns:
            dict with metrics
        """
        metrics = {}

        # Only calculate certain metrics if we have at least 2 clusters
        if n_clusters >= 2:
            mask = labels != -1
            n_non_noise = np.sum(mask)
            metric_functions = [
                ("silhouette", silhouette_score),
                ("davies_bouldin", davies_bouldin_score),
                ("calinski_harabasz", calinski_harabasz_score),
            ]
            for metric_name, metric_function in metric_functions:
                try:
                    if n_non_noise > 0:
                        metrics[metric_name] = float(
                            metric_function(X[mask], labels[mask])
                        )
                    else:
                        metrics[metric_name] = None
                except Exception as e:
                    AppLogger.warning("Failed to calculate %s score: %s", metric_name, e)
                    metrics[metric_name] = None
        else:
            metrics["silhouette"] = None
            metrics["davies_bouldin"] = None
            metrics["calinski_harabasz"] = None

        return metrics
