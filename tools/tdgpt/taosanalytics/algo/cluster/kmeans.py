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

"""K-Means clustering algorithm implementation."""

import numpy as np
from sklearn.cluster import KMeans
from sklearn.metrics import (
    calinski_harabasz_score,
    davies_bouldin_score,
    silhouette_score,
)

from taosanalytics.base import AbstractClusteringService
from taosanalytics.log import AppLogger


class _KMeansService(AbstractClusteringService):
    """K-Means clustering service using scikit-learn."""

    name = "kmeans"
    desc = "K-Means clustering with configurable number of clusters"
    _builtins = True

    def __init__(self):
        super().__init__()
        self.n_clusters = 3
        self.init = "k-means++"
        self.max_iter = 300
        self.random_state = None
        self.n_init = 10
        self.model = None

    def set_params(self, params: dict) -> None:
        """Set K-Means parameters.

        Args:
            params: dict with keys:
            - n_clusters (int, default 3): number of clusters
            - init (str, default 'k-means++'): initialization method
            - max_iter (int, default 300): maximum iterations
            - random_state (int, optional): random seed
            - n_init (int, default 10): number of times to run with different seeds
        """
        super().set_params(params)

        if "n_clusters" in params:
            self.n_clusters = int(params["n_clusters"])
            if self.n_clusters < 1:
                raise ValueError("n_clusters must be >= 1")

        if "init" in params:
            self.init = str(params["init"])
            if self.init not in ["k-means++", "random"]:
                raise ValueError("init must be 'k-means++' or 'random'")

        if "max_iter" in params:
            self.max_iter = int(params["max_iter"])
            if self.max_iter <= 0:
                raise ValueError("max_iter must be > 0")

        if "random_state" in params:
            self.random_state = int(params["random_state"])

        if "n_init" in params:
            self.n_init = int(params["n_init"])
            if self.n_init < 1:
                raise ValueError("n_init must be >= 1")

    def execute(self) -> dict:
        """Execute K-Means clustering.

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

            if len(X) < self.n_clusters:
                raise ValueError(
                    f"number of samples ({len(X)}) is less than n_clusters ({self.n_clusters})"
                )

            AppLogger.info("Running K-Means with n_clusters=%d", self.n_clusters)
            self.model = KMeans(
                n_clusters=self.n_clusters,
                init=self.init,
                max_iter=self.max_iter,
                random_state=self.random_state,
                n_init=self.n_init,
            )
            labels = self.model.fit_predict(X)

            # Calculate evaluation metrics
            metrics = self._calculate_metrics(X, labels)
            metrics["inertia"] = float(self.model.inertia_)

            result = {
                "labels": labels.tolist(),
                "centers": self.model.cluster_centers_.tolist(),
                "n_clusters": int(self.n_clusters),
                "metrics": metrics,
            }

            if self.ts_list is not None:
                result["ts_list"] = self.ts_list

            AppLogger.info(
                "K-Means completed: %d clusters, inertia=%.4f, silhouette=%.4f",
                self.n_clusters,
                metrics["inertia"],
                metrics["silhouette"] or 0,
            )
            return result

        except Exception as e:
            AppLogger.error("K-Means execution failed: %s", str(e))
            raise

    def _calculate_metrics(self, X: np.ndarray, labels: np.ndarray) -> dict:
        """Calculate clustering evaluation metrics.

        Args:
            X: feature matrix
            labels: cluster labels

        Returns:
            dict with metrics
        """
        metrics = {}

        # Silhouette score (range: [-1, 1], higher is better)
        try:
            metrics["silhouette"] = float(silhouette_score(X, labels))
        except Exception as e:
            AppLogger.warning("Failed to calculate silhouette score: %s", e)
            metrics["silhouette"] = None

        # Davies-Bouldin index (lower is better)
        try:
            metrics["davies_bouldin"] = float(davies_bouldin_score(X, labels))
        except Exception as e:
            AppLogger.warning("Failed to calculate davies_bouldin score: %s", e)
            metrics["davies_bouldin"] = None

        # Calinski-Harabasz index (higher is better)
        try:
            metrics["calinski_harabasz"] = float(calinski_harabasz_score(X, labels))
        except Exception as e:
            AppLogger.warning("Failed to calculate calinski_harabasz score: %s", e)
            metrics["calinski_harabasz"] = None

        return metrics
