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

"""Gaussian Mixture Model clustering algorithm implementation."""

import numpy as np
from sklearn.metrics import (
    calinski_harabasz_score,
    davies_bouldin_score,
    silhouette_score,
)
from sklearn.mixture import GaussianMixture

from taosanalytics.base import AbstractClusteringService
from taosanalytics.log import AppLogger


class _GaussianMixtureService(AbstractClusteringService):
    """Gaussian Mixture Model clustering service using scikit-learn."""

    name = "gmm"
    desc = "Gaussian Mixture Model soft clustering with probabilistic assignments"
    _builtins = True

    def __init__(self):
        super().__init__()
        self.n_components = 3
        self.covariance_type = "full"
        self.max_iter = 100
        self.random_state = None
        self.model = None

    def set_params(self, params: dict) -> None:
        """Set GMM parameters.

        Args:
            params: dict with keys:
            - n_components (int, default 3): number of Gaussian components
            - covariance_type (str, default 'full'): type of covariance ('full', 'tied', 'diag', 'spherical')
            - max_iter (int, default 100): maximum iterations for EM algorithm
            - random_state (int, optional): random seed
        """
        super().set_params(params)

        if "n_components" in params:
            self.n_components = int(params["n_components"])
            if self.n_components < 1:
                raise ValueError("n_components must be >= 1")

        if "covariance_type" in params:
            self.covariance_type = str(params["covariance_type"])
            if self.covariance_type not in ["full", "tied", "diag", "spherical"]:
                raise ValueError(
                    "covariance_type must be one of: full, tied, diag, spherical"
                )

        if "max_iter" in params:
            self.max_iter = int(params["max_iter"])
            if self.max_iter <= 0:
                raise ValueError("max_iter must be > 0")

        if "random_state" in params:
            self.random_state = int(params["random_state"])

    def execute(self) -> dict:
        """Execute Gaussian Mixture Model clustering.

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

            if len(X) < self.n_components:
                raise ValueError(
                    f"number of samples ({len(X)}) is less than n_components ({self.n_components})"
                )

            AppLogger.info(
                "Running GMM with n_components=%d, covariance_type=%s",
                self.n_components,
                self.covariance_type,
            )
            self.model = GaussianMixture(
                n_components=self.n_components,
                covariance_type=self.covariance_type,
                max_iter=self.max_iter,
                random_state=self.random_state,
            )
            self.model.fit(X)
            labels = self.model.predict(X)
            probabilities = self.model.predict_proba(X)

            # Calculate evaluation metrics
            metrics = self._calculate_metrics(X, labels)
            metrics["bic"] = float(self.model.bic(X))
            metrics["aic"] = float(self.model.aic(X))

            result = {
                "labels": labels.tolist(),
                "centers": self.model.means_.tolist(),
                "n_clusters": int(self.n_components),
                "probabilities": probabilities.tolist(),
                "metrics": metrics,
            }

            if self.ts_list is not None:
                result["ts_list"] = self.ts_list

            AppLogger.info(
                "GMM completed: %d components, BIC=%.4f, AIC=%.4f, silhouette=%.4f",
                self.n_components,
                metrics["bic"],
                metrics["aic"],
                metrics["silhouette"] or 0,
            )
            return result

        except Exception as e:
            AppLogger.error("GMM execution failed: %s", str(e))
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
