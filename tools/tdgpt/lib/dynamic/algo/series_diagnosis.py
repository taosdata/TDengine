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

"""Series diagnosis scorers: catch22 feature extraction + IsolationForest / OCSVM."""

import json
from abc import ABC, abstractmethod

import numpy as np

from lib.dynamic.algo.detector import BaseModelAnomalyDetector
from lib.dynamic.model_loader import ModelLoader
from lib.log import AppLogger

# catch22 produces unreliable features below this length.
_MIN_SERIES_LENGTH = 50


class BaseModelSeriesDiagnosisScorer(ABC):
    """Base class for whole-series diagnosis models.

    Subclass must implement _build_model() and _score().
    _extract_features() defaults to catch22; override to use a different extractor,
    but training and inference must use the same one.

    Score convention: higher = more anomalous.
    """

    target_algo: str = ""

    def __init__(self, path: str, series: list):
        self.path = path
        self.series = series
        self.model_info: dict | None = None
        self._model = None

    def build(self):
        self.model_info = self._load_config()
        if not self.model_info:
            return None
        if not self._is_expected_algo():
            AppLogger.error(
                "config does not describe a %s model (got algo=%s), skipping",
                self.target_algo,
                self.model_info.get("algo"),
            )
            return None
        self._model = self._build_model()
        return self._model

    def predict(self) -> float:
        """Validate series, extract features, apply pipeline, and return an anomaly score."""
        model = self._model or self.build()
        if model is None:
            AppLogger.error("model unavailable for series diagnosis: %s", self.path)
            raise RuntimeError(f"series diagnosis model unavailable: {self.path}")
        self._validate_series(self.series)
        features = self._extract_features(self.series)
        X = np.array(features, dtype=float).reshape(1, -1)
        X = self._apply_preprocessing(X)
        return self._score(model, X)

    def _validate_series(self, series: list) -> None:
        n = len(series) if series is not None else 0
        if n < _MIN_SERIES_LENGTH:
            raise ValueError(
                f"Series too short for diagnosis: got {n}, need >= {_MIN_SERIES_LENGTH}"
            )
        # Reject NaN, ±Inf, and None — all produce meaningless catch22 features.
        if any(v is None or not np.isfinite(float(v)) for v in series):
            raise ValueError("Series contains NaN, ±Inf, or None values")

    def _extract_features(self, series: list) -> list:
        """Extract catch22 features, replacing NaN/Inf with 0.0."""
        try:
            import pycatch22
        except ImportError as exc:
            raise RuntimeError(
                "pycatch22 is not available; install it to use series diagnosis"
            ) from exc
        raw = pycatch22.catch22_all(series)["values"]
        return [
            0.0 if v is None or not np.isfinite(v) else float(v)
            for v in raw
        ]

    def _apply_preprocessing(self, X: np.ndarray) -> np.ndarray:
        """Apply fill/standardization pipeline stored alongside the model pkl."""
        pipeline_state = self.model_info.get("_pipeline_state") if self.model_info else None
        if pipeline_state:
            X = ModelLoader.apply_pipeline_preprocessing(X, pipeline_state)
        return X

    def _load_config(self) -> dict | None:
        try:
            with open(self.path, "r", encoding="utf-8") as handle:
                return json.load(handle)
        except FileNotFoundError:
            AppLogger.error("model config not found: %s", self.path)
        except Exception as e:
            AppLogger.error("failed to load model config %s: %s", self.path, e)
        return None

    def _is_expected_algo(self) -> bool:
        algo = (self.model_info.get("algo") or "").upper().replace("-", "_")
        return algo == self.target_algo.upper().replace("-", "_")

    def _load_from_pkl(self, model_path: str, expected_type):
        model, pipeline_state = BaseModelAnomalyDetector._load_pkl_model(
            model_path, expected_type
        )
        if model is not None:
            self.model_info["_pipeline_state"] = pipeline_state
            AppLogger.info(
                "loaded %s from pkl file: %s", expected_type.__name__, model_path
            )
        return model

    @abstractmethod
    def _build_model(self):
        """Load the model ready for inference."""

    @abstractmethod
    def _score(self, model, X: np.ndarray) -> float:
        """Return anomaly score for the preprocessed feature matrix (shape 1×d).
        Higher = more anomalous."""


class IsolationForestSeriesDiagnosisScorer(BaseModelSeriesDiagnosisScorer):
    """Isolation Forest scorer using catch22 features.

    decision_function returns positive for inliers and negative for outliers;
    negated here so higher score = more anomalous.
    """

    target_algo = "IFOREST_DIAGNOSIS"

    def _build_model(self):
        try:
            from sklearn.ensemble import IsolationForest
        except ImportError as exc:
            AppLogger.error("sklearn is not available: %s", exc)
            return None
        model_path = self.model_info.get("model_path")
        if not model_path:
            AppLogger.error("IsolationForest requires model_path (pkl file)")
            return None
        return self._load_from_pkl(model_path, IsolationForest)

    def _score(self, model, X: np.ndarray) -> float:
        return float(-model.decision_function(X)[0])


class OCSVMSeriesDiagnosisScorer(BaseModelSeriesDiagnosisScorer):
    """One-Class SVM scorer using catch22 features.

    decision_function returns positive for inliers and negative for outliers;
    negated here so higher score = more anomalous.
    """

    target_algo = "SVM_DIAGNOSIS"

    def _build_model(self):
        try:
            from sklearn.svm import OneClassSVM
        except ImportError as exc:
            AppLogger.error("sklearn is not available: %s", exc)
            return None
        model_path = self.model_info.get("model_path")
        if not model_path:
            AppLogger.error("OneClassSVM requires model_path (pkl file)")
            return None
        return self._load_from_pkl(model_path, OneClassSVM)

    def _score(self, model, X: np.ndarray) -> float:
        return float(-model.decision_function(X)[0])
