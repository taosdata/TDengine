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

"""
BaseModelAnomalyDetector and concrete implementations for dynamic anomaly detection.

Mirrors the structure of forecaster.py:
  - BaseModelAnomalyDetector  ←→  BaseModelForecaster
  - detect()                  ←→  forecast()
  - _build_model()            ←→  _build_model()
  - _predict() → list[int]   ←→  _predict() → pd.DataFrame
"""

import json
from abc import ABC, abstractmethod

import numpy as np

from lib.dynamic.model_loader import ModelLoader
from lib.log import AppLogger


class BaseModelAnomalyDetector(ABC):
    """
    Dynamic loader for anomaly detection models driven by a JSON config file.

    Responsibilities:
      - Load and validate the config file
      - Confirm the config describes the expected algorithm (target_algo)
      - Load/build the model via _build_model()
      - Return per-point anomaly codes via detect() → list[int]

    Return convention (mirrors AbstractAnomalyDetectionService):
      valid_code  → normal point
      -1          → anomaly point
    """

    target_algo: str = ""

    def __init__(
        self,
        path: str,
        input_list: list,
        ts_list: list = None,
        valid_code: int = 1,
        input_data_lists: list | None = None,
    ):
        self.path = path
        self.input_list = input_list
        self.ts_list = ts_list
        self.valid_code = valid_code

        # input_data_lists holds one sub-list per column (same layout as
        # AbstractAnomalyDetectionService.input_data_lists).  Falls back to
        # [input_list] when not provided so single-column callers are unaffected.
        self.input_data_lists: list = (
            input_data_lists if input_data_lists is not None else [input_list]
        )
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

    def detect(self) -> list:
        """Run anomaly detection and return a per-point list of anomaly codes."""
        model = self._model or self.build()
        if model is None:
            AppLogger.error("model unavailable for anomaly detection: %s", self.path)
            raise RuntimeError(f"anomaly detection model unavailable: {self.path}")
        return self._predict(model)

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

    @abstractmethod
    def _build_model(self):
        """Load or build the model ready for inference."""

    @abstractmethod
    def _predict(self, model) -> list:
        """Run inference and return per-point anomaly codes (valid_code or -1)."""

    @abstractmethod
    def get_param(self) -> dict:
        """Return model parameters for logging / introspection."""

    @staticmethod
    def _load_pkl_model(model_path: str, expected_type) -> tuple:
        """Load pkl file and extract model + pipeline_state.

        Returns:
            (model, pipeline_state) tuple, or (None, None) on failure
        """
        return ModelLoader.load_pkl_model(model_path, expected_type)

    @staticmethod
    def _apply_preprocessing(
        X: np.ndarray, pipeline_state: dict
    ) -> np.ndarray:
        """Apply preprocessing from pipeline state (normalization, fillna, etc.)."""
        return ModelLoader.apply_pipeline_preprocessing(X, pipeline_state)


class IsolationForestModelDetector(BaseModelAnomalyDetector):
    """
    Anomaly detector that loads a pre-trained sklearn IsolationForest from
    ``model_path`` and scores the input at point or window level.

    Expected config layout:
    {
      "algo": "iforest",
      "mode": "window",        <- "window" or "point"; defaults to "window"
      "model_path": "/path/to/model.pkl",
      "best_params": {
        "n_estimators": 300,
        "max_samples": 256,
        "max_features": 0.5,
        "contamination": 0.001,
        "window_size": 100,   <- sliding window length
        "feature_fns": ["mean", "std", "slope"],
        "stride": 1           <- step between consecutive windows
      }
    }

    The pkl may carry its training-time window parameters in its pipeline state;
    those win over ``best_params``, which wins over the ``100`` / ``1`` / ``[]``
    defaults.

    Inference flow:
      1. Read the inference mode; ``point`` skips steps 3 and 5 and scores each
         point on its own.
      2. Load the IsolationForest from ``model_path`` (``best_params`` cannot
         construct one).
      3. Slide a window of length window_size over each input column with the given
         stride.  For each window, compute the features listed in feature_fns
         (supported: "mean", "std", "slope") and concatenate the feature vectors of
         all columns.  If feature_fns is empty the raw window values are used instead.
         With C columns the matrix has shape
           (n_windows, C * len(feature_fns))  when feature_fns is set, or
           (n_windows, C * window_size)        otherwise.
      4. Score the matrix with the loaded model: sklearn convention, 1 = inlier,
         -1 = outlier.
      5. Map window-level predictions back to per-point codes:
         a point is anomalous if any window that covers it is anomalous.
    """

    target_algo = "IFOREST"

    def _build_model(self):
        from sklearn.ensemble import IsolationForest

        model_path = self.model_info.get("model_path")

        if not model_path:
            AppLogger.error(
                "IsolationForest models require model_path (pkl file); best_params not supported"
            )
            return None

        model, pipeline_state = BaseModelAnomalyDetector._load_pkl_model(
            model_path, IsolationForest
        )
        if model is not None:
            self.model_info["_pipeline_state"] = pipeline_state
            AppLogger.info("loaded IsolationForest from pkl file: %s", model_path)
            return model

        return None

    @staticmethod
    def _extract_features(window: np.ndarray, feature_fns: list) -> list:
        """Compute a feature vector from a single window."""
        features = []
        for fn in feature_fns:
            if fn == "mean":
                features.append(float(np.mean(window)))
            elif fn == "std":
                features.append(float(np.std(window)))
            elif fn == "slope":
                x = np.arange(len(window), dtype=float)
                features.append(float(np.polyfit(x, window, 1)[0]))
            else:
                AppLogger.warning("unknown feature function '%s', skipping", fn)
        return features

    @staticmethod
    def _validate_window_params(params: dict) -> tuple[int, int]:
        """Return validated sliding-window parameters (window_size, stride)."""
        try:
            window_size = int(params.get("window_size", 100))
            stride = int(params.get("stride", 1))
        except (TypeError, ValueError) as e:
            raise ValueError("window_size and stride must be integers") from e

        if window_size <= 0 or stride <= 0:
            raise ValueError(
                "window_size and stride must be positive integers; "
                f"got window_size={window_size}, stride={stride}"
            )

        return window_size, stride

    def _resolve_window_params(self) -> tuple[list, int, int]:
        """Return ``(feature_fns, window_size, stride)`` for window-level scoring.

        A pkl carries the values it was trained with in its pipeline state, so
        that state wins. ``best_params`` is consulted next: the config layout
        documented on this class states the parameters there, and reading the
        pkl state alone made a model whose pipeline held preprocessing only
        silently score with the ``[]`` / ``100`` / ``1`` defaults.

        Precedence is pipeline state, then ``best_params``, then the defaults.
        """
        pipeline_state = self.model_info.get("_pipeline_state") or {}
        params = self.model_info.get("best_params") or {}

        if "feature_fns" in pipeline_state:
            feature_fns = pipeline_state["feature_fns"]
        else:
            feature_fns = params.get("feature_fns", [])

        window_size, stride = self._validate_window_params(
            {
                "window_size": pipeline_state.get(
                    "window_size", params.get("window_size", 100)
                ),
                "stride": pipeline_state.get("stride", params.get("stride", 1)),
            }
        )
        return feature_fns, window_size, stride

    def _predict(self, model) -> list:
        if self._get_inference_mode() == "point":
            return self._predict_point_level(model)
        return self._predict_window_level(model)

    def _get_inference_mode(self) -> str:
        """Return the mode the model was trained in: ``point`` or ``window``.

        ``mode`` is a top-level field of the config, written by the training
        run. The older lookup only read ``pipeline_state["mode"]`` for a loaded
        pkl, or ``best_params["mode"]`` when no pkl was loaded, and defaulted to
        ``window`` in both cases -- so a config that carried ``mode`` at the top
        level was scored at window level even when it had been trained point by
        point.

        Those older locations stay in the chain, so configs written before the
        top-level field existed keep their previous reading:

          * a pkl model uses ``pipeline_state["mode"]``, else ``window``;
          * a ``best_params`` model uses ``best_params["mode"]``, else
            ``window``.

        Returns:
            str: ``point`` or ``window``. A value this version does not know is
                logged and replaced by ``window``, the default the older lookup
                used.
        """
        pipeline_state = self.model_info.get("_pipeline_state")
        if pipeline_state:
            legacy_mode = pipeline_state.get("mode")
        else:
            legacy_mode = (self.model_info.get("best_params") or {}).get("mode")

        mode = str(self.model_info.get("mode") or legacy_mode or "window").strip().lower()
        if mode not in ("point", "window"):
            AppLogger.error("unknown inference mode '%s' in model config %s; using 'window'", mode, self.path)
            return "window"
        return mode

    def _predict_point_level(self, model) -> list:
        """Point-level anomaly detection without windowing.

        Treats each time point independently, evaluates raw values or simple per-point features.
        """
        n = len(self.input_list)
        X = np.array(self.input_data_lists, dtype=float).T

        if X.shape[0] != n:
            AppLogger.error(
                "input_data_lists shape mismatch: expected %d points, got %d",
                n,
                X.shape[0],
            )
            raise ValueError("input_data_lists shape mismatch")

        # _build_model only returns a pkl-backed model, so it is trained
        # already: apply the pkl's preprocessing, then predict.
        pipeline_state = self.model_info.get("_pipeline_state")
        if pipeline_state:
            X = BaseModelAnomalyDetector._apply_preprocessing(X, pipeline_state)
        raw_preds = model.predict(X)

        point_codes = [self.valid_code if pred == 1 else -1 for pred in raw_preds]
        return point_codes

    def _predict_window_level(self, model) -> list:
        """Window-based anomaly detection with sliding window and feature extraction.

        Builds a feature matrix from sliding windows, then maps window predictions back to points.
        """
        feature_fns, window_size, stride = self._resolve_window_params()
        AppLogger.debug(
            "window config: window_size=%d, stride=%d, feature_fns=%s",
            window_size,
            stride,
            feature_fns,
        )

        n = len(self.input_list)

        if n < window_size:
            AppLogger.warning(
                "input length %d is shorter than window_size %d; all points marked valid",
                n,
                window_size,
            )
            return [self.valid_code] * n

        # Build sliding window feature matrix across all columns.
        # Each row concatenates per-column features (or raw values).
        # Shape: (n_windows, C * len(feature_fns)) or (n_windows, C * window_size).
        last_start = n - window_size
        starts = list(range(0, last_start + 1, stride))
        if starts[-1] != last_start:
            starts.append(last_start)
        rows = []
        for i in starts:
            row = []
            for col in self.input_data_lists:
                w = np.array(col[i : i + window_size], dtype=float)
                if feature_fns:
                    row.extend(self._extract_features(w, feature_fns))
                else:
                    row.extend(w.tolist())
            rows.append(row)
        X = np.array(rows, dtype=float)

        # _build_model only returns a pkl-backed model, so it is trained already.
        raw_preds = model.predict(X)

        # Map window predictions back to per-point codes.
        # A point is flagged anomalous when any window that covers it is anomalous.
        point_codes = [self.valid_code] * n
        for start, pred in zip(starts, raw_preds):
            if pred == -1:
                end = min(start + window_size, n)
                for j in range(start, end):
                    point_codes[j] = -1

        return point_codes

    def get_param(self) -> dict:
        info = self.model_info or {}
        return dict(info.get("best_params", {}))


class SVMModelDetector(BaseModelAnomalyDetector):
    """
    Anomaly detector using One-Class SVM for point-level detection.

    SVM models are ONLY loaded from pkl files (pre-trained).
    Does not support best_params construction or window-based detection.

    Expected config layout:
    {
      "algo": "svm",
      "model_path": "/path/to/trained_svm.pkl"
    }

    The pkl file must contain either:
      - Direct OneClassSVM object, or
      - Dict with keys: {"model": OneClassSVM, "pipeline": {...}}

    Pipeline state (optional) can include:
      - "fill_values": dict for NaN handling per column
      - "center": list for standardization mean
      - "scale": list for standardization std dev

    Inference flow:
      1. Load OneClassSVM from pkl
      2. Apply preprocessing from pipeline state if available (fillna, standardization)
      3. Evaluate each point: model.predict(X)
      4. Return per-point anomaly codes (valid_code or -1)

    Note: SVM is designed for multi-dimensional point-level anomaly detection,
    not time-series pattern detection. Use IsolationForest for window-based detection.
    """

    target_algo = "SVM"

    def _build_model(self):
        from sklearn.svm import OneClassSVM

        model_path = self.model_info.get("model_path")
        if not model_path:
            AppLogger.error(
                "SVM models require model_path (pkl file); best_params not supported"
            )
            return None

        model, pipeline_state = BaseModelAnomalyDetector._load_pkl_model(
            model_path, OneClassSVM
        )
        if model is not None:
            self.model_info["_pipeline_state"] = pipeline_state
            AppLogger.info("loaded OneClassSVM from pkl file: %s", model_path)
            return model

        return None

    def _predict(self, model) -> list:
        """Point-level anomaly detection without windowing.
        Each point is evaluated independently as a multi-dimensional vector.
        """
        n = len(self.input_list)

        X = np.array(self.input_data_lists, dtype=float).T

        if X.shape[0] != n:
            AppLogger.error(
                "input_data_lists shape mismatch: expected %d points, got %d",
                n,
                X.shape[0],
            )
            raise ValueError("input_data_lists shape mismatch")

        # Apply pipeline preprocessing if available
        pipeline_state = self.model_info.get("_pipeline_state")
        if pipeline_state:
            X = BaseModelAnomalyDetector._apply_preprocessing(
                X, pipeline_state
            )

        raw_preds = model.predict(X)
        point_codes = [self.valid_code if pred == 1 else -1 for pred in raw_preds]
        return point_codes

    def get_param(self) -> dict:
        info = self.model_info or {}
        pipeline_state = info.get("_pipeline_state", {})
        return dict(pipeline_state)
