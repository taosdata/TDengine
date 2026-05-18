# encoding:utf-8
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
from typing import Optional

import numpy as np

from taosanalytics.log import AppLogger


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

    def __init__(self, path: str, input_list: list, ts_list: list = None, valid_code: int = 1,
                 input_data_lists: Optional[list] = None):
        self.path = path
        self.input_list = input_list
        self.ts_list = ts_list
        self.valid_code = valid_code
        # input_data_lists holds one sub-list per column (same layout as
        # AbstractAnomalyDetectionService.input_data_lists).  Falls back to
        # [input_list] when not provided so single-column callers are unaffected.
        self.input_data_lists: list = input_data_lists if input_data_lists is not None else [input_list]
        self.model_info: Optional[dict] = None
        self._model = None

    def build(self):
        self.model_info = self._load_config()
        if not self.model_info:
            return None

        if not self._is_expected_algo():
            AppLogger.error(
                "config does not describe a %s model (got algo=%s), skipping",
                self.target_algo, self.model_info.get('algo'))
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

    def _load_config(self) -> Optional[dict]:
        try:
            with open(self.path, "r", encoding="utf-8") as handle:
                return json.load(handle)
        except FileNotFoundError:
            AppLogger.error("model config not found: %s", self.path)
        except Exception as e:
            AppLogger.error("failed to load model config %s: %s", self.path, e)
        return None

    def _is_expected_algo(self) -> bool:
        algo = (self.model_info.get('algo') or '').upper().replace('-', '_')
        return algo == self.target_algo.upper().replace('-', '_')

    @abstractmethod
    def _build_model(self):
        """Load or build the model ready for inference."""

    @abstractmethod
    def _predict(self, model) -> list:
        """Run inference and return per-point anomaly codes (valid_code or -1)."""

    @abstractmethod
    def get_param(self) -> dict:
        """Return model parameters for logging / introspection."""


class IsolationForestModelDetector(BaseModelAnomalyDetector):
    """
    Anomaly detector that constructs an sklearn IsolationForest from best_params
    and scores the input via a sliding-window feature matrix.

    Expected config layout:
    {
      "algo": "iforest",
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

    Inference flow:
      1. Construct IsolationForest from best_params (n_estimators, max_samples,
         max_features, contamination).
      2. Slide a window of length window_size over each input column with the given
         stride.  For each window, compute the features listed in feature_fns
         (supported: "mean", "std", "slope") and concatenate the feature vectors of
         all columns.  If feature_fns is empty the raw window values are used instead.
         With C columns the matrix has shape
           (n_windows, C * len(feature_fns))  when feature_fns is set, or
           (n_windows, C * window_size)        otherwise.
      3. Call model.fit_predict() → sklearn convention: 1 = inlier, -1 = outlier.
      4. Map window-level predictions back to per-point codes:
         a point is anomalous if any window that covers it is anomalous.
    """
    target_algo = "IFOREST"

    def _build_model(self):
        from sklearn.ensemble import IsolationForest

        params = self.model_info.get('best_params', {})
        _IF_KEYS = {'n_estimators', 'max_samples', 'max_features', 'contamination', 'random_state'}
        if_params = {k: v for k, v in params.items() if k in _IF_KEYS}

        try:
            model = IsolationForest(**if_params)
            AppLogger.info("constructed IsolationForest with params: %s", if_params)
            return model
        except Exception as e:
            AppLogger.error("failed to construct IsolationForest: %s", e)
        return None

    @staticmethod
    def _extract_features(window: np.ndarray, feature_fns: list) -> list:
        """Compute a feature vector from a single window."""
        features = []
        for fn in feature_fns:
            if fn == 'mean':
                features.append(float(np.mean(window)))
            elif fn == 'std':
                features.append(float(np.std(window)))
            elif fn == 'slope':
                x = np.arange(len(window), dtype=float)
                features.append(float(np.polyfit(x, window, 1)[0]))
            else:
                AppLogger.warning("unknown feature function '%s', skipping", fn)
        return features

    @staticmethod
    def _validate_window_params(params: dict) -> tuple[int, int]:
        """Return validated sliding-window parameters from best_params."""
        try:
            window_size = int(params.get('window_size', 100))
            stride = int(params.get('stride', 1))
        except (TypeError, ValueError) as e:
            raise ValueError(
                "best_params.window_size and best_params.stride must be integers"
            ) from e

        if window_size <= 0 or stride <= 0:
            raise ValueError(
                "best_params.window_size and best_params.stride must be positive integers; "
                f"got window_size={window_size}, stride={stride}"
            )

        return window_size, stride

    def _predict(self, model) -> list:
        params = self.model_info.get('best_params', {})
        feature_fns = params.get('feature_fns', [])
        n = len(self.input_list)

        window_size, stride = self._validate_window_params(params)

        if n < window_size:
            AppLogger.warning(
                "input length %d is shorter than window_size %d; all points marked valid",
                n, window_size)
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
                w = np.array(col[i:i + window_size], dtype=float)
                if feature_fns:
                    row.extend(self._extract_features(w, feature_fns))
                else:
                    row.extend(w.tolist())
            rows.append(row)
        X = np.array(rows, dtype=float)

        # sklearn IsolationForest: 1 = inlier, -1 = outlier
        raw_preds = model.fit_predict(X)

        # Map window predictions back to per-point codes.
        # A point is flagged anomalous when any window covering it is anomalous.
        point_codes = [self.valid_code] * n
        for start, pred in zip(starts, raw_preds):
            if pred == -1:
                end = min(start + window_size, n)
                for j in range(start, end):
                    point_codes[j] = -1

        return point_codes

    def get_param(self) -> dict:
        info = self.model_info or {}
        return dict(info.get('best_params', {}))
