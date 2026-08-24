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

from taosanalytics.algo.dynamic.model_loader import ModelLoader
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
        """Return validated sliding-window parameters from best_params."""
        try:
            window_size = int(params.get("window_size", 100))
            stride = int(params.get("stride", 1))
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
        pipeline_state = self.model_info.get("_pipeline_state")
        inference_mode = self._get_inference_mode(pipeline_state)

        if inference_mode == "point":
            return self._predict_point_level(model)
        else:
            return self._predict_window_level(model)

    def _get_inference_mode(self, pipeline_state) -> str:
        """Determine inference mode: 'point' or 'window' (default)."""
        if pipeline_state:
            return pipeline_state.get("mode", "window")
        else:
            params = self.model_info.get("best_params", {})
            return params.get("mode", "window")

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

        # Apply pipeline preprocessing if available (normalization, fillna, etc.)
        pipeline_state = self.model_info.get("_pipeline_state")
        if pipeline_state:
            X = BaseModelAnomalyDetector._apply_preprocessing(
                X, pipeline_state
            )
            raw_preds = model.predict(X)
        else:
            raw_preds = model.fit_predict(X)

        point_codes = [self.valid_code if pred == 1 else -1 for pred in raw_preds]
        return point_codes

    def _predict_window_level(self, model) -> list:
        """Window-based anomaly detection with sliding window and feature extraction.

        Builds a feature matrix from sliding windows, then maps window predictions back to points.
        """
        pipeline_state = self.model_info.get("_pipeline_state")
        if pipeline_state:
            feature_fns = pipeline_state.get("feature_fns", [])
            window_size = int(pipeline_state.get("window_size", 100))
            stride = int(pipeline_state.get("stride", 1))
            AppLogger.debug(
                "using pipeline config from pkl: window_size=%d, stride=%d, feature_fns=%s",
                window_size,
                stride,
                feature_fns,
            )
        else:
            params = self.model_info.get("best_params", {})
            feature_fns = params.get("feature_fns", [])
            window_size, stride = self._validate_window_params(params)

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

        # Use predict() for pkl models (already trained), fit_predict() for best_params models (need training)
        if pipeline_state:
            raw_preds = model.predict(X)
        else:
            raw_preds = model.fit_predict(X)

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
