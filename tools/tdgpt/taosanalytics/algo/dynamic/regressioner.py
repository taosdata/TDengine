import json
from abc import ABC, abstractmethod

import numpy as np

from taosanalytics.algo.dynamic.detector import BaseModelAnomalyDetector
from taosanalytics.log import AppLogger


class BaseModelRegressioner(ABC):
    """
    Dynamic loader for regression models driven by a JSON config file.

    Responsibilities:
      - Load and validate the config file
      - Confirm the config describes the expected algorithm
      - Load/build the model via _build_model()
      - Return predicted values via predict() → list[float]

    Expected input: Feature matrix (n_samples, n_features)
    Expected output: Prediction values (n_samples,)
    """

    target_algo: str = ""

    def __init__(self, path: str, input_data: list, schema: list = None):
        self.path = path
        self.input_data = input_data  # Feature matrix: list of lists
        self.schema = schema  # Column metadata
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

    def predict(self) -> list:
        """Run regression and return predicted values."""
        model = self._model or self.build()
        if model is None:
            AppLogger.error("model unavailable for regression: %s", self.path)
            raise RuntimeError(f"regression model unavailable: {self.path}")
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

    def _load_from_pkl(self, model_path: str, expected_type):
        """Load model from pkl file, store pipeline_state in model_info."""
        model, pipeline_state = BaseModelAnomalyDetector._load_pkl_model(
            model_path, expected_type
        )
        if model is not None:
            self.model_info["_pipeline_state"] = pipeline_state
            AppLogger.info(
                "loaded %s from pkl file: %s", expected_type.__name__, model_path
            )
        return model

    def _apply_preprocessing(self, X: np.ndarray) -> np.ndarray:
        """Apply pipeline preprocessing if pipeline_state is available."""
        pipeline_state = self.model_info.get("_pipeline_state")
        if pipeline_state:
            X = BaseModelAnomalyDetector._apply_pipeline_preprocessing(
                X, pipeline_state
            )
        return X

    def _to_prediction_list(self, model, X: np.ndarray) -> list:
        """Run model.predict() and return as list."""
        raw_preds = model.predict(X)
        return raw_preds.tolist() if hasattr(raw_preds, "tolist") else list(raw_preds)

    @abstractmethod
    def _build_model(self):
        """Load or build the model ready for inference."""

    @abstractmethod
    def _predict(self, model) -> list:
        """Run inference and return predicted values."""

    @abstractmethod
    def get_param(self) -> dict:
        """Return model parameters for logging / introspection."""


class LinearRegressioner(BaseModelRegressioner):
    """Linear regression — pkl only."""

    target_algo = "LINEAR"

    def _build_model(self):
        from sklearn.linear_model import LinearRegression

        model_path = self.model_info.get("model_path")
        if not model_path:
            AppLogger.error("LinearRegression requires model_path (pkl file)")
            return None
        return self._load_from_pkl(model_path, LinearRegression)

    def _predict(self, model) -> list:
        X = np.array(self.input_data, dtype=float)
        if X.ndim == 1:
            X = X.reshape(-1, 1)
        X = self._apply_preprocessing(X)
        return self._to_prediction_list(model, X)

    def get_param(self) -> dict:
        info = self.model_info or {}
        return dict(info.get("_pipeline_state", {}))


class LassoRegressioner(BaseModelRegressioner):
    """Lasso regression — pkl only."""

    target_algo = "LASSO"

    def _build_model(self):
        from sklearn.linear_model import Lasso

        model_path = self.model_info.get("model_path")
        if not model_path:
            AppLogger.error("Lasso requires model_path (pkl file)")
            return None
        return self._load_from_pkl(model_path, Lasso)

    def _predict(self, model) -> list:
        X = np.array(self.input_data, dtype=float)
        if X.ndim == 1:
            X = X.reshape(-1, 1)
        X = self._apply_preprocessing(X)
        return self._to_prediction_list(model, X)

    def get_param(self) -> dict:
        info = self.model_info or {}
        return dict(info.get("_pipeline_state", {}))


class RidgeRegressioner(BaseModelRegressioner):
    """Ridge regression — pkl only."""

    target_algo = "RIDGE"

    def _build_model(self):
        from sklearn.linear_model import Ridge

        model_path = self.model_info.get("model_path")
        if not model_path:
            AppLogger.error("Ridge requires model_path (pkl file)")
            return None
        return self._load_from_pkl(model_path, Ridge)

    def _predict(self, model) -> list:
        X = np.array(self.input_data, dtype=float)
        if X.ndim == 1:
            X = X.reshape(-1, 1)
        X = self._apply_preprocessing(X)
        return self._to_prediction_list(model, X)

    def get_param(self) -> dict:
        info = self.model_info or {}
        return dict(info.get("_pipeline_state", {}))


class ElasticNetRegressioner(BaseModelRegressioner):
    """ElasticNet regression — pkl only."""

    target_algo = "ELASTICNET"

    def _build_model(self):
        from sklearn.linear_model import ElasticNet

        model_path = self.model_info.get("model_path")
        if not model_path:
            AppLogger.error("ElasticNet requires model_path (pkl file)")
            return None
        return self._load_from_pkl(model_path, ElasticNet)

    def _predict(self, model) -> list:
        X = np.array(self.input_data, dtype=float)
        if X.ndim == 1:
            X = X.reshape(-1, 1)
        X = self._apply_preprocessing(X)
        return self._to_prediction_list(model, X)

    def get_param(self) -> dict:
        info = self.model_info or {}
        return dict(info.get("_pipeline_state", {}))


class PolynomialRegressioner(BaseModelRegressioner):
    """Polynomial regression — pkl only.

    Expects pkl to contain a scikit-learn Pipeline with PolynomialFeatures
    as the first step (or equivalent feature transformer). The pipeline handles
    feature expansion internally, so _apply_preprocessing is skipped here.
    """

    target_algo = "POLYNOMIAL"

    def _build_model(self):
        from sklearn.pipeline import Pipeline

        model_path = self.model_info.get("model_path")
        if not model_path:
            AppLogger.error("PolynomialRegression requires model_path (pkl file)")
            return None
        return self._load_from_pkl(model_path, Pipeline)

    def _predict(self, model) -> list:
        X = np.array(self.input_data, dtype=float)
        if X.ndim == 1:
            X = X.reshape(-1, 1)
        X = self._apply_preprocessing(X)
        return self._to_prediction_list(model, X)

    def get_param(self) -> dict:
        info = self.model_info or {}
        return dict(info.get("_pipeline_state", {}))


class SVRRegressioner(BaseModelRegressioner):
    """Support Vector Regression — pkl only."""

    target_algo = "SVR"

    def _build_model(self):
        from sklearn.svm import SVR

        model_path = self.model_info.get("model_path")
        if not model_path:
            AppLogger.error("SVR requires model_path (pkl file)")
            return None
        return self._load_from_pkl(model_path, SVR)

    def _predict(self, model) -> list:
        X = np.array(self.input_data, dtype=float)
        if X.ndim == 1:
            X = X.reshape(-1, 1)
        X = self._apply_preprocessing(X)
        return self._to_prediction_list(model, X)

    def get_param(self) -> dict:
        info = self.model_info or {}
        return dict(info.get("_pipeline_state", {}))
