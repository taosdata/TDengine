import json
from abc import ABC, abstractmethod

import numpy as np

from taosanalytics.algo.dynamic.model_loader import ModelLoader
from taosanalytics.log import AppLogger


class BaseModelClassifier(ABC):
    """
    Dynamic loader for classification models driven by a JSON config file.

    Responsibilities:
      - Load and validate the config file
      - Confirm the config describes the expected algorithm
      - Load/build the model via _build_model()
      - Return predicted values via predict() → list[int] or list[str]

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
        """Run classification and return predicted values."""
        model = self._model or self.build()
        if model is None:
            AppLogger.error("model unavailable for classification: %s", self.path)
            raise RuntimeError(f"classification model unavailable: {self.path}")
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
        model, pipeline_state = ModelLoader.load_pkl_model(
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
            X = ModelLoader.apply_pipeline_preprocessing(
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

    def _predict(self, model) -> list:
        """Run inference and return predicted values."""
        X = np.array(self.input_data, dtype=float)
        if X.ndim == 1:
            X = X.reshape(-1, 1)
        X = self._apply_preprocessing(X)
        return self._to_prediction_list(model, X)

    @abstractmethod
    def get_param(self) -> dict:
        """Return model parameters for logging / introspection."""


class LogisticRegressionClassifier(BaseModelClassifier):
    """Logistic Regression classifier — pkl only."""

    target_algo = "LOGISTIC_REGRESSION"

    def _build_model(self):
        from sklearn.linear_model import LogisticRegression

        model_path = self.model_info.get("model_path")
        if not model_path:
            AppLogger.error("LogisticRegression requires model_path (pkl file)")
            return None
        return self._load_from_pkl(model_path, LogisticRegression)

    def get_param(self) -> dict:
        info = self.model_info or {}
        return dict(info.get("_pipeline_state", {}))


class DecisionTreeClassifier(BaseModelClassifier):
    """Decision Tree classifier — pkl only."""

    target_algo = "DECISION_TREE"

    def _build_model(self):
        from sklearn.tree import DecisionTreeClassifier

        model_path = self.model_info.get("model_path")
        if not model_path:
            AppLogger.error("DecisionTreeClassifier requires model_path (pkl file)")
            return None
        return self._load_from_pkl(model_path, DecisionTreeClassifier)

    def get_param(self) -> dict:
        info = self.model_info or {}
        return dict(info.get("_pipeline_state", {}))
