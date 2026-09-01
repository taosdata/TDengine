"""Shared model loading utilities for pkl-based models."""

import numpy as np

from taosanalytics.log import AppLogger


class ModelLoader:
    """Utilities for loading and preprocessing pkl-based models."""

    @staticmethod
    def load_pkl_model(model_path: str, expected_type) -> tuple:
        """Load pkl file and extract model + pipeline_state.

        Returns:
            (model, pipeline_state) tuple, or (None, None) on failure
        """
        import joblib

        try:
            AppLogger.info("start to load model from %s", model_path)
            data = joblib.load(model_path)

            # Handle both formats: direct model or dict with 'model' key
            if isinstance(data, dict) and "model" in data:
                model = data["model"]
                pipeline_state = data.get("pipeline", {})
                if not isinstance(model, expected_type):
                    AppLogger.error(
                        "loaded model from dict is not %s instance, got type: %s",
                        expected_type.__name__,
                        type(model).__name__,
                    )
                    return None, None
                return model, pipeline_state
            elif isinstance(data, expected_type):
                return data, {}
            else:
                AppLogger.error(
                    "loaded data is not %s instance or valid dict format, got type: %s",
                    expected_type.__name__,
                    type(data).__name__,
                )
                return None, None
        except FileNotFoundError:
            AppLogger.error("model pkl file not found at %s", model_path)
            return None, None
        except Exception as e:
            AppLogger.error(
                "failed to load model from pkl file %s: %s", model_path, str(e)
            )
            return None, None

    @staticmethod
    def load_pt_model(model_path: str) -> tuple:
        """Load PyTorch model from .pth or .pt file.

        Returns:
            (state_dict, {}) tuple, or (None, None) on failure
        """
        import torch

        try:
            AppLogger.info("start to load PyTorch model from %s", model_path)
            state_dict = torch.load(model_path, map_location="cpu", weights_only=True)

            # Handle both formats: direct state_dict or wrapped in dict
            if isinstance(state_dict, dict) and "model_state_dict" in state_dict:
                state_dict = state_dict["model_state_dict"]
            elif isinstance(state_dict, dict) and "state_dict" in state_dict:
                state_dict = state_dict["state_dict"]

            AppLogger.info("loaded PyTorch model state_dict with %d parameters", len(state_dict))
            return state_dict, {}
        except FileNotFoundError:
            AppLogger.error("PyTorch model file not found at %s", model_path)
            return None, None
        except Exception as e:
            AppLogger.error(
                "failed to load PyTorch model from %s: %s", model_path, str(e)
            )
            return None, None

    @staticmethod
    def apply_pipeline_preprocessing(
        X: np.ndarray, pipeline_state: dict
    ) -> np.ndarray:
        """Apply preprocessing from pipeline state (normalization, fillna, etc.).

        Pipeline state may contain:
        - fill_values: dict mapping column index to fill value for NaN
        - center/scale: for standardization
        """
        X = np.array(X, dtype=float, copy=True)

        # Handle missing values
        fill_values = pipeline_state.get("fill_values", {})
        if fill_values:
            for col_idx, fill_val in fill_values.items():
                if isinstance(col_idx, int) and col_idx < X.shape[1]:
                    col_mask = np.isnan(X[:, col_idx])
                    X[col_mask, col_idx] = fill_val

        # Handle standardization (center/scale)
        center = np.array(pipeline_state.get("center"))
        scale = np.array(pipeline_state.get("scale"))
        if center is not None and scale is not None:
            X = (X - center) / (scale + 1e-8)

        return X
