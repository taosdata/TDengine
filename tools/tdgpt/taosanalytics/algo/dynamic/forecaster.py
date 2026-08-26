import json
from abc import ABC, abstractmethod

import numpy as np
import pandas as pd
from prophet import Prophet
from statsmodels.tsa.arima.model import ARIMA

from taosanalytics.log import AppLogger


class BaseModelForecaster(ABC):
    """
    dynamic loader for time series forecasting models based on config files.
        - Reads model configuration from a JSON file
        - Validates that the config describes the expected algorithm
        - Validates that the input dataset has required columns (ts, y)
        - Builds the model using algorithm-specific logic
        - Provides a unified forecast() method to get predictions
        - Designed for extensibility: new algorithms can be supported by subclassing and implementing the abstract
    """

    target_algo: str = ""
    required_columns = {"ts", "y"}

    def __init__(self, path: str, df: pd.DataFrame, horizon: int = 10, **kwargs):
        self.path = path
        self.df = df
        self.horizon = horizon
        self.kwargs = kwargs
        self.model_info: dict | None = None
        self._model = None
        self._freq: str | None = None

        # default confidence level for prediction intervals
        self.alpha = kwargs.get("alpha", 0.05)

    def build(self):
        self.model_info = self._load_config()
        if not self.model_info:
            return None

        if not self._is_expected_algo():
            AppLogger.error(
                f"Config file does not describe a {self.target_algo.upper()} model, skipping"
            )
            return None

        if not self._has_required_columns():
            AppLogger.error(
                "Dataset must contain ts and y columns, aborting reconstruction"
            )
            return None

        try:
            self._model = self._build_model()
        except Exception as e:
            raise RuntimeError(
                f"failed to build {self.target_algo} model from {self.path}: {e}"
            ) from e
        return self._model

    def forecast(self):
        model = self._model or self.build()
        if model is None:
            return None
        return self._predict(model)

    def _load_config(self):
        try:
            with open(self.path, "r", encoding="utf-8") as handle:
                return json.load(handle)
        except FileNotFoundError:
            AppLogger.error("Model config not found: %s", self.path)
            return None

    def _is_expected_algo(self):
        algo = (self.model_info.get("algo") or "").upper()
        return algo == self.target_algo

    def _has_required_columns(self):
        return self.required_columns.issubset(self.df.columns)

    def _get_pytorch_subdir_path(self, filename: str) -> str | None:
        """Resolve model file path in pytorch/ subdirectory relative to config file.

        Config structure:
        /path/to/model/config.json
        /path/to/model/pytorch/weights.pt
        /path/to/model/pytorch/config.json

        Returns absolute path if file exists, None otherwise.
        """
        from pathlib import Path
        config_dir = Path(self.path).parent
        pytorch_file = config_dir / "pytorch" / filename
        if pytorch_file.exists():
            return str(pytorch_file)
        return None

    @abstractmethod
    def _build_model(self):
        """Return the fitted model ready for inference."""

    @abstractmethod
    def _predict(self, model) -> pd.DataFrame | None:
        """Run algorithm-specific prediction and return the forecast payload."""

    @abstractmethod
    def get_param(self) -> dict:
        """get the param for current model"""
        return {}


class ArimaModelForecaster(BaseModelForecaster):
    """
    arima model reconstructor based on config file. The config file should like this:
    {
        "algo": "arima",
        "best_params": {
            "p": 3,
            "d": 0,
            "q": 2,
            "P": 2,
            "D": 1,
            "Q": 2
        },
        "freq": "MS",
        "model_path": "/usr/local/taos/tdmodel/model/trn_dbf7c3931f5b49fb8ed28034a76b3008.pkl",
        "target_metric": "RMSE",
        "dataset_id": "ds_0100564fe1224580801727ffe2309ddd",
        "seasonal_order_s": 12
    }
    """

    target_algo = "ARIMA"

    def _build_model(self):
        best_param = self.model_info.get("best_params", {})
        if not best_param:
            AppLogger.error("Missing best_params, cannot build ARIMA model")
            return None

        if not {"p", "d", "q"}.issubset(best_param.keys()):
            AppLogger.error("best_params missing p/d/q, cannot build ARIMA model")
            return None

        param = {"order": (best_param["p"], best_param["d"], best_param["q"])}

        seasonal_order_s = self.model_info.get("seasonal_order_s", 0)
        if seasonal_order_s:
            if not {"P", "D", "Q"}.issubset(best_param.keys()):
                AppLogger.error(
                    "best_params missing P/D/Q, cannot build seasonal ARIMA model"
                )
                return None
            param["seasonal_order"] = (
                best_param["P"],
                best_param["D"],
                best_param["Q"],
                seasonal_order_s,
            )
        else:
            param["seasonal_order"] = (0, 0, 0, 0)

        freq = self.model_info.get("freq", "MS")
        series = self.df.set_index("ts")["y"].asfreq(freq)

        model = ARIMA(
            series, order=param["order"], seasonal_order=param["seasonal_order"]
        )

        return model.fit()

    def _predict(self, model):
        forecast_vals = model.get_forecast(steps=self.horizon)
        df_res = forecast_vals.summary_frame(alpha=self.alpha)  # 95% 置信区间

        return pd.DataFrame(
            {
                "ts": df_res.index,
                "yhat": df_res["mean"].values,
                "yhat_lower": df_res["mean_ci_lower"].values,
                "yhat_upper": df_res["mean_ci_upper"].values,
            }
        )

    def get_param(self) -> dict:
        best_param = self.model_info.get("best_params", {})
        best_param["seasonal_order_s"] = self.model_info.get("seasonal_order_s", 0)

        best_param["freq"] = self.model_info.get("freq", "MS")
        return best_param


class ProphetModelForecaster(BaseModelForecaster):
    """
    Prophet model reconstructor based on config file. The config file should contain:
    {
        "algo": "PROPHET",
        "best_params": {  "changepoint_prior_scale": 0.01, "seasonality_mode": "multiplicative" },
        "freq": "D"
    }
    """

    target_algo = "PROPHET"

    def _build_model(self):
        best_params = self.model_info.get("best_params") or {}
        if not isinstance(best_params, dict):
            AppLogger.error(
                "best_params missing or invalid, cannot build Prophet model"
            )
            return None

        freq = self.model_info.get("freq", "D")
        self.kwargs["freq"] = freq

        prophet_df = self.df.rename(columns={"ts": "ds"}).copy()
        prophet_df["ds"] = pd.to_datetime(prophet_df["ds"])
        prophet_df = prophet_df[["ds", "y"]]

        model = Prophet(**best_params)
        model.fit(prophet_df)
        return model

    def _predict(self, model):
        freq = self.kwargs.get("freq", "D")
        future_df = model.make_future_dataframe(periods=self.horizon, freq=freq)
        forecast = model.predict(future_df)

        return forecast[["ds", "yhat", "yhat_lower", "yhat_upper"]]

    def get_param(self) -> dict:
        best_params = self.model_info.get("best_params") or {}
        best_params["freq"] = self.model_info.get("freq", "D")
        return best_params


class DeepARModelForecaster(BaseModelForecaster):
    """
    DeepAR model reconstructor using GluonTS and PyTorch.

    The model files are resolved as follows:
    1. If model_path/config_path are explicitly set in config, use them
    2. Otherwise, search in pytorch/ subdirectory relative to config file:
       Config file: /path/to/model/model_config.json
       Model files: /path/to/model/pytorch/weights.pt
                    /path/to/model/pytorch/config.json

    Example config:
    {
        "algo": "deepar",
        "best_params": {"num_layers": 1, "hidden_size": 256, ...},
        "freq": "1D",
        "prediction_length": 12
    }
    """

    target_algo = "DEEPAR"

    def _build_model(self):
        import torch
        from gluonts.torch import DeepAREstimator

        model_path = self.model_info.get("model_path")
        config_path = self.model_info.get("config_path")

        # If paths not explicitly provided, resolve from pytorch/ subdirectory
        if not model_path:
            model_path = self._get_pytorch_subdir_path("weights.pt")
        if not config_path:
            config_path = self._get_pytorch_subdir_path("config.json")

        if not model_path:
            raise RuntimeError(
                "DeepAR requires model_path (PyTorch .pth/.pt file). "
                "Either set model_path in config or place weights.pt in pytorch/ subdirectory"
            )

        # Load model config from pytorch/ subdirectory
        cfg = {}
        if config_path:
            try:
                with open(config_path, "r", encoding="utf-8") as f:
                    cfg = json.load(f)
            except Exception as e:
                AppLogger.warning("Could not load config from %s: %s", config_path, e)

        freq_cfg = cfg.get("freq")
        freq_info = self.model_info.get("freq")
        if freq_cfg and freq_info and freq_cfg != freq_info:
            raise RuntimeError(
                "freq mismatch: pytorch/config.json has %r but main config has %r"
                % (freq_cfg, freq_info)
            )
        freq = freq_cfg or freq_info or "D"
        self._freq = freq
        prediction_length = cfg.get("prediction_length") or self.model_info.get("prediction_length", 12)

        if self.horizon > prediction_length:
            raise RuntimeError(
                f"requested forecast rows ({self.horizon}) exceeds DeepAR model's "
                f"prediction_length ({prediction_length})"
            )

        estimator = DeepAREstimator(
            freq=freq,
            prediction_length=prediction_length,
            context_length=cfg.get("context_length", self.model_info.get("context_length", 2 * prediction_length)),
            num_layers=cfg.get("num_layers", self.model_info.get("num_layers", 2)),
            hidden_size=cfg.get("hidden_size", self.model_info.get("hidden_size", 128)),
            dropout_rate=cfg.get("dropout_rate", self.model_info.get("dropout_rate", 0.1)),
            batch_size=cfg.get("batch_size", self.model_info.get("batch_size", 32)),
            num_feat_dynamic_real=cfg.get("num_feat_dynamic_real", 0),
            num_parallel_samples=cfg.get("num_parallel_samples", 100),
            lr=cfg.get("learning_rate", self.model_info.get("learning_rate", 0.001)),
        )

        module = estimator.create_lightning_module()
        AppLogger.info("Loading DeepAR model from %s", model_path)
        state_dict = torch.load(model_path, map_location="cpu", weights_only=True)
        module.load_state_dict(state_dict)
        module.eval()

        transformation = estimator.create_transformation()
        predictor = estimator.create_predictor(transformation, module)

        AppLogger.info("Successfully loaded DeepAR model from %s", model_path)
        return predictor

    def _predict(self, model):
        """Generate predictions using DeepAR model (PyTorchPredictor)."""
        from gluonts.dataset.pandas import PandasDataset

        if not self._has_required_columns():
            raise RuntimeError("input data must have 'ts' and 'y' columns")

        freq = self._freq or self.model_info.get("freq", "D")
        ts_index = pd.to_datetime(self.df["ts"])

        inferred_freq = pd.infer_freq(ts_index)
        if inferred_freq is not None and inferred_freq != freq:
            raise RuntimeError(
                "input data freq %r does not match model training freq %r"
                % (inferred_freq, freq)
            )

        target_series = pd.Series(self.df["y"].values, index=ts_index)
        target_series.index.freq = pd.tseries.frequencies.to_offset(freq)

        dataset = PandasDataset({"target": target_series})
        forecasts = list(model.predict(dataset))

        if not forecasts:
            raise RuntimeError("no predictions generated by DeepAR model")

        forecast = forecasts[0]
        q_lo = self.alpha / 2
        q_hi = 1.0 - self.alpha / 2
        results = []
        for step_idx in range(forecast.samples.shape[1]):
            step_samples = forecast.samples[:, step_idx]
            results.append({
                "yhat": float(np.quantile(step_samples, 0.5)),
                "yhat_lower": float(np.quantile(step_samples, q_lo)),
                "yhat_upper": float(np.quantile(step_samples, q_hi)),
            })

        last_ts = ts_index.max()
        future_ts = pd.date_range(start=last_ts, periods=len(results) + 1, freq=freq)[1:]

        return pd.DataFrame({
            "ts": future_ts[:len(results)],
            "yhat": [r["yhat"] for r in results],
            "yhat_lower": [r["yhat_lower"] for r in results],
            "yhat_upper": [r["yhat_upper"] for r in results],
        })

    def get_param(self) -> dict:
        best_params = self.model_info.get("best_params", {})
        best_params["freq"] = self.model_info.get("freq", "D")
        best_params["prediction_length"] = self.model_info.get("prediction_length", 12)
        return best_params

