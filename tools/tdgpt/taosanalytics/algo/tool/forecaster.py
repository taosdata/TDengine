import json
from abc import ABC, abstractmethod

import pandas as pd
from typing import Optional
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
        self.alpha = kwargs.get("alpha", 0.05)  # default confidence level for prediction intervals

    def build(self):
        self.model_info = self._load_config()
        if not self.model_info:
            return None

        if not self._is_expected_algo():
            AppLogger.error(f"Config file does not describe a {self.target_algo.upper()} model, skipping")
            return None

        if not self._has_required_columns():
            AppLogger.error("Dataset must contain ts and y columns, aborting reconstruction")
            return None

        self._model = self._build_model()
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
            AppLogger.error(f"Model config not found: {self.path}")
            return None

    def _is_expected_algo(self):
        algo = (self.model_info.get('algo') or '').upper()
        return algo == self.target_algo

    def _has_required_columns(self):
        return self.required_columns.issubset(self.df.columns)

    @abstractmethod
    def _build_model(self):
        """Return the fitted model ready for inference."""

    @abstractmethod
    def _predict(self, model) -> Optional[pd.DataFrame]:
        """Run algorithm-specific prediction and return the forecast payload."""

    @abstractmethod
    def get_param(self) -> dict:
        """ get the param for current model """
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
        best_param = self.model_info.get('best_params', {})
        if not best_param:
            AppLogger.error("Missing best_params, cannot build ARIMA model")
            return None

        if not {'p', 'd', 'q'}.issubset(best_param.keys()):
            AppLogger.error("best_params missing p/d/q, cannot build ARIMA model")
            return None

        param = {'order': (best_param['p'], best_param['d'], best_param['q'])}

        seasonal_order_s = self.model_info.get('seasonal_order_s', 0)
        if seasonal_order_s:
            if not {'P', 'D', 'Q'}.issubset(best_param.keys()):
                AppLogger.error("best_params missing P/D/Q, cannot build seasonal ARIMA model")
                return None
            param['seasonal_order'] = (best_param['P'], best_param['D'], best_param['Q'], seasonal_order_s)
        else:
            param['seasonal_order'] = (0, 0, 0, 0)

        freq = self.model_info.get('freq', 'MS')
        series = self.df.set_index('ts')['y'].asfreq(freq)

        model = ARIMA(series, order=param['order'], seasonal_order=param['seasonal_order'])

        return model.fit()

    def _predict(self, model):
        forecast_vals = model.get_forecast(steps=self.horizon)
        df_res = forecast_vals.summary_frame(alpha=self.alpha)  # 95% 置信区间

        return pd.DataFrame(
            {
                'ts': df_res.index,
                'yhat': df_res['mean'].values,
                'yhat_lower': df_res['mean_ci_lower'].values,
                'yhat_upper': df_res['mean_ci_upper'].values
            }
        )

    def get_param(self) -> dict:
        best_param = self.model_info.get('best_params', {})
        best_param['seasonal_order_s'] = self.model_info.get('seasonal_order_s', 0)

        best_param['freq'] = self.model_info.get('freq', 'MS')
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
        best_params = self.model_info.get('best_params') or {}
        if not isinstance(best_params, dict):
            AppLogger.error("best_params missing or invalid, cannot build Prophet model")
            return None

        freq = self.model_info.get('freq', 'D')
        self.kwargs['freq'] = freq

        prophet_df = self.df.rename(columns={'ts': 'ds'}).copy()
        prophet_df['ds'] = pd.to_datetime(prophet_df['ds'])
        prophet_df = prophet_df[['ds', 'y']]

        model = Prophet(**best_params)
        model.fit(prophet_df)
        return model

    def _predict(self, model):
        freq = self.kwargs.get('freq', 'D')
        future_df = model.make_future_dataframe(periods=self.horizon, freq=freq)
        forecast = model.predict(future_df)

        return forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']]

    def get_param(self) -> dict:
        best_params = self.model_info.get('best_params') or {}
        best_params['freq'] = self.model_info.get('freq', 'D')
        return best_params
