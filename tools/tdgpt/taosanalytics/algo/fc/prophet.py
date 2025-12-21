# encoding:utf-8
# pylint: disable=c0103
"""prophet class definition"""
from prophet import Prophet
import pandas as pd

from taosanalytics.algo.forecast import insert_ts_list
from taosanalytics.conf import app_logger
from taosanalytics.service import AbstractForecastService


class _ProphetService(AbstractForecastService):
    """Prophet algorithm to do forecast on input list"""
    name = "prophet"
    desc = "do time series data forecast using Facebook Prophet model"

    def __init__(self):
        super().__init__()

        self.growth = "linear"
        self.yearly_seasonality = "auto"
        self.weekly_seasonality = "auto"
        self.daily_seasonality = "auto"
        self.changepoint_prior_scale = 0.05

    def set_params(self, params):
        super().set_params(params)

        self.growth = params.get("growth", "linear")
        self.yearly_seasonality = params.get("yearly_seasonality", "auto")
        self.weekly_seasonality = params.get("weekly_seasonality", "auto")
        self.daily_seasonality = params.get("daily_seasonality", "auto")
        self.changepoint_prior_scale = float(params.get("changepoint_prior_scale", 0.05))

    def get_params(self):
        """ get the default value for fc algorithms """
        p = super().get_params()
        p.update(
            {
                "growth": self.growth,
                "yearly_seasonality": self.yearly_seasonality,
                "weekly_seasonality": self.weekly_seasonality,
                "daily_seasonality": self.daily_seasonality,
                "changepoint_prior_scale": self.changepoint_prior_scale
            }
        )
        return p

    def __do_forecast_helper(self, fc_rows):
        """ do Prophet forecast """
        if self.time_step is None:
            raise ValueError("time_step must be specified for Prophet")

        # Generate time index
        time_index = pd.date_range(
            start=pd.Timestamp(self.ts_list[0], unit=self.precision),
            periods=len(self.list),
            freq=pd.to_timedelta(self.time_step, unit=self.precision)
        )

        df = pd.DataFrame({"ds": time_index, "y": self.list})

        model = Prophet(
            growth=self.growth,
            yearly_seasonality=self.yearly_seasonality,
            weekly_seasonality=self.weekly_seasonality,
            daily_seasonality=self.daily_seasonality,
            changepoint_prior_scale=self.changepoint_prior_scale,
            interval_width=self.conf
        )

        model.fit(df)

        # Make future dataframe
        future = model.make_future_dataframe(
            periods=fc_rows,
            freq=pd.to_timedelta(self.time_step, unit=self.precision)
        )

        forecast = model.predict(future)

        # Extract forecasted yhat and confidence intervals
        fc_df = forecast.tail(fc_rows)
        yhat = fc_df["yhat"].tolist()
        yhat_lower = fc_df["yhat_lower"].tolist()
        yhat_upper = fc_df["yhat_upper"].tolist()

        res = [yhat, yhat_lower, yhat_upper] if self.return_conf else [yhat]

        return (
            res,
            None,  # Prophet does not expose MSE by default
            "Prophet"
        )

    def execute(self):
        """ do fc the time series data """
        if self.list is None or len(self.list) < 2:
            raise ValueError("number of input data is less than 2")

        if len(self.list) > 3000:
            raise ValueError("number of input data is too large")

        if self.rows <= 0:
            raise ValueError("fc rows is not specified yet")

        res, mse, model_info = self.__do_forecast_helper(self.rows)

        insert_ts_list(res, self.start_ts, self.time_step, self.rows)

        return {
            "mse": mse,
            "model_info": model_info,
            "res": res
        }

