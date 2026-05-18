# encoding:utf-8
"""DynamicForecastService: a forecast service driven by a parameter config file."""
import pandas as pd

from taosanalytics.algo.tool.forecaster import ArimaModelForecaster, ProphetModelForecaster
from taosanalytics.log import AppLogger
from taosanalytics.base import AbstractForecastService


class DynamicForecastService(AbstractForecastService):
    """
    a simple dynamic forecast service implementation for the model training with only parameters,
    the actual model can be loaded and executed when execute() is called.
    """

    def __init__(self, name: str, desc: str, algo: str, path: str):
        super().__init__()

        self.name = name
        self.desc = desc

        self.config_file_path = path
        self.algo = algo

    def execute(self):
        """ the actual model can be loaded and executed when execute() is called. """
        algo_name = self.algo.lower()
        AppLogger.info("execute dynamic forecast service:%s, algo:%s", self.name, algo_name)

        if algo_name == 'theta':
            raise NotImplementedError("Theta model is not implemented yet")

        if algo_name not in ('arima', 'prophet'):
            raise ValueError(f"unsupported algorithm '{algo_name}' in dynamic forecast service")

        # Build input DataFrame common to all forecast algorithms.
        try:
            datetime_list = pd.to_datetime(self.ts_list, unit=self.precision, utc=True)
            df = pd.DataFrame({
                'ts': datetime_list,
                'y': self.list,
            })
            df['ts'] = df['ts'].dt.tz_convert(self.tz)
        except Exception as e:
            msg = f"failed to prepare input data for {algo_name.upper()} model forecast: {e}"
            AppLogger.error(msg)
            raise RuntimeError(msg) from e

        if algo_name == 'arima':
            forecaster = ArimaModelForecaster(self.config_file_path, df, self.rows, alpha=1 - self.conf)
        else:
            # ProphetModelForecaster expects ts/y columns and renames ts→ds internally.
            # Strip the timezone from ts so Prophet receives tz-naive datetimes with
            # local time values already converted to the target tz.
            df_prophet = df.copy()
            df_prophet['ts'] = df_prophet['ts'].dt.tz_localize(None)
            forecaster = ProphetModelForecaster(
                self.config_file_path, df_prophet, self.rows, alpha=1 - self.conf)

        result = forecaster.forecast()

        if (result is None or
                not isinstance(result, pd.DataFrame) or
                not {'yhat', 'yhat_lower', 'yhat_upper'}.issubset(result.columns)):
            raise RuntimeError(
                f"failed to execute forecast with {algo_name.upper()} model forecaster "
                f"built from config file: {self.config_file_path}")

        if algo_name == 'prophet':
            # make_future_dataframe includes historical rows; keep only the future horizon.
            result = result.tail(self.rows).reset_index(drop=True)

        result_ts = [self.start_ts + i * self.time_step for i in range(self.rows)]
        res = [result_ts, result['yhat'].tolist()]
        if self.return_conf:
            res.append(result['yhat_lower'].tolist())
            res.append(result['yhat_upper'].tolist())

        return {
            "mse": None,
            "model_info": forecaster.get_param(),
            "res": res
        }
