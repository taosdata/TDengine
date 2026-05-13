# encoding:utf-8
# pylint: disable=c0103
"""arima class definition"""
import pmdarima as pm

from taosanalytics.algo.forecast import insert_ts_list
from taosanalytics.conf import app_logger
from taosanalytics.service import AbstractForecastService


class _ArimaService(AbstractForecastService):
    """ ARIMA algorithm is to do the fc in the input list """
    name = "arima"
    desc = "do time series data fc by using ARIMA model"

    def __init__(self):
        super().__init__()

        self.diff = 0
        self.start_p = 0
        self.max_p = 10
        self.start_q = 0
        self.max_q = 10

    def set_params(self, params):
        super().set_params(params)

        self.start_p = int(params['start_p']) if 'start_p' in params else 0
        self.max_p = int(params['max_p']) if 'max_p' in params else 0
        self.start_q = int(params['start_q']) if 'start_q' in params else 0
        self.max_q = int(params['max_q']) if 'max_q' in params else 0

    def get_params(self):
        """ get the default value for fc algorithms """
        p = super().get_params()
        p.update(
            {
                "start_p": self.start_p, "max_p": self.max_p, "start_q": self.start_q,
                "max_q": self.max_q, "diff": self.diff
            }
        )

        return p

    def __do_forecast_helper(self, fc_rows):
        """ do arima fc """
        # plot_acf(self.list, lags=25, title='raw_acf')
        # plot_pacf(self.list, lags=25, title='raw_pacf')
        # plt.show()

        seasonal = self.period > 0

        # Fit model
        model = pm.auto_arima(self.list,
                              start_p=self.start_p,
                              start_q=self.start_q,
                              max_p=self.max_p,
                              max_q=self.max_q,
                              d=1,
                              m=self.period,
                              seasonal=seasonal,
                              start_P=0,
                              D=self.diff)

        app_logger.log_inst.debug(model.summary())

        # predict N steps into the future
        fc = model.predict(n_periods=fc_rows, return_conf_int=self.return_conf,
                           alpha=self.conf)

        res1 = [fc[0].tolist(), fc[1][:, 0].tolist(), fc[1][:, 1].tolist()] if self.return_conf else [fc.tolist()]

        return (
            res1,
            model.arima_res_.mse,
            f"SARIMAX{model.order}x{model.seasonal_order}"
        )

    def execute(self):
        """ do fc the time series data"""

        if self.list is None or len(self.list) < self.period:
            raise ValueError("number of input data is less than the periods")

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
