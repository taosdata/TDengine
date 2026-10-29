# Copyright (c) 2025 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

"""arima class definition"""

import pmdarima as pm

from lib.base import AbstractForecastService
from lib.log import AppLogger
from lib.util import insert_ts_list


class _ArimaService(AbstractForecastService):
    """ARIMA algorithm is to do the fc in the input list"""

    name = "arima"
    desc = "do time series data fc by using ARIMA model"
    _builtins = True

    def __init__(self):
        super().__init__()

        self.diff = 0
        self.start_p = 0
        self.max_p = 10
        self.start_q = 0
        self.max_q = 10

    def set_params(self, params):
        super().set_params(params)

        self.start_p = int(params["start_p"]) if "start_p" in params else 0
        self.max_p = int(params["max_p"]) if "max_p" in params else 0
        self.start_q = int(params["start_q"]) if "start_q" in params else 0
        self.max_q = int(params["max_q"]) if "max_q" in params else 0

    def get_params(self):
        """get the default value for fc algorithms"""
        p = super().get_params()
        p.update(
            {
                "start_p": self.start_p,
                "max_p": self.max_p,
                "start_q": self.start_q,
                "max_q": self.max_q,
                "diff": self.diff,
            }
        )

        return p

    def __do_forecast_helper(self, fc_rows):
        """do arima forecast"""
        # plot_acf(self.list, lags=25, title='raw_acf')
        # plot_pacf(self.list, lags=25, title='raw_pacf')
        # plt.show()

        seasonal = self.period > 0

        # Fit model
        model = pm.auto_arima(
            self.list,
            start_p=self.start_p,
            start_q=self.start_q,
            max_p=self.max_p,
            max_q=self.max_q,
            d=1,
            m=self.period,
            seasonal=seasonal,
            start_P=0,
            D=self.diff,
        )

        AppLogger.debug(model.summary())

        # predict N steps into the future
        forecast_res = model.predict(
            n_periods=fc_rows, return_conf_int=self.return_conf, alpha=self.conf
        )

        res1 = (
            [
                forecast_res[0].tolist(),
                forecast_res[1][:, 0].tolist(),
                forecast_res[1][:, 1].tolist(),
            ]
            if self.return_conf
            else [forecast_res.tolist()]
        )

        return (
            res1,
            model.arima_res_.mse,
            f"SARIMAX{model.order}x{model.seasonal_order}",
        )

    def execute(self):
        """do forecast the time series data"""
        if self.list is None or len(self.list) < self.period:
            raise ValueError("number of input data is less than the periods")

        if len(self.list) > 3000:
            raise ValueError("number of input data is too large")

        if self.rows <= 0:
            raise ValueError("forecast rows is not specified yet")

        res, mse, model_info = self.__do_forecast_helper(self.rows)
        insert_ts_list(res, self.start_ts, self.time_step, self.rows)

        return {"mse": mse, "model_info": model_info, "res": res}
