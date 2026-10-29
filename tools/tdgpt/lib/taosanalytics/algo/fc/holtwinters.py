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

"""holt winters definition"""

from statsmodels.tsa.holtwinters import ExponentialSmoothing, SimpleExpSmoothing

from lib.base import AbstractForecastService
from lib.util import insert_ts_list


class _HoltWintersService(AbstractForecastService):
    """Holt winters algorithm is to do the fc in the input list"""

    name = "holtwinters"
    desc = "forecast algorithm by using exponential smoothing"
    _builtins = True

    def __init__(self):
        super().__init__()

        self.trend_option = None
        self.seasonal_option = None

    def set_params(self, params):
        super().set_params(params)

        self.trend_option = params["trend"] if "trend" in params else None

        if self.trend_option is not None:
            if self.trend_option not in ("add", "mul"):
                raise ValueError("trend parameter can only be 'mul' or 'add'")

        self.seasonal_option = params["seasonal"] if "seasonal" in params else None
        if self.seasonal_option is not None:
            if self.seasonal_option not in ("add", "mul"):
                raise ValueError("seasonal parameter can only be 'mul' or 'add'")

    def get_params(self):
        p = super().get_params()
        p.update({"trend": self.trend_option, "seasonal": self.seasonal_option})
        return p

    def __do_forecast_helper(self, source_data, fc_rows):
        """do holt winters impl"""
        if self.trend_option is None and self.seasonal_option is None:
            fitted_model = SimpleExpSmoothing(source_data).fit()
        else:
            if self.period == 0 or self.seasonal_option is None:
                # no valid seasonal periods, so not need to specify the seasonal parameters
                fitted_model = ExponentialSmoothing(
                    source_data, trend=self.trend_option
                ).fit()
            else:  # seasonal attributes
                fitted_model = ExponentialSmoothing(
                    source_data,
                    trend=self.trend_option,
                    seasonal=self.seasonal_option,
                    seasonal_periods=self.period,
                ).fit(optimized=True, method="TNC")

        fc = fitted_model.forecast(fc_rows)

        if self.return_conf:
            return [fc.tolist(), fc.tolist(), fc.tolist()], fitted_model.sse
        else:
            return [fc.tolist()], fitted_model.sse

    def execute(self):
        """do fc the time series data"""
        if self.list is None or len(self.list) < self.period:
            raise ValueError("number of input data is less than the periods")

        if self.rows <= 0:
            raise ValueError("fc rows is not specified yet")

        res, mse = self.__do_forecast_helper(self.list, self.rows)
        insert_ts_list(res, self.start_ts, self.time_step, self.rows)

        # add the conf range if required
        return {"mse": mse, "res": res}
