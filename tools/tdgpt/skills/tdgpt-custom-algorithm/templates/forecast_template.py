#!/usr/bin/env python3

# Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
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

"""Example univariate forecast algorithm for TDgpt.

Copy this file to ``<custom_algo_dir>/fc/<algorithm_name>.py``, set ``name`` to
the requested algorithm name, restart the TDgpt main service, then call it with
``POST /forecast`` or ``FORECAST(column, ...)``.

The base class parses ``start_ts``, ``time_step``, ``rows``, ``period``,
``conf`` and ``return_conf`` from the request. ``execute()`` must return
``{"mse", "model_info", "res"}`` where ``res`` is either
``[timestamps, forecast]`` or ``[timestamps, forecast, lower, upper]``.
"""

import math

import numpy as np

from lib.base import AbstractForecastService


class _CustomForecastService(AbstractForecastService):
    """Fit a linear trend on the most recent ``lookback`` points."""

    name = "custom_fc"
    desc = "Example linear-trend forecast algorithm"

    def __init__(self):
        super().__init__()
        self.lookback = 50

    def execute(self):
        values = np.asarray(self.list, dtype=float)
        if values.size < 2:
            raise ValueError("at least two input points are required")

        lookback = min(self.lookback, values.size)
        y = values[-lookback:]
        x = np.arange(y.size, dtype=float)
        slope, intercept = np.polyfit(x, y, 1)

        future_x = np.arange(y.size, y.size + self.rows, dtype=float)
        forecast = intercept + slope * future_x
        fitted = intercept + slope * x
        mse = float(np.mean((y - fitted) ** 2))

        timestamps = [
            self.start_ts + index * self.time_step for index in range(self.rows)
        ]
        result = [timestamps, forecast.tolist()]

        if self.return_conf:
            residual_std = float(np.std(y - fitted))
            if not math.isfinite(residual_std) or residual_std <= 0:
                residual_std = max(abs(float(np.mean(y))) * 1e-6, 1e-9)
            width = 1.96 * residual_std
            result.append((forecast - width).tolist())
            result.append((forecast + width).tolist())

        return {"mse": mse, "model_info": "linear_trend", "res": result}

    def set_params(self, params):
        # Required: the base class parses start_ts, time_step, rows, period,
        # conf and return_conf, and validates their ranges.
        super().set_params(params)
        if params and "lookback" in params:
            try:
                lookback = int(float(params["lookback"]))
            except (TypeError, ValueError):
                raise ValueError("parameter 'lookback' must be an integer")
            if lookback < 2:
                raise ValueError("parameter 'lookback' must be >= 2")
            self.lookback = lookback

    def get_params(self):
        params = super().get_params()
        params.update({"lookback": self.lookback})
        return params
