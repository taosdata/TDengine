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

"""Adapter skeleton: expose a user model as a TDgpt forecast algorithm.

Use this when the user's code does not inherit AbstractForecastService. Keep
this adapter class in the entry file and put the user's code in a helper
package next to it, for example ``my_algo_impl/``. Deploy both with
``--script my_algo.py --asset my_algo_impl``.
"""

import threading

import numpy as np

from lib.base import AbstractForecastService


class _UserForecastAdapter(AbstractForecastService):
    name = "user_fc"  # TODO: set to the requested algorithm name
    desc = "Adapter for a user-provided forecast model"

    def __init__(self):
        super().__init__()
        self._model = None
        self._lock = threading.Lock()
        # TODO: add typed parameters with defaults.

    def _load_model(self):
        """Build or load the user's model once, on first use."""
        if self._model is None:
            with self._lock:
                if self._model is None:
                    # TODO: import and construct the user's model here.
                    # from .my_algo_impl.user_model import load_model
                    # self._model = load_model()
                    raise NotImplementedError(
                        "wire the user's model in _load_model()"
                    )
        return self._model

    def set_params(self, params):
        # Required: the base class parses start_ts, time_step, rows, period,
        # conf and return_conf, and validates their ranges.
        super().set_params(params)
        # TODO: parse additional parameters and pass them to the model.

    def execute(self):
        values = np.asarray(self.list, dtype=float)
        if values.size < 2:
            raise ValueError("at least two input points are required")

        model = self._load_model()

        # TODO: call the user's model. It must return exactly self.rows
        # forecast points.
        forecast = np.asarray(
            model.forecast(values, self.rows), dtype=float
        ).ravel()
        if forecast.size != self.rows:
            raise ValueError(
                "user model returned %d points, expected %d"
                % (forecast.size, self.rows)
            )

        timestamps = [
            self.start_ts + index * self.time_step for index in range(self.rows)
        ]
        result = [timestamps, forecast.tolist()]

        if self.return_conf:
            # TODO: use the model's lower and upper bounds when available.
            width = np.zeros(self.rows, dtype=float)
            result.append((forecast - width).tolist())
            result.append((forecast + width).tolist())

        return {"mse": 0.0, "model_info": "user_model", "res": result}

    def get_params(self):
        params = super().get_params()
        # TODO: add the adapter's own parameters.
        return params
