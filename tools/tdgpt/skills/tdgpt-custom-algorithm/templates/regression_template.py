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

"""Example regression algorithm for TDgpt.

Copy this file to ``<custom_algo_dir>/regression/<algorithm_name>.py``, set
``name`` to the requested algorithm name, restart the TDgpt main service, then
call it with ``POST /regression``.

The input is a feature matrix: ``self.input_data`` is a list of rows. The
service must return one prediction per input row.
"""

import numpy as np

from lib.base import AbstractRegressionService


class _CustomRegressor(AbstractRegressionService):
    """Fit the last column from the preceding feature columns."""

    name = "custom_reg"
    desc = "Example linear regression"

    def __init__(self):
        super().__init__()
        self.degree = 1

    def execute(self):
        if not self.input_data:
            raise ValueError("input_data is required for regression")

        rows = np.asarray(self.input_data, dtype=float)
        if rows.ndim != 2 or rows.shape[1] < 2:
            raise ValueError(
                "regression expects at least two columns: features and target"
            )

        # TODO: replace this logic with the user's model. The last column is
        # treated as the target; the preceding columns are features.
        features = rows[:, :-1]
        target = rows[:, -1]

        if features.shape[1] == 1:
            coefficients = np.polyfit(features[:, 0], target, self.degree)
            predictions = np.polyval(coefficients, features[:, 0])
        else:
            design = np.column_stack([np.ones(len(features)), features])
            coefficients, *_ = np.linalg.lstsq(design, target, rcond=None)
            predictions = design @ coefficients

        return predictions.tolist()

    def set_params(self, params):
        """Map option strings to typed parameters for the user model."""
        super().set_params(params)
        if params and "degree" in params:
            try:
                degree = int(float(params["degree"]))
            except (TypeError, ValueError):
                raise ValueError("parameter 'degree' must be an integer")
            if degree < 1:
                raise ValueError("parameter 'degree' must be >= 1")
            self.degree = degree

    def get_params(self):
        return {"degree": self.degree}
