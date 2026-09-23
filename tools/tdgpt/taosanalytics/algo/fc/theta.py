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

"""theta definition"""

import numpy as np
from statsforecast.models import Theta

from taosanalytics.base import AbstractStatsForecastService


class _ThetaService(AbstractStatsForecastService):
    """Forecast time-series data using the Theta method."""

    name = "theta"
    desc = "forecast algorithm by using the standard Theta method"
    _builtins = True
    model_info = "Theta"

    def __init__(self):
        super().__init__()

        self.decomposition_type = "multiplicative"

    def set_params(self, params: dict) -> None:
        super().set_params(params)

        self.decomposition_type = str(
            params.get("decomposition_type", "multiplicative")
        ).lower()
        if self.decomposition_type not in ("additive", "multiplicative"):
            raise ValueError(
                "decomposition_type parameter can only be 'additive' or 'multiplicative'"
            )

    def get_params(self) -> dict:
        p = super().get_params()
        p.update({"decomposition_type": self.decomposition_type})
        return p

    def _validate_input_values(self, values: np.ndarray) -> None:
        """Validate Theta-specific input constraints."""
        if self.decomposition_type == "multiplicative" and np.any(values <= 0):
            raise ValueError(
                "multiplicative decomposition requires strictly positive input data"
            )

    def _fit_model(self, values: np.ndarray):
        """Fit and return the configured Theta model."""
        season_length = self.period if self.period > 0 else 1
        model = Theta(
            season_length=season_length, decomposition_type=self.decomposition_type
        )
        return model.fit(values)
