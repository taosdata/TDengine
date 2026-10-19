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

"""complex exponential smoothing definition"""

from statsforecast.models import AutoCES

from lib.base import AbstractStatsForecastService


class _CesService(AbstractStatsForecastService):
    """Forecast time-series data using CES."""

    name = "ces"
    desc = "forecast algorithm by using complex exponential smoothing"
    _builtins = True
    model_info = "CES"

    def __init__(self):
        super().__init__()

        self.model = "Z"

    def set_params(self, params: dict) -> None:
        super().set_params(params)

        self.model = str(params.get("model", "Z")).upper()
        if self.model not in ("N", "S", "P", "F", "Z"):
            raise ValueError("model parameter can only be 'N', 'S', 'P', 'F' or 'Z'")

    def get_params(self) -> dict:
        p = super().get_params()
        p.update({"model": self.model})
        return p

    def _fit_model(self, values):
        """Fit and return the configured CES model."""
        season_length = self.period if self.period > 0 else 1
        model = AutoCES(season_length=season_length, model=self.model)
        return model.fit(values)
