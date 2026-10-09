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

"""auto encoder algorithms to detect anomaly for time series data"""

import json

import requests

from taosanalytics.base import AbstractImputationService, AnalyticsService
from taosanalytics.conf import Configure
from taosanalytics.log import AppLogger


class _MomentImputationService(AbstractImputationService):
    """moment imputation service class"""

    name = "moment"
    desc = "Time-Series Foundation Model by CMU"
    _builtins = True

    def __init__(self):
        super().__init__()
        self.headers = {"Content-Type": "application/json"}
        self.service_host = Configure.get_instance().get_tsfm_service(self.name)

        # set the default frequency and time precision
        self.freq = "H"
        self.precision = "ms"

    def execute(self):
        # let's request the gpt service
        data = {
            "input": self.list,
            "ts": self.ts_list,
            "precision": self.precision,
            "freq": self.freq,
        }

        try:
            response = requests.post(
                self.service_host, data=json.dumps(data), headers=self.headers,
                timeout=Configure.get_instance().get_tsfm_exec_timeout(),
            )
        except requests.exceptions.RequestException:
            AppLogger.error(
                "failed to connect the service: %s", self.service_host, exc_info=True
            )
            raise ValueError("failed to connect to the upstream imputation service")

        if response.status_code == 404:
            AppLogger.error(f"failed to connect the service: {self.service_host}")
            raise ValueError("invalid host url")
        elif response.status_code != 200:
            AppLogger.error(
                f"failed to request the service: {self.service_host}, reason: {response.text}"
            )
            raise ValueError("upstream service returned an error")

        resp_json = response.json()
        AppLogger.debug(f"recv rsp, {resp_json}")

        return resp_json

    def set_params(self, params):
        super().set_params(params)

        AppLogger.info(
            "%s gpt host service: %s",
            self.__class__.__name__,
            self.service_host,
        )

        if "freq" in params:
            self.freq = params["freq"]

        if "precision" in params:
            self.precision = params["precision"]

        AppLogger.info(
            "%s specify freq: %s, precision: %s",
            self.__class__.__name__,
            self.freq,
            self.precision,
        )

    def get_status(self) -> str:
        if self.service_host is None:
            AppLogger.error("moment tsfm imputation service host is not specified yet")
            return AnalyticsService._toStatusName[AnalyticsService.UNAVAILABLE]

        try:
            _ = requests.get(self.service_host, headers=self.headers, timeout=5)
        except requests.exceptions.RequestException:
            AppLogger.error(
                "failed to connect the service: %s", self.service_host, exc_info=True
            )
            return AnalyticsService._toStatusName[AnalyticsService.UNAVAILABLE]

        return super().get_status()
