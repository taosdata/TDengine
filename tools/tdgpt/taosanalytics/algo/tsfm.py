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

import json

import requests

from taosanalytics.algo.forecast import insert_ts_list
from taosanalytics.base import AbstractForecastService, AnalyticsService
from taosanalytics.conf import Configure
from taosanalytics.log import AppLogger


class TsfmBaseService(AbstractForecastService):
    """tsfm base service class"""

    _builtins = True

    def __init__(self):
        super().__init__()
        self.headers = {"Content-Type": "application/json"}
        self.service_host = Configure.get_instance().get_tsfm_service(self.name)

    def execute(self):
        if self.list is None or len(self.list) < self.period:
            raise ValueError("number of input data is less than the periods")

        if self.rows <= 0:
            raise ValueError("forecast rows is not specified yet")

        # let's request the gpt service
        data = {
            "input": self.list,
            "ts": self.ts_list,
            "next_len": self.rows,
            "past_dynamic_real": self.past_dynamic_real,
            "dynamic_real": self.dynamic_real,
            "conf_interval": self.conf,
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
            raise ValueError("failed to connect to the upstream forecast service")

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

        if self.return_conf == 0:
            res = {
                "res": [resp_json["output"]],
                "conf": 0,
            }
        else:
            if resp_json.get("upper") is None:
                res = {
                    "res": [
                        resp_json["output"],
                        resp_json["output"],
                        resp_json["output"],
                    ],
                    "conf": self.conf,
                }
            else:  # return data with lower/upper interval value
                res = {
                    "res": [
                        resp_json["output"],
                        resp_json["lower"],
                        resp_json["upper"],
                    ],
                    "conf": self.conf,
                }

        insert_ts_list(res["res"], self.start_ts, self.time_step, self.rows)
        return res

    def set_params(self, params):
        super().set_params(params)

        AppLogger.info(
            "%s gpt host service: %s",
            self.__class__.__name__,
            self.service_host,
        )

    def get_status(self) -> str:
        if self.service_host is None:
            AppLogger.error("tsfm service host is not specified yet")
            return AnalyticsService._toStatusName[AnalyticsService.UNAVAILABLE]

        try:
            _ = requests.get(self.service_host, headers=self.headers, timeout=5)
        except Exception as e:
            AppLogger.error(
                "failed to connect the service: %s %s", self.service_host, str(e)
            )
            return AnalyticsService._toStatusName[AnalyticsService.UNAVAILABLE]

        return super().get_status()
