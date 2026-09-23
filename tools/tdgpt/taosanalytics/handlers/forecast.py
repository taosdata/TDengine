# Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
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

"""forecast handlers: encapsulates forecast business logic"""

from taosanalytics.algo.forecast import add_forecast_params, do_forecast
from taosanalytics.log import AppLogger
from taosanalytics.util import (
    do_check_before_exec,
    get_algo,
    get_dynamic_data,
    get_past_dynamic_data,
    parse_options,
)


def handle_forecast(request):
    """
    Execute forecast business logic.

    Parses the incoming request, validates parameters, runs the forecast
    algorithm, and returns the result dict.

    :param request: Flask request object
    :return: dict with forecast result or error information
    """

    try:
        req_json, payload, options, data_index, ts_index = do_check_before_exec(request)
    except Exception as e:
        AppLogger.error("failed to parse forecast request: %s", str(e))
        return {"msg": str(e), "rows": -1}

    params = parse_options(options)

    try:
        add_forecast_params(params, req_json)
    except ValueError as e:
        AppLogger.error("invalid fc params: %s", e)
        return {"msg": f"{e}", "rows": -1}

    # holt-winters by default
    try:
        algo = get_algo(req_json, "holtwinters")
        res1 = do_forecast(
            payload[data_index],
            payload[ts_index],
            algo,
            params,
            get_past_dynamic_data(payload, req_json["schema"]),
            get_dynamic_data(payload, req_json["schema"]),
        )

        res = {"option": options, "rows": params.get("rows", 0)}
        res.update(res1)

        AppLogger.debug("forecast result: %s", res)
        return res
    except Exception as e:
        AppLogger.error("forecast failed, %s", str(e))
        return {"msg": str(e), "rows": -1}
