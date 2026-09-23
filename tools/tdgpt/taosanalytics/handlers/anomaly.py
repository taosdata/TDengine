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

"""anomaly handlers: encapsulates anomaly detection business logic"""

from taosanalytics.algo.anomaly import do_ad_check
from taosanalytics.log import AppLogger
from taosanalytics.util import do_check_before_exec, get_algo, parse_options


def handle_anomaly(request):
    """
    Execute anomaly detection business logic.

    Parses the incoming request, validates parameters, runs the anomaly
    detection algorithm, and returns the result dict.

    :param request: Flask request object
    :return: dict with anomaly detection result or error information
    """

    try:
        req_json, payload, options, data_index, ts_index = do_check_before_exec(
            request, True
        )
    except Exception as e:
        AppLogger.error("failed to do anomaly-detection, %s", str(e))
        return {"msg": str(e), "rows": -1}

    try:
        algo = get_algo(req_json, "ksigma")
        ts_list = payload[ts_index].copy()
        payload.pop(ts_index)
    except (IndexError, TypeError, ValueError) as e:
        return {"msg": str(e), "rows": -1}

    params = parse_options(options)

    try:
        _, ano_window, mask_list = do_ad_check(payload, ts_list, algo, params)
        result = {
            "algo": algo,
            "option": options,
            "res": ano_window,
            "rows": len(ano_window),
            "mask": mask_list,
        }

        AppLogger.debug("anomaly-detection result: %s", str(result))
        return result
    except Exception as e:
        result = {"res": {}, "rows": -1, "msg": str(e)}
        AppLogger.error("failed to do anomaly-detection, %s", str(e))
        return result
