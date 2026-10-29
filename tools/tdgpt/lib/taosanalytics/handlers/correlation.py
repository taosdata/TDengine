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

"""correlation: algo logic and request handler"""

from lib.log import AppLogger
from lib.service_registry import loader
from lib.util import (
    do_check_before_exec,
    get_algo,
    get_more_data_list,
    parse_options,
)


def do_dtw(input_list1: list, input_list2: list, params):
    s = loader.get_service("dtw")
    s.set_input_list(input_list1, None)
    s.set_second_input_data(input_list2)
    s.set_params(params)
    dist, path = s.execute()
    return dist, path


def do_tlcc(input_list1: list, input_list2: list, params):
    s = loader.get_service("tlcc")
    s.set_input_list(input_list1, None)
    s.set_second_input_data(input_list2)
    s.set_params(params)
    lags, ccf_vals = s.execute()
    return lags, ccf_vals


def handle_correlation(request):
    """
    Execute correlation business logic.

    :param request: Flask request object
    :return: dict with correlation result or error information
    """
    try:
        # check for rows limitation to reduce the dtw process time
        req_json, payload, options, data_index, ts_index = do_check_before_exec(
            request, False
        )
    except Exception as e:
        return {"msg": str(e), "rows": -1}

    params = parse_options(options)

    if "algo" not in req_json:
        return {"msg": "algo is missing in requests", "rows": -1}

    try:
        algo = get_algo(req_json, None)
        second_list = get_more_data_list(payload, req_json["schema"])

        if algo == "dtw":
            dist, path = do_dtw(payload[data_index], second_list, params)
            res = {"option": options, "rows": len(path), "distance": dist, "path": path}
            AppLogger.debug("dtw result: %s", res)
            return res
        elif algo == "tlcc":
            lags, ccf_vals = do_tlcc(payload[data_index], second_list, params)
            res = {
                "option": options,
                "rows": len(lags),
                "lags": lags,
                "ccf_vals": ccf_vals,
            }
            AppLogger.debug("tlcc result: %s", res)
            return res
        else:
            raise ValueError(f"unsupported algo: {algo}")
    except Exception as e:
        AppLogger.error("correlation failed, %s", str(e))
        return {"msg": str(e), "rows": -1}
