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

"""imputation: algo logic and request handler"""

import time

from matplotlib import pyplot as plt

from lib.conf import Configure
from lib.log import AppLogger
from lib.service_registry import loader
from lib.util import do_check_before_exec, get_algo, parse_options, parse_time_delta_string


def _get_us_factor(unit: str) -> int:
    """Returns the conversion factor to microseconds for a given time unit."""
    factors = {
        "us": 1,
        "ms": 1000,
        "s": 1000 * 1000,
        "m": 1000 * 1000 * 60,
        "h": 1000 * 1000 * 60 * 60,
        "d": 1000 * 1000 * 60 * 60 * 24,
    }

    if unit not in factors:
        raise ValueError(f"Unsupported unit: {unit}")

    return factors[unit]


def check_freq_param(ts_list: list, freq, prec):
    value, unit = parse_time_delta_string(freq)
    try:
        delta_in_us = _get_us_factor(unit) * value
    except ValueError:
        raise ValueError(f"Unsupported frequency: {freq}")

    if delta_in_us <= 0:
        raise ValueError(f"Invalid frequency: {freq}")

    try:
        _, prec_unit = parse_time_delta_string(prec)
        factor = _get_us_factor(prec_unit)
    except ValueError:
        raise ValueError(f"Unsupported precision: {prec}")

    rev_ts_list = [val * factor for val in ts_list]

    for i in range(1, len(ts_list)):
        if delta_in_us + rev_ts_list[i - 1] > rev_ts_list[i]:
            raise ValueError(
                f"invalid freq or precision for the input ts list, freq:{freq}, prec:{prec}, ts interval between: "
                f"{rev_ts_list[i - 1]} {rev_ts_list[i]}"
            )


def do_set_imputation_params(params, json_obj):
    """add params into parameters"""
    valid_precision_list = ["d", "h", "m", "s", "ms", "us", "ns"]
    if "prec" in json_obj:
        time_str = json_obj["prec"]

        _, unit = parse_time_delta_string(time_str)
        if unit not in valid_precision_list:
            raise ValueError(f"precision should be one of {valid_precision_list}")

        params["precision"] = time_str
    else:
        params["precision"] = "ms"

    valid_freq_dict = {"d": "D", "h": "H", "m": "T", "s": "S", "ms": "L", "us": "U"}
    if "freq" in json_obj:
        freq_str = json_obj["freq"]

        value, unit = parse_time_delta_string(freq_str)
        if unit not in valid_freq_dict:
            raise ValueError(f"freq should be one of {valid_freq_dict.keys()}")

        params["freq"] = str(value) + valid_freq_dict[unit]
    else:
        params["freq"] = "1L"


def draw_imputation_final_result(data, mask):
    if not Configure.get_instance().get_draw_result_option():
        return

    plt.clf()
    plt.plot(data, label="target", c="darkblue")

    for index, val in enumerate(mask):
        if val == 1:
            plt.scatter(
                index, data[index], marker="o", color="r", alpha=0.5, s=100, zorder=3
            )

    plt.legend(fontsize=14)
    plt.savefig("imputation_res.png")
    plt.close()


def do_imputation(input_list, ts_list, algo_name, params):
    """data imputation handler"""
    s = loader.get_service(algo_name)

    if s is None:
        s = loader.get_service("moment")

    if s is None:
        raise ValueError(f"failed to load {algo_name} analysis service")

    s.set_input_data(input_list, ts_list)
    s.set_params(params)

    start = time.time()
    AppLogger.debug("start to do imputation")

    res = s.execute()

    AppLogger.debug(
        "imputation done, elapsed time:%.2fms", (time.time() - start) * 1000
    )

    res["algo"] = algo_name
    draw_imputation_final_result(res["target"], res["mask"])

    return res


def handle_imputation(request):
    """
    Execute imputation business logic.

    :param request: Flask request object
    :return: dict with imputation result or error information
    """

    try:
        req_json, payload, options, data_index, ts_index = do_check_before_exec(request)
    except Exception as e:
        return {"msg": str(e), "rows": -1}

    params = parse_options(options)

    try:
        do_set_imputation_params(params, req_json)
    except ValueError as e:
        AppLogger.error("invalid imputation params: %s", e)
        return {"msg": f"{e}", "rows": -1}

    try:
        algo = get_algo(req_json, "moment")
        freq = req_json["freq"] if "freq" in req_json else "1ms"
        prec = req_json["prec"] if "prec" in req_json else "ms"
        check_freq_param(payload[ts_index], freq, prec)

        imputat_res = do_imputation(
            payload[data_index], payload[ts_index], algo, params
        )

        final_res = {"option": options, "rows": len(imputat_res["ts"])}
        final_res.update(imputat_res)

        AppLogger.debug("imputation result: %s", final_res)
        return final_res
    except Exception as e:
        AppLogger.error("imputation failed, %s", str(e))
        return {"msg": str(e), "rows": -1}
