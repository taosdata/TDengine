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

"""forecast: algo logic and request handler"""

import os
import time

import numpy as np
import pandas as pd
from matplotlib import pyplot as plt

from lib.conf import Configure
from lib.log import AppLogger
from lib.service_registry import loader
from lib.util import (
    do_check_before_exec,
    get_algo,
    get_dynamic_data,
    get_past_dynamic_data,
    parse_options,
)


def do_forecast(
    input_list, ts_list, algo_name, params, past_dynamic_real=None, dynamic_real=None
):
    """data forecast handler"""
    s = loader.get_service(algo_name) or loader.get_service("holtwinters")
    if s is None:
        raise ValueError(f"failed to load {algo_name} or holtwinters analysis service")

    s.set_input_data(input_list, ts_list, past_dynamic_real, dynamic_real)
    s.set_params(params)

    start = time.time()
    AppLogger.debug("start to do forecast")

    res = s.execute()

    AppLogger.debug(f"forecast done, elapsed time:{(time.time() - start) * 1000:.2f}ms")

    res["period"] = s.period
    res["algo"] = algo_name

    check_forecast_results(res)

    fc = res["res"]
    draw_forecast_results(input_list, len(fc) > 2, s.conf, fc, algo_name)
    return res


def add_forecast_params(params, json_obj):
    """add params into parameters"""
    if "forecast_rows" in json_obj:
        raw = json_obj["forecast_rows"]
        if isinstance(raw, bool) or not isinstance(raw, int):
            raise ValueError(f"forecast_rows must be an integer in [1, 1024], got {type(raw).__name__}")
        rows = raw
        if rows < 1 or rows > 1024:
            raise ValueError(f"forecast_rows must be in [1, 1024], got {rows}")
        params["rows"] = rows

    if "start" in json_obj:
        params["start_ts"] = int(json_obj["start"])

    if "every" in json_obj:
        params["time_step"] = int(json_obj["every"])

    if "conf" in json_obj:
        params["conf"] = float(json_obj["conf"])

    if "return_conf" in json_obj:
        params["return_conf"] = int(json_obj["return_conf"])

    if "prec" in json_obj:
        params["precision"] = json_obj["prec"]

    if "tz" in json_obj:
        params["tz"] = json_obj["tz"]




def draw_forecast_results(input_list, return_conf, conf_val, fc, fig_name):
    """Visualize the forecast results"""
    if not Configure.get_instance().get_draw_result_option():
        return

    base_path = Configure.get_instance().get_img_dir()

    try:
        os.makedirs(base_path, exist_ok=True)
    except OSError as exc:
        AppLogger.error("failed to create image directory '%s': %s", base_path, exc)
        return

    if not os.access(base_path, os.W_OK):
        AppLogger.error("image directory '%s' is not writable", base_path)
        return

    AppLogger.debug("draw forecast result in debug model")
    plt.clf()

    plt.plot(input_list)

    predicate = [input_list[-1]]
    predicate.extend(fc[1])

    x = np.arange(len(input_list) - 1, len(input_list) + len(fc[1]), 1)
    plt.plot(x, predicate, linestyle="--", c="blue")

    if return_conf:
        start_x = np.arange(len(input_list), len(input_list) + len(fc[1]), 1)
        lower_series = pd.Series(fc[2], index=start_x)
        upper_series = pd.Series(fc[3], index=start_x)

        plt.fill_between(
            lower_series.index, lower_series, upper_series, color="k", alpha=0.15
        )

    plt.legend(["input", "forecast", f"pred:{conf_val}"], loc="upper left")

    safe_name = os.path.basename(fig_name)
    plt.savefig(os.path.join(base_path, safe_name))
    plt.close()

    AppLogger.debug("draw results completed in debug model")


def check_forecast_results(res):
    AppLogger.debug("start to check forecast result")

    if "res" not in res:
        raise ValueError("forecast result is empty")

    forecast_result = res["res"]
    if len(forecast_result) < 2:
        raise ValueError("result length should greater than or equal to 2")

    n_rows = len(forecast_result[0])
    if n_rows != len(forecast_result[1]):
        raise ValueError(
            "result length is not identical, ts rows:%d  res rows:%d"
            % (n_rows, len(forecast_result[1]))
        )

    if len(forecast_result) > 2 and (
        len(forecast_result[2]) != n_rows or len(forecast_result[3]) != n_rows
    ):
        raise ValueError(
            "result length is not identical in confidence, ts rows:%d, lower confidence rows:%d, "
            "upper confidence rows%d"
            % (n_rows, len(forecast_result[2]), len(forecast_result[3]))
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
    except (ValueError, TypeError) as e:
        AppLogger.error("invalid fc params: %s", e)
        return {"msg": str(e), "rows": -1}

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
