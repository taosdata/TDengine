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

"""anomaly detection: algo logic and request handler"""

import os

import numpy as np
from matplotlib import pyplot as plt

from lib.conf import Configure
from lib.errors import failed_load_model_except
from lib.log import AppLogger
from lib.service_registry import loader
from lib.util import convert_results_to_windows, do_check_before_exec, get_algo, parse_options


def do_ad_check(input_list, ts_list, algo_name, params):
    """actual anomaly detection handler"""
    s = loader.get_service(algo_name)

    if s is None:
        AppLogger.error(f"specified model not found: {algo_name}")
        failed_load_model_except(algo_name)

    s.set_input_list(input_list, ts_list)
    s.set_params(params)

    res = s.execute()

    n_error = abs(sum(filter(lambda x: x != s.valid_code, res)))
    AppLogger.debug(
        "There are %d in input, and %d anomaly points found: %s",
        len(input_list),
        n_error,
        res,
    )

    draw_anomaly_results(input_list, res, algo_name, s.valid_code, algo_name)

    ano_window, mask_list = convert_results_to_windows(res, ts_list, s.valid_code)
    return res, ano_window, mask_list


def draw_anomaly_results(input_list, res, fig_name, valid_code, algo_name: str):
    """draw the detected anomaly points"""

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

    plt.figure(figsize=(9, 6))

    is_2darr = False
    if isinstance(input_list[0], list):
        for item in input_list:
            plt.plot(item, "b-", alpha=0.5)
        is_2darr = True
    else:
        plt.plot(input_list, "b-", label="Data")

    outlier_indices = np.where(np.array(res) != valid_code)

    if is_2darr:
        for item in input_list:
            outlier_val = np.array(item)[outlier_indices]
            plt.scatter(
                outlier_indices,
                outlier_val,
                color="red",
                s=100,
                marker="o",
                edgecolors="darkred",
                linewidth=1.5,
                label=f"Detected Anomaly Points: ({len(outlier_val)})",
                zorder=5,
            )
    else:
        outlier_val = np.array(input_list)[outlier_indices]
        plt.scatter(
            outlier_indices,
            outlier_val,
            color="red",
            s=100,
            marker="o",
            edgecolors="darkred",
            linewidth=1.5,
            label=f"Detected Anomaly Points: ({len(outlier_val)})",
            zorder=5,
        )

    plt.title(f"Anomaly Detection ({algo_name})", fontsize=14, fontweight="bold")
    plt.legend(loc="upper right")
    plt.tight_layout()
    plt.grid(True, alpha=0.3)

    plt.savefig(os.path.join(base_path, fig_name))
    plt.close()

    AppLogger.debug("draw results completed in debug model")


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
