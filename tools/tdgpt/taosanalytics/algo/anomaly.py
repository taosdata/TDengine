# encoding:utf-8
# pylint: disable=c0103
""" anomaly detection register/display functions """
import os

import numpy as np
from matplotlib import pyplot as plt
from taosanalytics.conf import Configure
from taosanalytics.error import failed_load_model_except
from taosanalytics.service_registry import loader
from taosanalytics.log import AppLogger
from taosanalytics.util import convert_results_to_windows


def do_ad_check(input_list, ts_list, algo_name, params):
    """ actual anomaly detection handler """
    s = loader.get_service(algo_name)

    if s is None:
        AppLogger.error(f"specified model not found: {algo_name}")
        failed_load_model_except(algo_name)

    s.set_input_list(input_list, ts_list)
    s.set_params(params)

    res = s.execute()

    n_error = abs(sum(filter(lambda x: x != s.valid_code, res)))
    AppLogger.debug("There are %d in input, and %d anomaly points found: %s",
                              len(input_list),
                              n_error,
                              res)

    draw_anomaly_results(input_list, res, algo_name, s.valid_code, algo_name)

    ano_window, mask_list = convert_results_to_windows(res, ts_list, s.valid_code)
    return res, ano_window, mask_list


def draw_anomaly_results(input_list, res, fig_name, valid_code, algo_name:str):
    """ draw the detected anomaly points """

    # not in debug, do not visualize the anomaly detection result
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

    if isinstance(input_list[0], list):
        # 2-d list
        input = input_list[0]
    else:
        input = input_list

    plt.plot(input, 'b-', label='Data')

    outlier_indices = np.where(np.array(res) != valid_code)
    outlier_val = np.array(input)[outlier_indices]

    plt.scatter(outlier_indices, outlier_val,
                color='red', s=100, marker='o',
                edgecolors='darkred', linewidth=1.5,
                label=f'Detected Anomaly Points: ({len(outlier_val)})',
                zorder=5)

    plt.title(f"Anomaly Detection ({algo_name})", fontsize=14, fontweight='bold')
    plt.legend(loc='upper right')
    plt.tight_layout()
    plt.grid(True, alpha=0.3)

    plt.savefig(os.path.join(base_path, fig_name))
    plt.close()

    AppLogger.debug("draw results completed in debug model")
