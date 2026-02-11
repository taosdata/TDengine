# encoding:utf-8
# pylint: disable=c0103
""" anomaly detection register/display functions """
import numpy as np
from matplotlib import pyplot as plt
from taosanalytics.conf import app_logger, conf
from taosanalytics.error import failed_load_model_except
from taosanalytics.servicemgmt import loader
from taosanalytics.util import convert_results_to_windows


def do_ad_check(input_list, ts_list, algo_name, params):
    """ actual anomaly detection handler """
    s = loader.get_service(algo_name)

    if s is None:
        app_logger.log_inst.error("specified model not found:%s" % (algo_name))
        failed_load_model_except(algo_name)

    s.set_input_list(input_list, ts_list)
    s.set_params(params)

    res = s.execute()

    n_error = abs(sum(filter(lambda x: x != s.valid_code, res)))
    app_logger.log_inst.debug("There are %d in input, and %d anomaly points found: %s",
                              len(input_list),
                              n_error,
                              res)

    draw_ad_results(input_list, res, algo_name, s.valid_code)

    ano_window, mask_list = convert_results_to_windows(res, ts_list, s.valid_code)
    return res, ano_window, mask_list


def draw_ad_results(input_list, res, fig_name, valid_code):
    """ draw the detected anomaly points """

    # not in debug, do not visualize the anomaly detection result
    if not conf.get_draw_result_option():
        return

    plt.clf()
    plt.figure(figsize=(9, 6))

    plt.plot(input_list, 'b-', label='Data')

    outlier_indices = np.where(np.array(res) != valid_code)
    outlier_val = np.array(input_list)[outlier_indices]

    plt.scatter(outlier_indices, outlier_val,
                color='red', s=100, marker='o',
                edgecolors='darkred', linewidth=1.5,
                label=f'Detected Anomaly Points: ({len(outlier_val)})',
                zorder=5)

    plt.title("Anomaly Detection", fontsize=14, fontweight='bold')
    plt.legend()
    plt.tight_layout()
    plt.grid(True, alpha=0.3)

    plt.savefig(fig_name)
