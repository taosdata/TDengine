# encoding:utf-8
# pylint: disable=c0103
""" anomaly detection register/display functions """

from matplotlib import pyplot as plt
from taosanalytics.conf import app_logger, conf
from taosanalytics.servicemgmt import loader
from taosanalytics.util import convert_results_to_windows


def do_ad_check(input_list, ts_list, algo_name, params):
    """ actual anomaly detection handler """
    s = loader.get_service(algo_name)

    if s is None:
        s = loader.get_service("ksigma")

    if s is None:
        raise ValueError(f"failed to load {algo_name} or ksigma analysis service")

    s.set_input_list(input_list, ts_list)
    s.set_params(params)

    res = s.execute()

    n_error = abs(sum(filter(lambda x: x != s.valid_code, res)))
    app_logger.log_inst.debug("There are %d in input, and %d anomaly points found: %s",
                              len(input_list),
                              n_error,
                              res)

    # draw_ad_results(input_list, res, algo_name, s.valid_code)

    ano_window = convert_results_to_windows(res, ts_list, s.valid_code)
    return res, ano_window


def draw_ad_results(input_list, res, fig_name, valid_code):
    """ draw the detected anomaly points """

    # not in debug, do not visualize the anomaly detection result
    if not conf.get_draw_result_option():
        return

    plt.clf()
    for index, val in enumerate(res):
        if val != valid_code:
            plt.scatter(index, input_list[index], marker='o', color='r', alpha=0.5, s=100, zorder=3)

    plt.plot(input_list, label='sample')
    plt.savefig(fig_name)
