# encoding:utf-8
# pylint: disable=c0103
"""forecast helper methods"""

import numpy as np
import pandas as pd
from matplotlib import pyplot as plt

from taosanalytics.conf import app_logger, conf
from taosanalytics.servicemgmt import loader


def do_forecast(input_list, ts_list, algo_name, params):
    """ data fc handler """
    s = loader.get_service(algo_name)

    if s is None:
        s = loader.get_service("holtwinters")

    if s is None:
        raise ValueError(f"failed to load {algo_name} or holtwinters analysis service")

    s.set_input_list(input_list, ts_list)
    s.set_params(params)

    app_logger.log_inst.debug("start to do forecast")
    res = s.execute()

    app_logger.log_inst.debug("forecast done")

    res["period"] = s.period
    res["algo"] = algo_name

    check_fc_results(res)

    fc = res["res"]
    draw_fc_results(input_list, len(fc) > 2, fc, len(fc[0]), algo_name)
    return res


def do_add_fc_params(params, json_obj):
    """ add params into parameters """
    if "forecast_rows" in json_obj:
        params["fc_rows"] = int(json_obj["forecast_rows"])

    if "start" in json_obj:
        params["start_ts"] = int(json_obj["start"])

    if "every" in json_obj:
        params["time_step"] = int(json_obj["every"])

    if "conf" in json_obj:
        params["conf"] = int(json_obj["conf"])

    if "return_conf" in json_obj:
        params["return_conf"] = int(json_obj["return_conf"])


def insert_ts_list(res, start_ts, time_step, fc_rows):
    """ insert the ts list before return results """
    ts_list = [start_ts + i * time_step for i in range(fc_rows)]
    res.insert(0, ts_list)
    return res


def draw_fc_results(input_list, return_conf, fc, n_rows, fig_name):
    """Visualize the forecast results """
    # controlled by option, do not visualize the anomaly detection result
    if not conf.get_draw_result_option():
        return

    app_logger.log_inst.debug('draw forecast result in debug model')
    plt.clf()

    x = np.arange(len(input_list), len(input_list) + n_rows, 1)

    # draw the range of conf
    if return_conf:
        lower_series = pd.Series(fc[2], index=x)
        upper_series = pd.Series(fc[3], index=x)

        plt.fill_between(lower_series.index, lower_series, upper_series, color='k', alpha=.15)

    plt.plot(input_list)
    plt.plot(x, fc[1], c='blue')
    plt.savefig(fig_name)

    app_logger.log_inst.debug("draw results completed in debug model")


def check_fc_results(res):
    app_logger.log_inst.debug("start to check forecast result")

    if "res" not in res:
        raise ValueError("forecast result is empty")

    fc = res["res"]
    if len(fc) < 2:
        raise ValueError("result length should greater than or equal to 2")

    n_rows = len(fc[0])
    if n_rows != len(fc[1]):
        raise ValueError("result length is not identical, ts rows:%d  res rows:%d" % (
            n_rows, len(fc[1])))

    if len(fc) > 2 and (len(fc[2]) != n_rows or len(fc[3]) != n_rows):
        raise ValueError(
            "result length is not identical in confidence, ts rows:%d, lower confidence rows:%d, "
            "upper confidence rows%d" %
            (n_rows, len(fc[2]), len(fc[3])))
