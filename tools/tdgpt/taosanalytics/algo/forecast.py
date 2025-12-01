# encoding:utf-8
# pylint: disable=c0103
"""forecast helper methods"""
import time

import numpy as np
import pandas as pd
from matplotlib import pyplot as plt

from taosanalytics.conf import app_logger, conf
from taosanalytics.servicemgmt import loader


def do_forecast(input_list, ts_list, algo_name, params, past_dynamic_real = None, dynamic_real = None):
    """ data fc handler """
    s = loader.get_service(algo_name)

    if s is None:
        s = loader.get_service("holtwinters")

    if s is None:
        raise ValueError(f"failed to load {algo_name} or holtwinters analysis service")

    s.set_input_data(input_list, ts_list, past_dynamic_real, dynamic_real)
    s.set_params(params)

    start = time.time()
    app_logger.log_inst.debug("start to do forecast")

    res = s.execute()

    app_logger.log_inst.debug("forecast done, elapsed time:%.2fms", (time.time() - start) * 1000)

    res["period"] = s.period
    res["algo"] = algo_name

    check_fc_results(res)

    fc = res["res"]
    draw_fc_results(input_list, len(fc) > 2, s.conf, fc, algo_name)
    return res


def do_add_fc_params(params, json_obj):
    """ add params into parameters """
    if "forecast_rows" in json_obj:
        params["rows"] = int(json_obj["forecast_rows"])

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


def insert_ts_list(res, start_ts, time_step, fc_rows):
    """ insert the ts list before return results """
    ts_list = [start_ts + i * time_step for i in range(fc_rows)]
    res.insert(0, ts_list)
    return res


def draw_fc_results(input_list, return_conf, conf_val, fc, fig_name):
    """Visualize the forecast results """
    # controlled by option, do not visualize the anomaly detection result
    if not conf.get_draw_result_option():
        return

    app_logger.log_inst.debug('draw forecast result in debug model')
    plt.clf()

    plt.plot(input_list)

    predicate = [input_list[-1]]
    predicate.extend(fc[1])

    x = np.arange(len(input_list) - 1, len(input_list) + len(fc[1]), 1)
    plt.plot(x, predicate, linestyle='--', c='blue')

    # draw the range of conf
    if return_conf:
        start_x = np.arange(len(input_list), len(input_list) + len(fc[1]), 1)
        lower_series = pd.Series(fc[2], index=start_x)
        upper_series = pd.Series(fc[3], index=start_x)

        plt.fill_between(lower_series.index, lower_series, upper_series, color='k', alpha=.15)

    plt.legend(['input', 'forecast', f'pred:{conf_val}'], loc='upper left')

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
