# encoding:utf-8
# pylint: disable=c0103
"""forecast helper methods"""
import time

import numpy as np
import pandas as pd
from matplotlib import pyplot as plt

from taosanalytics.conf import app_logger, conf
from taosanalytics.servicemgmt import loader


def do_imputation(input_list, ts_list, algo_name, params):
    """ data fc handler """
    s = loader.get_service(algo_name)

    if s is None:
        s = loader.get_service("moment-imputation")

    if s is None:
        raise ValueError(f"failed to load {algo_name} analysis service")

    s.set_input_data(input_list, ts_list)
    s.set_params(params)

    start = time.time()
    app_logger.log_inst.debug("start to do imputation")

    res = s.execute()

    app_logger.log_inst.debug("imputation done, elapsed time:%.2fms", (time.time() - start) * 1000)

    res["algo"] = algo_name
    draw_imputation_final_result(res["target"], res["mask"])

    return res

def draw_imputation_final_result(data, mask):
    plt.clf()
    plt.plot(data, label='target', c='darkblue')

    for index, val in enumerate(mask):
        if val == 1:
            plt.scatter(index, data[index], marker='o', color='r', alpha=0.5, s=100, zorder=3)

    plt.legend(fontsize=14)
    plt.savefig("imputation_res.png")

def do_set_params(params, json_obj):
    """ add params into parameters """

    # day, hour, minute, second, millisecond, micro-second, nanosecond
    valid_precision_list = ['d', 'h', 'm', 's', 'ms', 'us', 'ns']
    if "prec" in json_obj:
        params["precision"] = json_obj["prec"]

        if params['precision'] not in valid_precision_list:
            raise ValueError(f"precision should be one of {valid_precision_list}")

    valid_freq_list = ['D', 'B', 'H', 'T', 's', 'L', 'U', 'N']
    if "freq" in json_obj:
        params["freq"] = json_obj["freq"]

        if params['freq'] not in valid_freq_list:
            raise ValueError(f"freq should be one of {valid_freq_list}")
