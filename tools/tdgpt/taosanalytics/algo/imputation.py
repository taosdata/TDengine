# encoding:utf-8
# pylint: disable=c0103
"""forecast helper methods"""
import time
from matplotlib import pyplot as plt

from taosanalytics.conf import app_logger, conf
from taosanalytics.servicemgmt import loader
from taosanalytics.util import parse_time_delta_string


def do_imputation(input_list, ts_list, algo_name, params):
    """ data fc handler """
    s = loader.get_service(algo_name)

    if s is None:
        s = loader.get_service("moment")

    if s is None:
        raise ValueError(f"failed to load {algo_name} analysis service")

    s.set_input_data(input_list, ts_list)
    s.set_params(params)

    start = time.time()
    app_logger.log_inst.debug("start to do imputation")

    res = s.execute()

    app_logger.log_inst.debug("imputation done, elapsed time:%.2fms", (time.time() - start) * 1000)

    # add the imputation model in the result
    res["algo"] = algo_name

    # draw the imputation result
    draw_imputation_final_result(res["target"], res["mask"])

    return res

def draw_imputation_final_result(data, mask):
    if not conf.get_draw_result_option():
        return

    plt.clf()
    plt.plot(data, label='target', c='darkblue')

    for index, val in enumerate(mask):
        if val == 1:
            plt.scatter(index, data[index], marker='o', color='r', alpha=0.5, s=100, zorder=3)

    plt.legend(fontsize=14)
    plt.savefig("imputation_res.png")

def do_set_imputation_params(params, json_obj):
    """ add params into parameters """
    # day, hour, minute, second, millisecond, microsecond, nanosecond
    valid_precision_list = ['d', 'h', 'm', 's', 'ms', 'us', 'ns']
    if "prec" in json_obj:
        time_str = json_obj["prec"]

        _, unit = parse_time_delta_string(time_str)
        if unit not in valid_precision_list:
            raise ValueError(f"precision should be one of {valid_precision_list}")

        params["precision"] = time_str
    else:
        params['precision'] = 'ms'

    valid_freq_dict = {'d':'D', 'h':'H', 'm':'T', 's':'S', 'ms':'L', 'us':'U'}
    if "freq" in json_obj:
        freq_str = json_obj["freq"]

        value, unit = parse_time_delta_string(freq_str)
        if unit not in valid_freq_dict.keys():
            raise ValueError(f"freq should be one of {valid_freq_dict.keys()}")

        params["freq"] = str(value) + valid_freq_dict[unit]
    else:
        params['freq'] = '1L'

def _get_us_factor(unit: str) -> int:
    """Returns the conversion factor to microseconds for a given time unit."""
    factors = {
        'us': 1,
        'ms': 1000,
        's': 1000 * 1000,
        'm': 1000 * 1000 * 60,
        'h': 1000 * 1000 * 60 * 60,
        'd': 1000 * 1000 * 60 * 60 * 24,
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

    # check the correctness of freq and the input data
    for i in range(1, len(ts_list)):
        if delta_in_us + rev_ts_list[i - 1] > rev_ts_list[i]:
            raise ValueError(
                f"invalid freq or precision for the input ts list, freq:{freq}, prec:{prec}, ts interval between: "
                f"{rev_ts_list[i - 1]} {rev_ts_list[i]}")
