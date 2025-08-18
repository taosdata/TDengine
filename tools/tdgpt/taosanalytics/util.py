# encoding:utf-8
"""utility methods to helper query processing"""
import os

import numpy as np
from huggingface_hub import snapshot_download
from statsmodels.stats.diagnostic import acorr_ljungbox
from statsmodels.tsa.stattools import adfuller
from tqdm import tqdm

from taosanalytics.conf import app_logger


def validate_pay_load(json_obj):
    """ validate the input payload """
    if "data" not in json_obj:
        raise ValueError('data attr does not exist in json')

    data = json_obj["data"]

    if len(data) <= 1:
        raise ValueError('only one column, primary timestamp column should be provided')

    rows = len(data[0])

    if rows != len(data[1]):
        raise ValueError('data inconsistent, number of rows are not identical')

    if rows < 10 or rows > 40000:
        raise ValueError(f'number of rows should between 10 and 40000, actual {rows} rows')

    if "schema" not in json_obj:
        raise ValueError('schema is missing')

    index = get_data_index(json_obj["schema"])
    if index == -1:
        raise ValueError('invalid schema info, data column is missing')


def convert_results_to_windows(result, ts_list, valid_code):
    """generate the window according to anomaly detection result"""
    skey, ekey = -1, -1
    wins = []

    if ts_list is None or result is None or len(result) != len(ts_list):
        return wins

    for index, val in enumerate(result):
        if val != valid_code:
            ekey = ts_list[index]
            if skey == -1:
                skey = ts_list[index]
        else:
            if ekey != -1:
                wins.append([skey, ekey])
                skey, ekey = -1, -1

    if ekey != -1:
        wins.append([skey, ekey])

    return wins


def is_white_noise(input_list):
    """ determine whether the input list is a white noise list or not """
    if len(input_list) < 16:  # the number of items in the list is insufficient
        return False

    res = acorr_ljungbox(input_list, lags=[6, 12, 16], boxpierce=True, return_df=True)
    q_lb = res.lb_pvalue.array[2]
    return q_lb >= 0.05


def is_stationary(input_list):
    """ determine whether the input list is weak stationary or not """
    adf, pvalue, usedlag, nobs, critical_values, _ = adfuller(input_list, autolag='AIC')
    app_logger.log_inst.info("adf is:%f critical value is:%s" % (adf, critical_values))
    return pvalue < 0.05


def parse_options(option_str) -> dict:
    """
    the option format is like the following string: "algo=ksigma,k=2,invalid_option=invalid_str"
    convert it to the dict format
    """
    options = {}

    if option_str is None or len(option_str) == 0:
        return options

    opt_list = option_str.split(",")
    for line in opt_list:
        if "=" not in line or len(line.strip()) < 3:
            continue

        kv_pair = line.strip().split("=")
        if kv_pair[0].strip() == '' or kv_pair[1].strip() == '':
            continue

        options[kv_pair[0].strip()] = kv_pair[1].strip()

    return options


def get_data_index(schema):
    """get the data index according to the schema info"""
    for index, val in enumerate(schema):
        if val[0] == "val":
            return index

    return -1

def get_past_dynamic_data(data, schema):
    past_dynamic = []

    for index, val in enumerate(schema):
        if val[0].startswith("past_dynamic_real"):
            past_dynamic.append(data[index])

    return None if len(past_dynamic) == 0 else past_dynamic

def get_dynamic_data(data, schema):
    dynamic = []

    for index, val in enumerate(schema):
        if val[0].startswith("dynamic_real"):
            dynamic.append(data[index])

    return None if len(dynamic) == 0 else dynamic


def get_ts_index(schema):
    """get the timestamp index according to the schema info"""
    for index, val in enumerate(schema):
        if val[0] == "ts":
            return index
    return -1


def create_sequences(values, time_steps):
    """ create sequences for training model """
    output = []
    for i in range(len(values) - time_steps + 1):
        output.append(values[i: (i + time_steps)])
    return np.stack(output)


def download_model(model_name, model_dir, enable_ep = False):
    # model_list = ['Salesforce/moirai-1.0-R-small']
    ep = 'https://hf-mirror.com' if enable_ep else None
    model_list = [model_name]

    if not os.path.exists(model_dir):
        print(f"create model directory: {model_dir}")
        os.mkdir(model_dir)

    if ep:
        print(f"set the download ep:{ep}")

    for item in tqdm(model_list):
        snapshot_download(
            repo_id=item,
            local_dir=model_dir,  # storage directory
            local_dir_use_symlinks=False,   # disable the link
            resume_download=True,
            endpoint=ep
        )
