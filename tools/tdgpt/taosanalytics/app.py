# encoding:utf-8
# pylint: disable=c0103
"""the main route definition for restful service"""
import os.path
import sys

import numpy as np
from scipy.stats import pearsonr

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/../")

# Imports below require modified sys.path
from flask import Flask, request  # noqa: E402 - Import after sys.path modification
import taosanalytics  # noqa: E402 - Import after sys.path modification
from taosanalytics.algo.imputation import (do_imputation, do_set_imputation_params, check_freq_param)  # noqa: E402
from taosanalytics.algo.anomaly import do_ad_check  # noqa: E402
from taosanalytics.algo.forecast import do_forecast, do_add_fc_params  # noqa: E402
from taosanalytics.algo.correlation import do_dtw, do_tlcc  # noqa: E402
from taosanalytics.algo.tool.profile_search import do_profile_search_impl  # noqa: E402
from taosanalytics.algo.tool.batch import do_batch_process, update_config  # noqa: E402
from taosanalytics.conf import conf  # noqa: E402
from taosanalytics.model import model_manager  # noqa: E402
from taosanalytics.servicemgmt import loader  # noqa: E402
from taosanalytics.util import (  # noqa: E402
    app_logger, parse_options, get_past_dynamic_data, get_dynamic_data,
    get_more_data_list, do_check_before_exec, do_initial_check, SINGLE_COLUMN_ERROR_MSG
)

app = Flask(__name__)
app.config["PROPAGATE_EXCEPTIONS"] = True

# load the all algos
app_logger.set_handler(conf.get_log_path())
app_logger.set_log_level(conf.get_log_level())
loader.load_all_service()

_ANODE_VER = f'TDgpt - TDengine TSDB© Time-Series Data Analytics Platform (ver {taosanalytics.__version__})'


@app.route("/")
def start():
    """ default rsp """
    return _ANODE_VER


@app.route("/status")
def server_status():
    """ return server status """
    return {
        'protocol': 1.0,
        'status': 'ready'
    }


@app.route("/list")
def list_all_services():
    """
    API function to return all available services, including both fc and anomaly detection
    """
    return loader.get_service_list()


@app.route("/models")
def list_all_models():
    """ list all available models """
    return model_manager.get_model_list()


@app.route("/anomaly-detect", methods=['POST'])
def handle_ad_request():
    """handle the anomaly detection requests"""
    app_logger.log_inst.info('recv ad request from %s', request.remote_addr)

    try:
        req_json, payload, options, data_index, ts_index = do_check_before_exec(request, True)
    except Exception as e:
        app_logger.log_inst.error("failed to do anomaly-detection, %s", str(e))
        return {"msg": str(e), "rows": -1}

    algo = req_json["algo"].lower() if "algo" in req_json else "ksigma"

    try:
        ts_list = payload[ts_index].copy()
        payload.pop(ts_index)
    except ValueError as e:
        return {"msg": str(e), "rows": -1}

    params = parse_options(options)

    try:
        res_list, ano_window, mask_list = do_ad_check(payload, ts_list, algo, params)
        result = {"algo": algo, "option": options, "res": ano_window, "rows": len(ano_window), "mask": mask_list}

        app_logger.log_inst.debug("anomaly-detection result: %s", str(result))
        return result

    except Exception as e:
        result = {"res": {}, "rows": -1, "msg": str(e)}
        app_logger.log_inst.error("failed to do anomaly-detection, %s", str(e))

        return result


@app.route("/forecast", methods=['POST'])
def handle_forecast_req():
    """handle the fc request """
    app_logger.log_inst.info('recv fc from %s', request.remote_addr)
    req_json, payload, options, data_index, ts_index = do_check_before_exec(request)

    params = parse_options(options)

    try:
        do_add_fc_params(params, req_json)
    except ValueError as e:
        app_logger.log_inst.error("invalid fc params: %s", e)
        return {"msg": f"{e}", "rows": -1}

    # holt-winters by default
    algo = req_json['algo'].lower() if 'algo' in req_json else 'holtwinters'

    try:
        res1 = do_forecast(payload[data_index], payload[ts_index], algo, params,
                           get_past_dynamic_data(payload, req_json["schema"]),
                           get_dynamic_data(payload, req_json["schema"]))

        res = {"option": options, "rows": params["rows"]}
        res.update(res1)

        app_logger.log_inst.debug("forecast result: %s", res)
        return res
    except Exception as e:
        app_logger.log_inst.error('forecast failed, %s', str(e))
        return {"msg": str(e), "rows": -1}


@app.route("/imputation", methods=['POST'])
def handle_imputation_req():
    """handle the imputation request """
    app_logger.log_inst.info('recv imputation from %s', request.remote_addr)
    try:
        req_json, payload, options, data_index, ts_index = do_check_before_exec(request)
    except Exception as e:
        return {"msg": str(e), "rows": -1}

    params = parse_options(options)

    try:
        do_set_imputation_params(params, req_json)
    except ValueError as e:
        app_logger.log_inst.error("invalid imputation params: %s", e)
        return {"msg": f"{e}", "rows": -1}

    algo = req_json['algo'].lower() if 'algo' in req_json else 'moment'

    try:
        freq = req_json["freq"] if 'freq' in req_json else '1ms'
        prec = req_json['prec'] if 'prec' in req_json else 'ms'
        check_freq_param(payload[ts_index], freq, prec)

        imputat_res = do_imputation(payload[data_index], payload[ts_index], algo, params)

        final_res = {"option": options, "rows": len(imputat_res["ts"])}
        final_res.update(imputat_res)

        app_logger.log_inst.debug("imputation result: %s", final_res)
        return final_res
    except Exception as e:
        app_logger.log_inst.error('imputation failed, %s', str(e))
        return {"msg": str(e), "rows": -1}


@app.route("/correlation", methods=['POST'])
def handle_correlation_req():
    """handle the correlation request """
    app_logger.log_inst.info('recv correlation from %s', request.remote_addr)
    try:
        # check for rows limitation to reduce the dtw process time
        req_json, payload, options, data_index, ts_index = do_check_before_exec(request, False)
    except Exception as e:
        return {"msg": str(e), "rows": -1}

    params = parse_options(options)

    algo = req_json['algo'].lower()

    try:
        second_list = get_more_data_list(payload, req_json["schema"])

        if algo == 'dtw':
            dist, path = do_dtw(payload[data_index], second_list, params)

            res = {"option": options, "rows": len(path), "distance": dist, "path": path}
            app_logger.log_inst.debug("dtw result: %s", res)

            return res
        elif algo == 'tlcc':
            lags, ccf_vals = do_tlcc(payload[data_index], second_list, params)

            res = {"option": options, "rows": len(lags), "lags": lags, "ccf_vals": ccf_vals}
            app_logger.log_inst.debug("tlcc result: %s", res)

            return res
        else:
            raise ValueError(f"unsupported algo: {algo}")
    except Exception as e:
        app_logger.log_inst.error('correlation failed, %s', str(e))
        return {"msg": str(e), "rows": -1}


# Keep both routes mapped to this handler so existing clients using the legacy endpoint continue to work, while 
# the new integration uses the versioned one.
@app.route('/api/v1/analysis/batch', methods=['POST'])
@app.route("/tool/batch", methods=['POST'])
def handle_batch_req():
    """handle the batch request"""
    app_logger.log_inst.info('recv batch req from %s', request.remote_addr)

    try:
        payload_obj = do_initial_check(request)
    except Exception as e:
        return {"msg": str(e), "rows": -1}

    data = payload_obj.get("data", None)
    ts = payload_obj.get("ts", None)
    windows = payload_obj.get("window", None)

    if data is None or ts is None or windows is None:
        msg = "'data', 'ts', and 'window' are required fields in the payload."
        app_logger.log_inst.error(msg)
        return {"msg": msg, "rows": -1}

    conf = update_config(payload_obj.get("config", None))

    try:
        # median, lower bounding, upper bounding, processed_batches
        center, lower, upper, processed_batches = do_batch_process(np.array(ts), np.array(data), windows, conf)

        res = {"rows": lower.size, "center": center.tolist(), "lower": lower.tolist(), "upper": upper.tolist()}
        app_logger.log_inst.debug("batch processed result: %s", res)

        return res
    except Exception as e:
        app_logger.log_inst.error('golden batch process failed, %s', str(e))
        return {"msg": str(e), "rows": -1}


def handle_pearsonr(request, api_version):
    """
    Execute pearsonr correlation logic.

    :param request: Flask request object
    :param api_version: API version to determine the specific implementation of Pearson correlation
    :return: dict with correlation result or error information
    """
    try:
        # check for rows limitation to reduce the process time
        req_json, payload, options, data_index, _ = do_check_before_exec(request, False)
    except ValueError as e:
        msg = str(e)
        if msg == SINGLE_COLUMN_ERROR_MSG:
            msg = 'a second data column is required for pearsonr'
        return {"msg": msg, "rows": -1}
    except Exception as e:
        return {"msg": str(e), "rows": -1}

    if api_version != 'v1':
        app_logger.log_inst.error('unsupported API version: %s', api_version)
        return {"msg": f"unsupported API version: {api_version}", "rows": -1}

    try:
        second_list = get_more_data_list(payload, req_json["schema"])
        if second_list is None:
            return {"msg": "a second data column is required for pearsonr", "rows": -1}
        
        correlation, p_value = pearsonr(payload[data_index], second_list)
        if not np.isfinite(correlation):
            correlation = 0.0
            p_value = 1.0

        correlation = float(correlation)
        p_value = float(p_value)

        app_logger.log_inst.debug(f"pearsonr correlation: {correlation}, p value: {p_value}")
        res = {"option": options, "rows": 1, "correlation_coefficient": correlation, "p_value": p_value}

        return res
    except Exception as e:
        app_logger.log_inst.error('pearsonr correlation failed, %s', str(e))
        return {"msg": str(e), "rows": -1}

def do_profile_search(request, api_version):
    """
    Execute profile search logic.

    :param request: Flask request object
    :param api_version: API version to determine the specific implementation of profile search
    :return: dict with profile search result or error information

    Request body example:
    Supported normalization values: "min-max", "z-score", "centering", "none".
    Supported algo types: "dtw", "cosine".
    For "dtw", algo.params may include:
    - "radius": search radius for fastdtw
    - "min_window": minimum ts window size for profile search
    - "max_window": maximum ts window size for profile search
    - "window_size_step": step for ts window size between min_window and max_window, only applicable for dtw algo
    - "window_sliding_step": step for sliding the ts window when searching
    Result selection notes:
    - Return the top N similar profiles with "num".
    - Or return all profiles with distance below the threshold when using dtw.
    - Or return all profiles with similarity above the threshold when using cosine similarity.
    - "num" and "threshold" cannot be set at the same time.
    - "exclude_contained" is only applicable for dtw and means whether to exclude the worse matched profile in a strict-containment pair, keeping the better one (the match with the smaller distance). For example, if there are two matched profiles with ts window [1, 5] and [2, 4], and one strictly contains the other, the worse match will be excluded if "exclude_contained" is set to true. 
    - "exclude_source" is applicable for all algorithms and means whether to exclude the matched profile that contains the source profile. For example, if the source profile has ts window [2, 4], the matched profile with ts window [2, 4] will be excluded if "exclude_source" is set to true.
    - Threshold-based results are capped at 500 matches.
    target_data.ts may be either:
    - a unix timestamp list, such as [1, 2, 3, 4, 5, 6]
    - a ts window list, such as [[1, 5], [2, 6]]
    {
        "normalization": "z-score",
        "algo": {
            "type": "dtw",
            "params": {
                "radius": 5,
                "min_window": 5,
                "max_window": 20,
                "window_size_step": 2,
                "window_sliding_step": 1
            }
        },
        "result": {
            "num": 3,
            "exclude_contained": true,
            "exclude_source": true
        },
        "source_data": {
            "ts": [1000, 2000, 3000, 4000, 5000],
            "data": [1, 2, 3, 4, 5]
        },

        "target_data": {
            "ts": [1, 2, 3, 4, 5, 6],
            "data": [1, 2, 3, 4, 5, 6]
        }
    }
    Response example:
    metric_type is either "dtw_distance" or "cosine_similarity".
    Sort rule:
    - "dtw_distance": smaller "criteria" means more similar (ascending)
    - "cosine_similarity": larger "criteria" means more similar (descending)
    In each match, "num" is the number of data points in the matched window.
    {
        "rows": 3,
        "metric_type": "dtw_distance",
        "matches": [
            {
                "criteria": 0.12,
                "ts_window": [1, 5],
                "num": 7
            },
            {
                "criteria": 0.21,
                "ts_window": [2, 6],
                "num": 5
            },
            {
                "criteria": 0.35,
                "ts_window": [3, 7],
                "num": 4
            }
        ]
    }

    """
    if api_version != 'v1':
        app_logger.log_inst.error('unsupported API version: %s', api_version)
        return {"msg": f"unsupported API version: {api_version}", "rows": -1}

    try:
        req_json = do_initial_check(request)
    except Exception as e:
        return {"msg": str(e), "rows": -1}

    try:
        result = do_profile_search_impl(req_json)
        app_logger.log_inst.debug("profile-search result: %s", result)
        return result

    except Exception as e:
        app_logger.log_inst.error('profile search failed, %s', str(e))
        return {"msg": str(e), "rows": -1}


@app.route('/api/v1/analysis/pearsonr', methods=['POST'])
def handle_pearsonr_req():
    """handle the pearsonr correlation request """
    app_logger.log_inst.info('recv pearsonr correlation request from %s', request.remote_addr)
    return handle_pearsonr(request, api_version='v1')


@app.route('/api/v1/analysis/profile-search', methods=['POST'])
def handle_profile_search_req():
    """handle the profile search request """
    app_logger.log_inst.info('recv profile search request from %s', request.remote_addr)
    return do_profile_search(request, api_version='v1')


if __name__ == '__main__':
    app.run(port=6035)
