# encoding:utf-8
# pylint: disable=c0103
"""the main route definition for restful service"""
import os
import os.path
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/../")

import numpy as np
import taosanalytics
from flask import Flask, request
from scipy.stats import pearsonr
from taosanalytics.handlers.imputation import handle_imputation
from taosanalytics.handlers.anomaly import handle_anomaly
from taosanalytics.handlers.forecast import handle_forecast
from taosanalytics.handlers.correlation import handle_correlation
from taosanalytics.handlers.misc import handle_batch

from taosanalytics.conf import Configure
from taosanalytics.log import AppLogger
from taosanalytics.model_file_mgt import ModelFileManager
from taosanalytics.service_registry import loader
from taosanalytics.util import (do_check_before_exec, get_more_data_list,
                                 do_initial_check, SINGLE_COLUMN_ERROR_MSG)
from taosanalytics.algo.tool.profile_search import do_profile_search_impl

from taosanalytics.handlers.dynamic_model import (do_handle_undeploy_model, do_handle_dynamic_model)

app_logger = AppLogger()


def _init_app():
    """Initialize configuration, logger, and load services. Called on module import."""
    # Read config path from environment variable or use default
    conf_path = os.environ.get('TDGPT_CONF')

    # Init configuration
    conf = Configure.init(conf_path)

    # Set log parameters
    AppLogger.set_handler(conf.get_log_path())
    AppLogger.set_log_level(conf.get_log_level())

    # Register all services
    loader.register_all_services()

    AppLogger.info("TDgpt service initialized (config: %s)", conf.path)


# Create Flask app
app = Flask(__name__)
app.config["PROPAGATE_EXCEPTIONS"] = True


@app.route("/")
def index():
    """ default rsp """
    return taosanalytics._ANODE_VER


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
    return ModelFileManager.get_instance().get_model_list()


@app.route("/anomaly-detect", methods=['POST'])
def handle_ad_request():
    """handle the anomaly detection requests"""
    AppLogger.info('recv ad request from %s', request.remote_addr)
    return handle_anomaly(request)


@app.route("/forecast", methods=['POST'])
def handle_forecast_req():
    """handle the fc request """
    AppLogger.info('recv forecast request from %s', request.remote_addr)
    return handle_forecast(request)


@app.route("/imputation", methods=['POST'])
def handle_imputation_req():
    """handle the imputation request """
    return handle_imputation(request)


@app.route("/correlation", methods=['POST'])
def handle_correlation_req():
    """handle the correlation request """
    AppLogger.info('recv correlation from %s', request.remote_addr)
    return handle_correlation(request)


# Keep both routes mapped to this handler so existing clients using the legacy endpoint continue to work, while 
# the new integration uses the versioned one.
@app.route('/api/v1/analysis/batch', methods=['POST'])
@app.route("/tool/batch", methods=['POST'])
def handle_batch_req():
    """handle the batch request request """
    return handle_batch(request)


@app.route('/deploy', methods=['POST'])
def deploy_model():
    """deploy model to production environment, e.g. load model to memory, etc."""
    return do_handle_dynamic_model(request)


@app.route('/undeploy', methods=['POST'])
def undeploy_model():
    return do_handle_undeploy_model(request)


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
    # Parse args before initializing so the correct config file is used from
    # the start; services are loaded only once, with the final configuration.
    from taosanalytics.util import parse_args

    args = parse_args()

    if args.conf_path:
        os.environ['TDGPT_CONF'] = args.conf_path

    _init_app()

    # Run development server
    conf = Configure.get_instance()
    host, port = conf.get_server_bind()
    AppLogger.info("Starting development server on %s:%d", host, port)

    app.run(host=host, port=port)
else:
    # Initialize on module import when used as a WSGI module (e.g. gunicorn).
    # When running as __main__, initialization is deferred until after argument
    # parsing so that a -c/--config path is applied before services are loaded.
    _init_app()
