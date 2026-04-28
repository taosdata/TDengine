# encoding:utf-8
"""misc handlers: encapsulates miscellaneous tool business logic"""

import numpy as np
from scipy.stats import pearsonr

from taosanalytics.algo.tool.batch import do_batch_process, update_config
from taosanalytics.algo.tool.profile_search import do_profile_search_impl
from taosanalytics.log import AppLogger
from taosanalytics.util import SINGLE_COLUMN_ERROR_MSG, do_initial_check, do_check_before_exec, get_more_data_list


def handle_batch(request):
    """
    Execute batch processing business logic.

    :param request: Flask request object
    :return: dict with batch result or error information
    """
    try:
        payload_obj = do_initial_check(request)
    except Exception as e:
        return {"msg": str(e), "rows": -1}

    data = payload_obj.get("data", None)
    ts = payload_obj.get("ts", None)
    windows = payload_obj.get("window", None)

    if data is None or ts is None or windows is None:
        msg = "'data', 'ts', and 'window' are required fields in the payload."
        AppLogger.error(msg)
        return {"msg": msg, "rows": -1}

    conf = update_config(payload_obj.get("config", None))

    try:
        # median, lower bounding, upper bounding, processed_batches
        center, lower, upper, processed_batches = do_batch_process(np.array(ts), np.array(data), windows, conf)

        res = {"rows": lower.size, "center": center.tolist(), "lower": lower.tolist(), "upper": upper.tolist()}
        AppLogger.debug("batch processed result: %s", res)

        return res
    except Exception as e:
        AppLogger.error('golden batch process failed, %s', str(e))
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
        AppLogger.error('unsupported API version: %s', api_version)
        return {"msg": f"unsupported API version: {api_version}", "rows": -1}

    try:
        second_list = get_more_data_list(payload, req_json["schema"])
        if second_list is None:
            return {"msg": "a second data column is required for pearsonr", "rows": -1}
        
        correlation, p_value = pearsonr(payload[data_index], second_list)

        if not np.isfinite(correlation):
            correlation = 0.0
            p_value = 1.0
        else:
            correlation = float(correlation)
            p_value = float(p_value)

        AppLogger.debug(f"pearsonr correlation: {correlation}, p value: {p_value}")
        res = {"option": options, "rows": 1, "correlation_coefficient": correlation, "p_value": p_value}

        return res
    except Exception as e:
        AppLogger.error('pearsonr correlation failed, %s', str(e))
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
        AppLogger.error('unsupported API version: %s', api_version)
        return {"msg": f"unsupported API version: {api_version}", "rows": -1}

    try:
        req_json = do_initial_check(request)
    except Exception as e:
        return {"msg": str(e), "rows": -1}

    try:
        result = do_profile_search_impl(req_json)
        AppLogger.debug("profile-search result: %s", result)
        return result

    except Exception as e:
        AppLogger.error('profile search failed, %s', str(e))
        return {"msg": str(e), "rows": -1}

