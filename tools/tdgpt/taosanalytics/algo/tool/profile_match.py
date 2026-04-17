# encoding:utf-8
"""profile matching core logic"""

import heapq
from enum import IntEnum

import numpy as np
from fastdtw import fastdtw


class ProfileMatchLimits(IntEnum):
    MIN_RADIUS = 1
    MAX_RADIUS = 10
    MIN_PROFILE_MATCH_RESULTS = 1
    MAX_PROFILE_MATCH_RESULTS = 500

    MIN_WINDOW = 1


def _normalize_series(series_arr, norm_type):
    arr = np.array(series_arr, dtype=float)
    if arr.ndim != 1 or arr.size == 0:
        raise ValueError("input series must be a non-empty 1-D numeric array")

    if norm_type == "none":
        return arr
    if norm_type == "centering":
        return arr - float(np.mean(arr))
    if norm_type == "z-score":
        std = float(np.std(arr))
        if std == 0:
            return np.zeros_like(arr)
        return (arr - float(np.mean(arr))) / std
    if norm_type == "min-max":
        min_val = float(np.min(arr))
        max_val = float(np.max(arr))
        if max_val == min_val:
            return np.zeros_like(arr)
        return (arr - min_val) / (max_val - min_val)

    raise ValueError(f"unsupported normalization: {norm_type}")


def _calc_cosine_similarity(arr1, arr2):
    den = float(np.linalg.norm(arr1) * np.linalg.norm(arr2))
    if den == 0:
        return 0.0
    return float(np.dot(arr1, arr2) / den)


def _build_window_candidates_from_series(ts_vals, data_vals, source_len, min_window, max_window):
    ts_arr = np.array(ts_vals)
    data_arr = np.array(data_vals, dtype=float)

    if ts_arr.ndim != 1 or data_arr.ndim != 1:
        raise ValueError('when "target_data.data" is 1-D, "target_data.ts" must also be 1-D')
    if ts_arr.size != data_arr.size:
        raise ValueError('"target_data.ts" length must equal "target_data.data" length for 1-D data')
    if ts_arr.size == 0:
        raise ValueError('"target_data.data" cannot be empty')

    min_w = min_window if min_window is not None else source_len
    max_w = max_window if max_window is not None else source_len

    min_w = int(min_w)
    max_w = int(max_w)
    if min_w <= 0 or max_w <= 0 or min_w > max_w:
        raise ValueError("invalid min_window/max_window")

    max_w = min(max_w, int(data_arr.size))
    min_w = min(min_w, max_w)

    for win_size in range(min_w, max_w + 1):
        for start in range(0, int(data_arr.size - win_size + 1)):
            end = start + win_size - 1
            yield {
                "series": data_arr[start:end + 1],
                "ts_window": [ts_arr[start].item(), ts_arr[end].item()],
                "num": end - start + 1
            }


def _build_candidates_from_profiles(ts_vals, profiles, min_window, max_window):
    if not isinstance(profiles, list) or len(profiles) == 0:
        raise ValueError('"target_data.data" must be a non-empty array')

    if not isinstance(ts_vals, list):
        raise ValueError('"target_data.ts" must be an array')

    has_candidate = False
    for idx, profile in enumerate(profiles):
        profile_arr = np.array(profile, dtype=float)
        if profile_arr.ndim != 1 or profile_arr.size == 0:
            raise ValueError("each profile in target_data.data must be a non-empty 1-D numeric array")

        if min_window is not None and profile_arr.size < int(min_window):
            continue
        if max_window is not None and profile_arr.size > int(max_window):
            continue

        if len(ts_vals) <= idx:
            raise ValueError('when "target_data.data" is a list of profiles, "target_data.ts" and "target_data.data" must have matching lengths')
        
        if isinstance(ts_vals[idx], (list, tuple)) and len(ts_vals[idx]) == 2:
            ts_window = [ts_vals[idx][0], ts_vals[idx][1]]
        else:
            raise ValueError('when "target_data.data" is a list of profiles, each corresponding item in "target_data.ts" must be a [start_ts, end_ts] pair')

        has_candidate = True
        yield {
            "series": profile_arr,
            "ts_window": ts_window,
            "num": profile_arr.size
        }

    if not has_candidate:
        raise ValueError("no candidate profiles after min_window/max_window filtering")


def _validate_and_parse_profile_match_input(req_json):
    norm_type = req_json.get("normalization", "none")
    if norm_type is None:
        norm_type = "none"

    norm_type = str(norm_type).lower()

    if norm_type not in {"none", "min-max", "z-score", "centering"}:
        raise ValueError(f"unsupported normalization: {norm_type}")

    algo_obj = req_json.get("algo", {})
    algo_type = str(algo_obj.get("type", "dtw")).lower()
    if algo_type not in {"dtw", "cosine"}:
        raise ValueError(f"unsupported algo: {algo_type}")

    algo_params = algo_obj.get("params", {})
    if algo_params is None:
        algo_params = {}

    result_obj = req_json.get("result", {})
    if result_obj is None:
        result_obj = {}

    has_num = "num" in result_obj
    has_threshold = "threshold" in result_obj
    if has_num and has_threshold:
        raise ValueError('"num" and "threshold" cannot be set at the same time')
    if not has_num and not has_threshold:
        raise ValueError('either "num" or "threshold" must be provided')
    
    if has_threshold:
        # validate the threshold value
        try:
            t = float(result_obj["threshold"])
        except Exception:
            raise ValueError('"result.threshold" must be a number')

        if algo_type == "dtw" and t < 0:
            raise ValueError('for dtw algorithm, "result.threshold" must be non-negative')
        if algo_type == "cosine" and (t < -1 or t > 1):
            raise ValueError('for cosine similarity, "result.threshold" must be in range [-1, 1]')
        
        if not np.isfinite(t):
            raise ValueError('"result.threshold" cannot be NaN or Inf')

    top_n = None
    if has_num:
        try:
            top_n = int(result_obj["num"])
        except Exception:
            raise ValueError('"result.num" must be an integer')
        if top_n < ProfileMatchLimits.MIN_PROFILE_MATCH_RESULTS or top_n > ProfileMatchLimits.MAX_PROFILE_MATCH_RESULTS:
            raise ValueError(f'"result.num" must be in range [{ProfileMatchLimits.MIN_PROFILE_MATCH_RESULTS}, {ProfileMatchLimits.MAX_PROFILE_MATCH_RESULTS}]')

    source_data = req_json.get("source_data", None)
    target_data = req_json.get("target_data", None)

    if source_data is None or target_data is None:
        raise ValueError('"source_data" and "target_data" are required')

    ts_list = target_data.get("ts", None) if isinstance(target_data, dict) else None
    data_list = target_data.get("data", None) if isinstance(target_data, dict) else None

    if ts_list is None or data_list is None:
        raise ValueError('"target_data.ts" and "target_data.data" are required')

    if not np.isfinite(ts_list).all():
        raise ValueError('"target_data.ts" contains NaN or Inf')
    
    if not np.isfinite(data_list).all():
        raise ValueError('"target_data.data" contains NaN or Inf')

    source_arr = np.array(source_data, dtype=float)
    if source_arr.ndim != 1 or source_arr.size == 0:
        raise ValueError('"source_data" must be a non-empty 1-D numeric array')
    if not np.all(np.isfinite(source_arr)):
        raise ValueError('"source_data" contains NaN or Inf')

    if algo_type == "dtw":
        radius = int(algo_params.get("radius", ProfileMatchLimits.MIN_RADIUS))
        if radius < ProfileMatchLimits.MIN_RADIUS or radius > ProfileMatchLimits.MAX_RADIUS:
            raise ValueError(f"radius value out of range, valid range [{ProfileMatchLimits.MIN_RADIUS}, {ProfileMatchLimits.MAX_RADIUS}]")
    else: 
        radius = None

    if algo_type != "dtw" and ("min_window" in algo_params or "max_window" in algo_params):
        raise ValueError('"min_window" and "max_window" can only be set for dtw algorithm')

    min_window = algo_params.get("min_window", None)
    max_window = algo_params.get("max_window", None)
    
    if min_window is not None:
        min_window = int(min_window)
    if max_window is not None:
        max_window = int(max_window)
    if min_window is not None and min_window < ProfileMatchLimits.MIN_WINDOW:
        raise ValueError("min_window must be a positive integer")
    if max_window is not None and max_window < ProfileMatchLimits.MIN_WINDOW:
        raise ValueError("max_window must be a positive integer")
    if min_window is not None and max_window is not None and min_window > max_window:
        raise ValueError("min_window cannot be larger than max_window")

    return {
        "norm_type": norm_type,
        "algo_type": algo_type,
        "result_obj": result_obj,
        "has_threshold": has_threshold,
        "top_n": top_n,
        "source_arr": source_arr,
        "ts_list": ts_list,
        "data_list": data_list,
        "radius": radius,
        "min_window": min_window,
        "max_window": max_window,
    }


def do_profile_match_impl(req_json):
    parsed = _validate_and_parse_profile_match_input(req_json)

    norm_type = parsed["norm_type"]
    algo_type = parsed["algo_type"]
    result_obj = parsed["result_obj"]
    has_threshold = parsed["has_threshold"]
    top_n = parsed["top_n"]
    source_arr = parsed["source_arr"]
    ts_list = parsed["ts_list"]
    data_list = parsed["data_list"]
    radius = parsed["radius"]
    min_window = parsed["min_window"]
    max_window = parsed["max_window"]

    is_profile_list = isinstance(data_list, list) and len(data_list) > 0 and isinstance(data_list[0], (list, tuple))
    if is_profile_list:
        candidates_stream = _build_candidates_from_profiles(ts_list, data_list, min_window, max_window)
    else:
        candidates_stream = _build_window_candidates_from_series(
            ts_list, data_list, source_arr.size, min_window, max_window
        )

    source_norm = _normalize_series(source_arr, norm_type)
    metric_type = "dtw_distance" if algo_type == "dtw" else "cosine_similarity"

    top_heap = []
    seq = 0

    def _heap_key(criteria_val, seq_idx):
        # Higher key means better candidate for both dtw/cosine.
        if algo_type == "dtw":
            return (-criteria_val, -seq_idx)
        return (criteria_val, -seq_idx)

    threshold = float(result_obj["threshold"]) if has_threshold else None
    top_n = ProfileMatchLimits.MAX_PROFILE_MATCH_RESULTS if top_n is None else top_n

    for item in candidates_stream:
        candidate_norm = _normalize_series(item["series"], norm_type)
        seq += 1

        if algo_type == "dtw":
            criteria, _ = fastdtw(source_norm, candidate_norm, radius=radius)
            criteria = float(criteria)
        else:
            if source_norm.size != candidate_norm.size:
                raise ValueError("for cosine similarity, source_data and each candidate profile must have the same length")

            criteria = _calc_cosine_similarity(source_norm, candidate_norm)

        if has_threshold:
            if algo_type == "dtw" and criteria > threshold:
                continue
            if algo_type == "cosine" and criteria < threshold:
                continue

        match_obj = {
            "criteria": criteria,
            "ts_window": item["ts_window"],
            "num": item["num"]
        }

        # Keep only a bounded number of matches in memory.
        key = _heap_key(criteria, seq)
        heap_item = (key, seq, match_obj)
        if len(top_heap) < top_n:
            heapq.heappush(top_heap, heap_item)
        elif key > top_heap[0][0]:
            heapq.heapreplace(top_heap, heap_item)

    # Rebuild deterministic order to match existing output semantics.
    if algo_type == "dtw":
        top_heap.sort(key=lambda x: (x[2]["criteria"], x[1]))
    else:
        top_heap.sort(key=lambda x: (-x[2]["criteria"], x[1]))
    matches = [x[2] for x in top_heap]


    return {
        "rows": len(matches),
        "metric_type": metric_type,
        "matches": matches
    }
