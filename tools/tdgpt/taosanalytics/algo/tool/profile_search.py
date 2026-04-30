# encoding:utf-8
"""profile search core logic"""

import heapq
from enum import Enum, IntEnum

import numpy as np
from fastdtw import fastdtw


def _np_scalar(val):
    """Convert a numpy scalar (or object-array element) to a JSON-serializable Python type.

    Numpy arrays with dtype=object (e.g. created from very large Python integers) contain
    plain Python objects that do not have a ``.item()`` method.  Using ``.item()`` on such
    elements raises ``AttributeError``.  This helper calls ``.item()`` when available and
    falls back to returning the value unchanged otherwise.
    """
    return val.item() if hasattr(val, 'item') else val


class ProfileSearchLimits(IntEnum):
    MIN_RADIUS = 1
    MAX_RADIUS = 10
    MIN_PROFILE_SEARCH_RESULTS = 1
    MAX_PROFILE_SEARCH_RESULTS = 500

    MIN_WINDOW = 1

    MAX_SOURCE_LEN = 10000
    MAX_TARGET_LEN = 100000
    MAX_PROFILES = 10000
    MAX_WINDOW_CANDIDATES = 10000


class NormalizationMethod(str, Enum):
    NONE = "none"
    MIN_MAX = "min-max"
    Z_SCORE = "z-score"
    CENTERING = "centering"


def _validate_normalization(norm_type):
    if norm_type is None:
        norm_type = NormalizationMethod.NONE.value

    norm_type = str(norm_type).lower()
    if norm_type not in {m.value for m in NormalizationMethod}:
        raise ValueError(f"unsupported normalization: {norm_type}")
    
    return norm_type


def _validate_result_constraints(result_obj, algo_type):
    has_num = "num" in result_obj
    has_threshold = "threshold" in result_obj
    
    if has_num and has_threshold:
        raise ValueError('"num" and "threshold" cannot be set at the same time')
    if not has_num and not has_threshold:
        raise ValueError('either "num" or "threshold" must be provided')

    if has_threshold:
        try:
            t = float(result_obj["threshold"])
        except (ValueError, TypeError, KeyError):
            raise ValueError('"result.threshold" must be a number')

        if not np.isfinite(t):
            raise ValueError('"result.threshold" cannot be NaN or Inf')

        if algo_type == "dtw" and t < 0:
            raise ValueError('for dtw algorithm, "result.threshold" must be non-negative')
        if algo_type == "cosine" and (t < -1 or t > 1):
            raise ValueError('for cosine similarity, "result.threshold" must be in range [-1, 1]')

    top_n = None
    if has_num:
        try:
            top_n = int(result_obj["num"])
        except (ValueError, TypeError, KeyError):
            raise ValueError('"result.num" must be an integer')
        if top_n < ProfileSearchLimits.MIN_PROFILE_SEARCH_RESULTS or top_n > ProfileSearchLimits.MAX_PROFILE_SEARCH_RESULTS:
            raise ValueError(f'"result.num" must be in range [{ProfileSearchLimits.MIN_PROFILE_SEARCH_RESULTS}, {ProfileSearchLimits.MAX_PROFILE_SEARCH_RESULTS}]')

    return has_threshold, top_n


def _validate_source_data(source_data):
    source_arr = np.array(source_data, dtype=float)
    if source_arr.ndim != 1 or source_arr.size == 0:
        raise ValueError('"source_data" must be a non-empty 1-D numeric array')

    if source_arr.size > ProfileSearchLimits.MAX_SOURCE_LEN:
        raise ValueError(
            f'"source_data" length {source_arr.size} exceeds maximum allowed '
            f'({ProfileSearchLimits.MAX_SOURCE_LEN})'
        )

    if not np.all(np.isfinite(source_arr)):
        raise ValueError('"source_data" contains NaN or Inf')

    return source_arr


def _parse_source_data(source_data):
    # Keep backward compatibility with legacy source_data: [1, 2, 3]
    if isinstance(source_data, dict):
        if "data" not in source_data:
            raise ValueError('"source_data.data" is required when "source_data" is an object')

        source_arr = _validate_source_data(source_data.get("data"))

        source_ts_window = None
        source_ts = source_data.get("ts")
        if source_ts is not None:
            if isinstance(source_ts, (list, tuple)) and len(source_ts) == source_arr.size:
                source_ts_window = [_np_scalar(source_ts[0]), _np_scalar(source_ts[-1])]
            else:
                raise ValueError('"source_data.ts" must have the same length as "source_data.data"')

        return source_arr, source_ts_window

    return _validate_source_data(source_data), None


def _validate_profile_list(profile_list, source_len, algo_type):
    if len(profile_list) > ProfileSearchLimits.MAX_PROFILES:
        raise ValueError(
            f'"target_data.data" has too many profiles ({len(profile_list)}); '
            f'max is {ProfileSearchLimits.MAX_PROFILES}'
        )

    total_len = 0

    for idx, profile in enumerate(profile_list):
        profile_arr = np.array(profile, dtype=float)

        if profile_arr.ndim != 1 or profile_arr.size == 0:
            raise ValueError("input series must be a non-empty 1-D numeric array")
        
        if not np.all(np.isfinite(profile_arr)):
            raise ValueError(f'"target_data.data[{idx}]" contains NaN or Inf')
        
        if algo_type == 'cosine' and profile_arr.size != source_len:
            raise ValueError(f'for cosine similarity, each profile in "target_data.data" must have the same length as "source_data" ({source_len})')

        total_len += profile_arr.size

    if total_len > ProfileSearchLimits.MAX_TARGET_LEN:
        raise ValueError(
            f'total length of all profiles in "target_data.data" ({total_len}) exceeds maximum allowed '
            f'({ProfileSearchLimits.MAX_TARGET_LEN})'
        )


def _validate_min_max_window(min_window, max_window):
    if min_window is not None:
        min_window = int(min_window)
    if max_window is not None:
        max_window = int(max_window)

    if min_window is not None and min_window < ProfileSearchLimits.MIN_WINDOW:
        raise ValueError(f'"min_window" must be greater than or equal to {ProfileSearchLimits.MIN_WINDOW}')
    if max_window is not None and max_window < ProfileSearchLimits.MIN_WINDOW:
        raise ValueError(f'"max_window" must be greater than or equal to {ProfileSearchLimits.MIN_WINDOW}')
    if min_window is not None and max_window is not None and min_window > max_window:
        raise ValueError(f'"min_window" cannot be larger than "max_window"')

    return min_window, max_window


def _validate_radius(algo_type, algo_params):
    if algo_type == "dtw":
        radius = int(algo_params.get("radius", ProfileSearchLimits.MIN_RADIUS))
        if radius < ProfileSearchLimits.MIN_RADIUS or radius > ProfileSearchLimits.MAX_RADIUS:
            raise ValueError(
                f"radius value out of range, valid range [{ProfileSearchLimits.MIN_RADIUS}, {ProfileSearchLimits.MAX_RADIUS}]"
            )
        return radius

    return None

def _validate_integer_param(algo_params, param_name, default_val: int, min_val: int = 1):
    if default_val < min_val:
        raise ValueError("default_val should be >= min_val")

    raw_val = algo_params.get(param_name, None)
    if raw_val is None:
        raw_val = default_val
    else:
        if isinstance(raw_val, bool):
            raise ValueError(f'"{param_name}" must be an integer')
        if not isinstance(raw_val, int):
            raise ValueError(f'"{param_name}" must be an integer')
    if raw_val < min_val:
        raise ValueError(f'"{param_name}" must be greater than or equal to {min_val}')

    return raw_val

def _validate_window_steps(algo_type, algo_params):
    has_window_size_step = "window_size_step" in algo_params

    if algo_type != "dtw" and has_window_size_step:
        raise ValueError('"window_size_step" can only be set for dtw algorithm')

    window_size_step = _validate_integer_param(algo_params, "window_size_step", 1, 1)
    window_sliding_step = _validate_integer_param(algo_params, "window_sliding_step", 1, 1)

    return window_size_step, window_sliding_step


def _normalize_series(series_arr, norm_type):
    arr = np.array(series_arr, dtype=float)
    
    if norm_type == NormalizationMethod.NONE.value:
        return arr
    if norm_type == NormalizationMethod.CENTERING.value:
        return arr - float(np.mean(arr))
    if norm_type == NormalizationMethod.Z_SCORE.value:
        std = float(np.std(arr))
        if std == 0:
            return np.zeros_like(arr)
        return (arr - float(np.mean(arr))) / std
    if norm_type == NormalizationMethod.MIN_MAX.value:
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


def _build_window_candidates_from_series(ts_vals, data_vals, source_len: int, min_window: int, max_window: int,
                                        window_size_step: int, window_sliding_step: int,
                                        exclude_source: bool = False, source_ts_window=None):
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
    if min_w > int(data_arr.size):
        raise ValueError(
            f'min_window ({min_w}) exceeds target series length ({int(data_arr.size)}); '
            'reduce min_window or use a longer target series'
        )

    for win_size in range(min_w, max_w + 1, window_size_step):
        for start in range(0, data_arr.size - win_size + 1, window_sliding_step):
            end = start + win_size - 1
            assert end < data_arr.size and end < ts_arr.size

            if exclude_source and source_ts_window is not None:
                if ts_arr[start] <= source_ts_window[0] and ts_arr[end] >= source_ts_window[1]:
                    continue

            yield {
                "series": data_arr[start:end + 1],
                "ts_window": [_np_scalar(ts_arr[start]), _np_scalar(ts_arr[end])],
                "num": end - start + 1
            }


def _build_candidates_from_profiles(ts_vals, profiles, min_window, max_window,
                                    exclude_source=False, source_ts_window=None):
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

        if exclude_source and source_ts_window is not None:
            if ts_window[0] <= source_ts_window[0] and ts_window[1] >= source_ts_window[1]:
                continue

        has_candidate = True
        yield {
            "series": profile_arr,
            "ts_window": ts_window,
            "num": profile_arr.size
        }

    if not has_candidate:
        raise ValueError("no candidate profiles after min_window/max_window filtering")


def _parse_profile_search_input(req_json):
    norm_type = _validate_normalization(req_json.get("normalization", "none"))

    algo_obj = req_json.get("algo", None)
    if algo_obj is None or not isinstance(algo_obj, dict):
        raise ValueError('"algo" object is required and must be a dictionary')
    
    algo_type = str(algo_obj.get("type", "dtw")).lower()
    if algo_type not in {"dtw", "cosine"}:
        raise ValueError(f"unsupported algo: {algo_type}")

    algo_params = algo_obj.get("params", {})
    if algo_params is None:
        algo_params = {}

    result_obj = req_json.get("result", {})
    if not isinstance(result_obj, dict):
        raise ValueError('"result" must be a dictionary if provided')

    source_data = req_json.get("source_data", None)
    target_data = req_json.get("target_data", None)

    if source_data is None or target_data is None:
        raise ValueError('"source_data" and "target_data" are required')

    ts_list = target_data.get("ts", None) if isinstance(target_data, dict) else None
    data_list = target_data.get("data", None) if isinstance(target_data, dict) else None

    if ts_list is None or data_list is None:
        raise ValueError('"target_data.ts" and "target_data.data" are required')

    return {
        "norm_type": norm_type,
        "algo_type": algo_type,
        "algo_params": algo_params,
        "result_obj": result_obj,
        "source_data": source_data,
        "ts_list": ts_list,
        "data_list": data_list,
    }


def _validate_bool_field(result_obj, field_name):
    val = result_obj.get(field_name, False)
    if isinstance(val, bool):
        return val
    raise ValueError(f'"result.{field_name}" must be a boolean')


def _validate_params(parsed_input):
    norm_type = parsed_input["norm_type"]
    algo_type = parsed_input["algo_type"]
    algo_params = parsed_input["algo_params"]
    result_obj = parsed_input["result_obj"]
    ts_list = parsed_input["ts_list"]
    data_list = parsed_input["data_list"]

    if algo_type != "dtw" and ("min_window" in algo_params or "max_window" in algo_params):
        raise ValueError('"min_window" and "max_window" can only be set for dtw algorithm')
    
    has_threshold, top_n = _validate_result_constraints(result_obj, algo_type)
    source_arr, source_ts_window = _parse_source_data(parsed_input["source_data"])

    exclude_source = _validate_bool_field(result_obj, "exclude_source")
    exclude_overlap = _validate_bool_field(result_obj, "exclude_overlap")

    min_window, max_window = _validate_min_max_window(
        algo_params.get("min_window", None),
        algo_params.get("max_window", None),
    )
    
    window_size_step, window_sliding_step = _validate_window_steps(algo_type, algo_params)

    is_profile_list = isinstance(data_list, list) and len(data_list) > 0 and isinstance(data_list[0], (list, tuple))
    if is_profile_list:
        _validate_profile_list(data_list, source_arr.size, algo_type)
    else:
        data_arr = np.array(data_list, dtype=float)
        if data_arr.size > ProfileSearchLimits.MAX_TARGET_LEN:
            raise ValueError(
                f'"target_data.data" length {data_arr.size} exceeds maximum allowed '
                f'({ProfileSearchLimits.MAX_TARGET_LEN})'
            )
        if not np.all(np.isfinite(data_arr)):
            raise ValueError('"target_data.data" contains NaN or Inf')

        _validate_possible_candidates(source_arr, data_arr.size, min_window, max_window,
                                    window_size_step, window_sliding_step)

    radius = _validate_radius(algo_type, algo_params)

    return {
        "norm_type": norm_type,
        "algo_type": algo_type,
        "result_obj": result_obj,
        "has_threshold": has_threshold,
        "top_n": top_n,
        "source_arr": source_arr,
        "source_ts_window": source_ts_window,
        "ts_list": ts_list,
        "data_list": data_list,
        "radius": radius,
        "min_window": min_window,
        "max_window": max_window,
        "window_size_step": window_size_step,
        "window_sliding_step": window_sliding_step,
        "exclude_source": exclude_source,
        "exclude_overlap": exclude_overlap,
        "is_profile_list": is_profile_list
    }

def _validate_possible_candidates(source_arr, data_list_size, min_window, max_window,
                                  window_size_step, window_sliding_step):
    eff_min_w = min_window if min_window is not None else int(source_arr.size)
    eff_max_w = max_window if max_window is not None else int(source_arr.size)

    eff_min_w = min(eff_min_w, data_list_size)
    eff_max_w = min(eff_max_w, data_list_size)

    if eff_min_w > 0 and eff_max_w >= eff_min_w:
        total_candidates = 0
        for win_size in range(int(eff_min_w), int(eff_max_w) + 1, window_size_step):
            candidate_count_for_size = (data_list_size - win_size) // window_sliding_step + 1
            total_candidates += max(0, candidate_count_for_size)

        if total_candidates > ProfileSearchLimits.MAX_WINDOW_CANDIDATES:
            raise ValueError(
                f'sliding window would generate {total_candidates} candidates, '
                f'which exceeds the maximum of {ProfileSearchLimits.MAX_WINDOW_CANDIDATES}; '
                f'reduce target_data length or narrow the window range'
            )


def _is_interval_overlapping(window_a, window_b):
    """Return whether ``window_a`` and ``window_b`` overlap ([1,5] and [5, 8] don't overlap)."""
    return window_a[0] < window_b[1] and window_b[0] < window_a[1]


def _filter_exclude_overlap(matches, limit=None):
    """Greedily keep matches whose ts_window does not overlap with any already-kept match.

    matches must be sorted best-first. For each candidate, it is discarded if its
    ts_window overlaps with an already-kept match's ts_window (adjacent windows
    sharing only an endpoint are not considered overlapping).
    """
    if len(matches) <= 1:
        return matches

    kept = []  # list of (ts_window, original_index)

    for idx, match in enumerate(matches):
        ts_window = match.get("ts_window")
        if not isinstance(ts_window, (list, tuple)) or len(ts_window) != 2:
            raise ValueError(f'matches[{idx}].ts_window must be a [start_ts, end_ts] pair')

        has_overlap = any(
            _is_interval_overlapping(ts_window, k_window)
            for k_window, _ in kept
        )

        if not has_overlap:
            kept.append((ts_window, idx))

            if limit is not None and len(kept) >= limit:
                break

    kept_indices = {k[1] for k in kept}
    return [m for i, m in enumerate(matches) if i in kept_indices]


# When exclusion filters are active, the heap is oversampled by this factor so
# that filtering still yields target_rows results in most cases.
_EXCLUSION_OVERSAMPLE = 8

def _heap_key(algo_type, criteria_val, seq_idx):
    # Higher heap key means a better candidate after normalization of the metric:
    # cosine uses the raw similarity (higher is better), while DTW inverts the
    # distance (lower is better) so both algorithms can share the same heap comparison logic.
    if algo_type == "dtw":
        return (-criteria_val, -seq_idx)
    return (criteria_val, -seq_idx)

def do_profile_search_impl(req_json):
    parsed_input = _parse_profile_search_input(req_json)
    parsed = _validate_params(parsed_input)

    norm_type = parsed["norm_type"]
    algo_type = parsed["algo_type"]
    result_obj = parsed["result_obj"]
    has_threshold = parsed["has_threshold"]
    top_n = parsed["top_n"]
    source_arr = parsed["source_arr"]
    source_ts_window = parsed["source_ts_window"]
    ts_list = parsed["ts_list"]
    data_list = parsed["data_list"]
    radius = parsed["radius"]
    min_window = parsed["min_window"]
    max_window = parsed["max_window"]
    window_size_step = parsed["window_size_step"]
    window_sliding_step = parsed["window_sliding_step"]
    exclude_source = parsed["exclude_source"]
    exclude_overlap = parsed["exclude_overlap"]

    source_norm = _normalize_series(source_arr, norm_type)
    metric_type = "dtw_distance" if algo_type == "dtw" else "cosine_similarity"
    threshold = float(result_obj["threshold"]) if has_threshold else None
    target_rows = ProfileSearchLimits.MAX_PROFILE_SEARCH_RESULTS if top_n is None else top_n
    need_exclusion_filter = exclude_overlap

    def _build_candidates():
        if parsed["is_profile_list"]:
            return _build_candidates_from_profiles(
                ts_list, data_list, min_window, max_window,
                exclude_source=exclude_source, source_ts_window=source_ts_window
            )
        return _build_window_candidates_from_series(
            ts_list, data_list, source_arr.size, min_window, max_window,
            window_size_step, window_sliding_step,
            exclude_source=exclude_source, source_ts_window=source_ts_window
        )

    # Score all candidates once.
    # - Without exclude_overlap: stream results directly into a fixed-size heap,
    #   discarding weaker candidates on the fly.  No retry is needed so there is no
    #   reason to accumulate a separate all_passed list.
    # - With exclude_overlap: every passing result is saved in all_passed so that
    #   the retry loop can rebuild the heap with a larger limit without recomputing
    #   any distances.
    all_passed = [] if need_exclusion_filter else None
    top_heap = [] if not need_exclusion_filter else None
    seq = 0

    for item in _build_candidates():
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

        key = _heap_key(algo_type, criteria, seq)
        heap_item = (key, seq, match_obj)

        if need_exclusion_filter:
            all_passed.append(heap_item)
        else:
            if len(top_heap) < target_rows:
                heapq.heappush(top_heap, heap_item)
            elif key > top_heap[0][0]:
                heapq.heapreplace(top_heap, heap_item)

    if not need_exclusion_filter:
        # The heap already holds the top target_rows results.  Sort and return.
        if algo_type == "dtw":
            top_heap.sort(key=lambda x: (x[2]["criteria"], x[1]))
        else:
            top_heap.sort(key=lambda x: (-x[2]["criteria"], x[1]))
        matches = [x[2] for x in top_heap]
    else:
        # Exclusion filters are active: rebuild the heap from all_passed with a
        # progressively larger heap_limit until filtering yields enough results.
        oversample = _EXCLUSION_OVERSAMPLE
        matches = []
        total_passed = len(all_passed)

        while True:
            heap_limit = target_rows * oversample

            top_heap = []
            for heap_item in all_passed:
                key = heap_item[0]
                if len(top_heap) < heap_limit:
                    heapq.heappush(top_heap, heap_item)
                elif key > top_heap[0][0]:
                    heapq.heapreplace(top_heap, heap_item)

            if algo_type == "dtw":
                top_heap.sort(key=lambda x: (x[2]["criteria"], x[1]))
            else:
                top_heap.sort(key=lambda x: (-x[2]["criteria"], x[1]))

            matches = [x[2] for x in top_heap]

            matches = _filter_exclude_overlap(matches, limit=target_rows)

            # Got enough results, or all passing candidates already fit in the heap.
            if len(matches) >= target_rows or total_passed <= heap_limit:
                break

            # The heap was saturated and filtering removed too many entries.
            # Double the oversample factor and rebuild from the cached scored list.
            oversample *= 2

    return {
        "rows": len(matches),
        "metric_type": metric_type,
        "matches": matches
    }
