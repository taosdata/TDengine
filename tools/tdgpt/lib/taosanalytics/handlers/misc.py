# Copyright (c) 2025 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

"""misc handlers: encapsulates miscellaneous tool business logic"""

import math

import numpy as np
from scipy.stats import pearsonr

from lib.log import AppLogger
from lib.service_registry import loader
from lib.taosanalytics.algo.envelop.batch_algorithms import (
    DEFAULT_ALGO,
    BatchAlgoParamsError,
    resolve_params,
)
from lib.taosanalytics.algo.envelop.batch_env import (
    gen_batch_envelop,
    get_default_config,
    update_config,
)
from lib.taosanalytics.algo.search.profile_search import do_profile_search_impl
from lib.util import (
    SINGLE_COLUMN_ERROR_MSG,
    do_check_before_exec,
    do_initial_check,
    get_more_data_list,
)

# Accepted size of one batch-envelope request. The pipeline resamples every
# batch to `normalize.target_len` samples (1000 by default) and runs an O(n)
# Hampel filter over it, so both ends are bounded: below the minimum there is not
# enough signal to build a baseline from, above it the extra points only cost
# time without changing the produced envelope.
_MIN_BATCH_INPUT_POINTS = 10
_MAX_BATCH_INPUT_POINTS = 40000

# Range of a signed 64-bit timestamp. `ts` stays an integer (it is a unix
# timestamp, and float64 stops being exact above 2**53), so an entry outside this
# range would make `np.array(ts)` fall back to `object` dtype.
_INT64_MIN = -(2**63)
_INT64_MAX = 2**63 - 1


def _check_timestamp(index, value):
    """Return why one `ts` entry cannot be used as a timestamp, or None.

    Timestamps are never converted to float: they are unix timestamps, and
    float64 stops representing integers exactly above 2**53, which would silently
    move window boundaries. They are only checked, because `np.array(ts)` widens
    the whole column to `object` dtype as soon as one entry is not a number --
    and every comparison or subtraction downstream (`time >= start`,
    `np.diff(time)`) then fails with a TypeError.
    """
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return (
            f"'ts' entry at index {index} must be a number, "
            f"got {type(value).__name__}: {value!r}."
        )

    if isinstance(value, int):
        if not (_INT64_MIN <= value <= _INT64_MAX):
            return (
                f"'ts' entry at index {index} does not fit a 64-bit timestamp: "
                f"{value!r}."
            )
        return None

    if not math.isfinite(value):
        return f"'ts' entry at index {index} must be finite, got {value!r}."

    return None


def _prepare_batch_input(ts, data):
    """Validate `ts` and `data` and return `data` as floats, or the reason why not.

    `ts` and `data` are paired by position: split_by_windows builds its mask from
    `ts` and applies the same mask to `data`, so a length difference either raises
    IndexError deep inside NumPy or, worse, pairs values with the wrong timestamp.

    Every entry of `data` is then fed to median/diff/interpolation arithmetic, so
    the values are converted here instead of leaving it to `np.array(data)`:

      * a non-number would fail inside NumPy with a message the client cannot act
        on -- and `bool` is rejected explicitly, since it is an int subclass and
        would silently be treated as 0/1;
      * an int too wide for float (e.g. ``2 ** 1024``) cannot be represented, and
        ``np.array`` would widen the whole column to ``object`` dtype, which
        breaks ``np.abs``/``np.median`` further down with a TypeError;
      * NaN and Inf pass every arithmetic step without raising and would reach the
        response as a golden envelope full of NaN.

    Timestamps are checked with `_check_timestamp` instead of converted, for the
    reason stated there, and they must be in ascending order: the batches are cut
    from `ts` as received, so an unsorted series would produce a curve that has
    nothing to do with the data.

    Args:
        ts: Timestamps of the payload, as received (not converted).
        data: Measured values of the payload, as received.

    Returns:
        tuple: (error_message, values). The message is None when the input is
        usable, in which case values holds `data` converted to float.
    """
    if not isinstance(ts, (list, tuple)) or not isinstance(data, (list, tuple)):
        return (
            "'ts' and 'data' must be arrays of values, got "
            f"{type(ts).__name__} and {type(data).__name__}.",
            None,
        )

    if len(ts) != len(data):
        return (
            "'ts' and 'data' must have the same length, "
            f"got {len(ts)} and {len(data)}.",
            None,
        )

    if not data:
        return "'ts' and 'data' must not be empty.", None

    if not (_MIN_BATCH_INPUT_POINTS <= len(data) <= _MAX_BATCH_INPUT_POINTS):
        return (
            f"'data' length must be in [{_MIN_BATCH_INPUT_POINTS}, "
            f"{_MAX_BATCH_INPUT_POINTS}], got {len(data)}.",
            None,
        )

    values = []
    for index, value in enumerate(data):
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return (
                f"'data' entry at index {index} must be a number, "
                f"got {type(value).__name__}: {value!r}.",
                None,
            )

        try:
            number = float(value)
        except OverflowError:
            return (
                f"'data' entry at index {index} is too large to be measured: "
                f"{value!r}.",
                None,
            )

        if not math.isfinite(number):
            return (
                f"'data' entry at index {index} must be finite, got {value!r}.",
                None,
            )

        values.append(number)

    for index, value in enumerate(ts):
        msg = _check_timestamp(index, value)
        if msg:
            return msg, None

        # The samples are paired by position, so an out-of-order timestamp cannot
        # be repaired downstream: `split_by_windows` keeps the received order and
        # `normalize_progress` would have to drop points to interpolate at all,
        # returning a curve of a different batch. Rejected here, where the client
        # can still act on it.
        if index and value < ts[index - 1]:
            return (
                f"'ts' must be sorted in ascending order: entry at index {index} "
                f"({value!r}) is earlier than the previous one ({ts[index - 1]!r}).",
                None,
            )

    return None, values


def handle_build_batch_env(request):
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

    msg, values = _prepare_batch_input(ts, data)
    if msg:
        AppLogger.error(msg)
        return {"msg": msg, "rows": -1}

    MAX_WINDOWS = 1000
    if not isinstance(windows, list) or len(windows) > MAX_WINDOWS:
        msg = f"'window' must be a list with at most {MAX_WINDOWS} entries, "
        msg += f"got {len(windows) if isinstance(windows, list) else type(windows).__name__}"

        AppLogger.error(msg)
        return {"msg": msg, "rows": -1}

    algo = payload_obj.get("algo", None)
    if algo is None:
        algo = DEFAULT_ALGO
    if not isinstance(algo, str) or not algo.strip():
        msg = f"'algo' must be a non-empty string, got {algo!r}."
        AppLogger.error(msg)
        return {"msg": msg, "rows": -1}
    algo = algo.strip()

    ts_array = np.array(ts)
    values_array = np.array(values)

    if algo == DEFAULT_ALGO:
        conf = update_config(payload_obj.get("config", None))
        try:
            # median, lower bounding, upper bounding, processed_batches
            center, lower, upper, processed_batches = gen_batch_envelop(
                ts_array, values_array, windows, conf
            )
            res = _envelope_response(
                center,
                lower,
                upper,
                {"algo": DEFAULT_ALGO, "version": None, "entry_sha256": None},
            )
        except Exception as e:
            AppLogger.error("golden batch process failed, %s", str(e))
            return {"msg": str(e), "rows": -1}

        AppLogger.debug("batch processed result: %s", res)
        return res

    plugin = loader.get_batch_algo(algo)
    if plugin is None:
        msg = (
            f"unknown batch algorithm '{algo}', available: "
            f"{', '.join([DEFAULT_ALGO] + sorted(loader.batch_services))}"
        )
        AppLogger.error(msg)
        return {"msg": msg, "rows": -1}

    try:
        params = resolve_params(plugin, payload_obj.get("config", None))
    except BatchAlgoParamsError as e:
        AppLogger.error("invalid parameters for batch algorithm '%s': %s", algo, e)
        return {"msg": str(e), "rows": -1}

    try:
        center, lower, upper = plugin.cls().build(ts_array, values_array, windows, params)
        res = _envelope_response(center, lower, upper, plugin.trace())
    except Exception as e:
        # The plugin runs in the service process, so its failure is reported with
        # the algorithm and version that produced it instead of being retried.
        AppLogger.error(
            "batch algorithm '%s' (version=%s) failed: %s",
            algo,
            plugin.version,
            e,
        )
        return {"msg": f"batch algorithm '{algo}' failed: {e}", "rows": -1}

    AppLogger.debug(
        "batch algorithm '%s' (version=%s) result: rows=%s",
        algo,
        plugin.version,
        res["rows"],
    )
    return res


def _envelope_response(center, lower, upper, trace):
    """Validate the three curves and build the response body.

    `center`, `lower` and `upper` are returned to the client as plain lists, so
    they must be one-dimensional, of equal length and finite; a plugin returning
    anything else would otherwise surface as an opaque JSON encoding error.
    """
    arrays = []
    for label, value in (
        ("center", center),
        ("lower", lower),
        ("upper", upper),
    ):
        try:
            array = np.asarray(value, dtype=float)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"'{label}' is not a numeric array: {exc}") from exc

        if array.ndim != 1:
            raise ValueError(f"'{label}' must be one-dimensional, got shape {array.shape}")
        if not np.isfinite(array).all():
            raise ValueError(f"'{label}' contains NaN or infinite values")
        arrays.append(array)

    sizes = [array.size for array in arrays]
    if len(set(sizes)) != 1:
        raise ValueError(
            "center, lower and upper must have the same length, got "
            f"center={sizes[0]}, lower={sizes[1]}, upper={sizes[2]}"
        )

    center, lower, upper = arrays
    res = {
        "rows": int(lower.size),
        "center": center.tolist(),
        "lower": lower.tolist(),
        "upper": upper.tolist(),
    }
    res.update(trace)
    return res


def handle_list_batch_algos():
    """Describe the built-in pipeline and the user-provided batch algorithms."""
    builtin = {
        "name": DEFAULT_ALGO,
        "description": "built-in golden batch envelope pipeline",
        "version": None,
        "entry_sha256": None,
        "builtin": True,
        # The built-in pipeline consumes the nested pipeline configuration, not
        # a flat parameter schema, so it is reported under the request field
        # name instead of `params` to keep the two shapes apart.
        "config": get_default_config(),
    }
    return {"protocol": 1.0, "algo": [builtin] + loader.get_batch_algo_list()}


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
            msg = "a second data column is required for pearsonr"
        return {"msg": msg, "rows": -1}
    except Exception as e:
        return {"msg": str(e), "rows": -1}

    if api_version != "v1":
        AppLogger.error("unsupported API version: %s", api_version)
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

        AppLogger.debug(f"pearsonr correlation: {correlation}, p value: {p_value}")
        res = {
            "option": options,
            "rows": 1,
            "correlation_coefficient": correlation,
            "p_value": p_value,
        }

        return res
    except Exception as e:
        AppLogger.error("pearsonr correlation failed, %s", str(e))
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
    - "exclude_source" is applicable for all algorithms and means whether to exclude the matched profile that contains
    the source profile. For example, if the source profile has ts window [2, 4], the matched profile with ts window
    [2, 4] will be excluded if "exclude_source" is set to true.
    - "exclude_overlap" is applicable for all algorithms and means whether to exclude any matched profile that
    overlaps with a better-ranked result. For example, if there are two matched profiles with ts window [1, 5]
    and [4, 6], the profile [4, 6] will be excluded if "exclude_overlap" is set to true. Endopint-touching windows
    are treated as adjacent/non-overlapping, so windows such as [1, 5] and [5, 9] are not excluded by "exclude_overlap".
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
            "exclude_source": true,
            "exclude_overlap": true
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
    if api_version != "v1":
        AppLogger.error("unsupported API version: %s", api_version)
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
        AppLogger.error("profile search failed, %s", str(e))
        return {"msg": str(e), "rows": -1}
