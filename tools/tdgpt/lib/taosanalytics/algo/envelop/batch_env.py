# Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
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

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
from scipy.interpolate import interp1d
from scipy.signal import savgol_filter

from lib.conf import Configure
from lib.log import AppLogger


############################################
# Hampel Filter
############################################
def hampel_filter(values, window_size=7, n_sigmas=3):
    if window_size <= 0 or n_sigmas > 3:
        AppLogger.error(
            f"invalid parameters for hampel filter, window size:{window_size}, sigma:{n_sigmas}"
        )
        raise ValueError(
            f"invalid parameters for hampel filter, window size:{window_size}, sigma:{n_sigmas}"
        )

    values = np.array(values)
    new_vals = values.copy()

    outlier_indices = []

    k = 1.4826  # scale factor

    for i in range(len(values)):
        start = max(i - window_size, 0)
        end = min(i + window_size, len(values))

        window = values[start:end]
        median = np.median(window)
        mad = k * np.median(np.abs(window - median))

        if mad == 0:
            continue

        if abs(values[i] - median) > n_sigmas * mad:
            new_vals[i] = median
            outlier_indices.append(i)

    return new_vals, np.array(outlier_indices)


############################################
# Derivative Check
############################################
def derivative_check(time, values, max_rate=np.inf):
    time = np.array(time)
    values = np.array(values)

    dt = np.diff(time)
    dv = np.diff(values)

    rate = np.zeros_like(values)
    rate[1:] = dv / dt

    mask = np.abs(rate) <= max_rate

    # Keep the first point
    mask[0] = True

    return time[mask], values[mask]


############################################
# Progress Normalization
############################################
def normalize_progress(time, values, target_len=1000, method="linear"):
    time_range = time.max() - time.min()
    if time_range > 0:
        t_norm = (time - time.min()) / time_range
    else:
        t_norm = np.zeros_like(time, dtype=float)

    if method == "cubic" and len(values) >= 4:
        kind = "cubic"
    else:
        kind = "linear"

    f = interp1d(t_norm, values, kind=kind, fill_value="extrapolate")

    new_t = np.linspace(0, 1, target_len)
    new_vals = f(new_t)

    return new_t, new_vals


############################################
# Plot helper
############################################
def plot_compare(before, after, title, save_dir):
    plt.figure(figsize=(12, 8))
    plt.plot(before, label="Before", alpha=0.6, linewidth=1)
    plt.plot(after, label="After", linewidth=1)

    plt.title(title, fontsize=14, fontweight="bold")

    plt.legend()
    plt.tight_layout()
    plt.grid(True, alpha=0.3)

    Path(save_dir).mkdir(parents=True, exist_ok=True)
    plt.savefig(f"{save_dir}/{title}.png")
    plt.close()


def plot_hempel_filter(before, after, outlier_indices, info, save_dir):
    title = "1_hampel"

    plt.figure(figsize=(12, 8))

    plt.plot(before, "b-", label="Before", alpha=0.7, linewidth=1)
    plt.plot(after, "g-", label="After", linewidth=2)

    if outlier_indices.size > 0:
        outlier_values = before[outlier_indices]
        plt.scatter(
            outlier_indices,
            outlier_values,
            color="red",
            s=100,
            marker="o",
            edgecolors="darkred",
            linewidth=1.5,
            label=f"Detected Anomaly Points: ({len(outlier_indices)})",
            zorder=5,
        )

    plt.title(title + f" ({info})", fontsize=14, fontweight="bold")

    plt.legend()
    plt.tight_layout()
    plt.grid(True, alpha=0.3)

    Path(save_dir).mkdir(parents=True, exist_ok=True)
    plt.savefig(f"{save_dir}/{title}.png")
    plt.close()


def plot_sg_smoothing(before, after, title, info, save_dir):
    plt.figure(figsize=(12, 8))

    plt.subplot(2, 1, 1)

    plt.plot(before, "b-", alpha=0.7, linewidth=1, label="Raw Data")
    plt.plot(after, "r-", linewidth=2, label="After SG Smoothing")

    plt.title(title + " (" + info + ")", fontsize=14, fontweight="bold")
    plt.grid(True, alpha=0.3)
    plt.legend()

    # draw difference
    plt.subplot(2, 1, 2)
    difference = after - before
    plt.plot(difference, "g-", linewidth=1)
    plt.xlabel("X")
    plt.ylabel("Diff")
    plt.title("Diff between raw data and smoothed data")
    plt.grid(True, alpha=0.3)

    plt.tight_layout()

    Path(save_dir).mkdir(parents=True, exist_ok=True)
    plt.savefig(f"{save_dir}/{title}.png")
    plt.close()


############################################
# Window Split
############################################
def split_by_windows(time, values, windows):
    segments = []

    for start, end in windows:
        mask = (time >= start) & (time <= end)
        segments.append((time[mask], values[mask]))

    return segments


############################################
# Golden Batch
############################################
def build_golden(batches, config):
    batches = np.array(batches)

    method = config["golden"]["method"]

    if method == "mean_std":
        mean = np.mean(batches, axis=0)
        std = np.std(batches, axis=0)

        upper = mean + config["golden"].get("n_std", 2) * std
        lower = mean - config["golden"].get("n_std", 2) * std

        center = mean

    else:  # median_percentile
        median = np.median(batches, axis=0)

        p_low = config["golden"].get("lower_percentile", 10)
        p_high = config["golden"].get("upper_percentile", 90)

        lower = np.percentile(batches, p_low, axis=0)
        upper = np.percentile(batches, p_high, axis=0)

        center = median

    return center, lower, upper


############################################
# Main Pipeline
############################################
def gen_batch_envelop(ts_list, val_list, win_list, config):
    segments = split_by_windows(ts_list, val_list, win_list)

    processed_batches = []

    for idx, (t, v) in enumerate(segments):

        if len(v) < 10:
            if t.size >= 2:
                AppLogger.warning(
                    f"data points less than threshold, discard time window [{t[0]}, {t[1]}]"
                )
            else:
                AppLogger.warning(
                    "data points less than threshold, discard empty time window"
                )
            continue

        conf = Configure.get_instance()
        save_dir = f"{conf.get_img_dir()}/batch_{idx}"

        # Hampel filter
        if config["hampel"]["active"]:
            v_hampel, outlier_indices = hampel_filter(
                v,
                config["hampel"]["window_size"],
                config["hampel"]["sigma"],
            )

            if conf.get_draw_result_option():
                info = f'window_size: {config["hampel"]["window_size"]}, sigma:{config["hampel"]["sigma"]}'
                plot_hempel_filter(v, v_hampel, outlier_indices, info, save_dir)
        else:
            v_hampel = v

        # Derivative check
        if config["derivative"]["active"]:
            threshold = config["derivative"].get("max_rate", np.inf)
            if threshold <= 0:
                AppLogger.warning("the max rate is set to 0, no results generated")
                return np.array([]), np.array([]), np.array([]), np.array([])

            t_der, v_der = derivative_check(
                t,
                v_hampel,
                config["derivative"].get("max_rate", np.inf),
            )

            if conf.get_draw_result_option():
                plot_compare(v_hampel, v_der, "2_derivative", save_dir)
        else:
            t_der = t
            v_der = v_hampel

        # Normalize
        t_norm, v_norm = normalize_progress(
            t_der,
            v_der,
            config["normalize"]["target_len"],
            config["normalize"]["method"],
        )

        if conf.get_draw_result_option():
            plot_compare(v_der, v_norm, "3_normalize", save_dir)

        # SG smoothing
        if config["savgol"]["active"]:
            sg_cfg = config["savgol"]
            if sg_cfg["window"] % 2 == 0 or sg_cfg["window"] <= 0:
                AppLogger.error(
                    f"Savitzky-Golay window must be a positive odd integer, input size: {sg_cfg['window']}"
                )
                raise ValueError(
                    f"Savitzky-Golay window must be a positive odd integer, input size: {sg_cfg['window']}"
                )

            v_sg = savgol_filter(
                v_norm,
                window_length=sg_cfg["window"],
                polyorder=sg_cfg["polyorder"],
            )

            if conf.get_draw_result_option():
                info = (
                    f'window_size: {sg_cfg["window"]}, poly_order:{sg_cfg["polyorder"]}'
                )
                plot_sg_smoothing(v_norm, v_sg, "4_savgol", info, save_dir)
        else:
            v_sg = v_norm

        processed_batches.append(v_sg)

    AppLogger.debug(
        "total %d time windows data to build golden batch results"
        % (len(processed_batches))
    )

    if len(processed_batches) <= 0:
        AppLogger.warning(
            "empty results return since no valid input time window for golden batch process"
        )
        return np.array([]), np.array([]), np.array([]), np.array([])

    # main process
    center, lower, upper = build_golden(processed_batches, config)

    if conf.get_draw_result_option():
        plt.figure(figsize=(12, 7))

        for b in processed_batches:
            plt.plot(b, color="gray", alpha=0.2)

        plt.plot(center, label="Golden", linewidth=2)
        plt.fill_between(
            np.arange(len(center)),
            lower,
            upper,
            alpha=0.3,
            label="Envelope",
        )

        plt.legend()

        plt.title(f'Golden Batch with Envelope ({config["golden"]["method"]})')
        Path(save_dir).mkdir(parents=True, exist_ok=True)
        plt.savefig(f"{save_dir}/golden_batch.png")
        plt.close()

    AppLogger.debug(
        f"build golden batch completed, center: {center}, lower:{lower}, upper:{upper}"
    )
    return center, lower, upper, processed_batches


def get_default_config():
    default_config = {
        "plot": False,
        "hampel": {
            "window_size": 7,
            "sigma": 3,
            "active": True,
        },
        "derivative": {
            "max_rate": 50,
            "active": True,
        },
        "normalize": {
            "target_len": 1000,
            # linear|cubic
            "method": "linear",
        },
        "savgol": {
            "window": 9,
            "polyorder": 2,
            "active": True,
        },
        "golden": {
            # median_percentile + lower_percentile/upper_percentile|mean_std + nstd
            "method": "median_percentile",
            "lower_percentile": 10,
            "upper_percentile": 90,
        },
    }

    return default_config


def update_config(param) -> dict:
    config = get_default_config()

    if param is not None:
        for key, value in param.items():
            if key == "plot":
                AppLogger.warning("Ignoring request-supplied 'plot' key: plot is controlled by server configuration only")
                continue
            if key not in config:
                AppLogger.warning(f"Ignoring unknown config key: {key}")
                continue

            # Validate and update config with type/range checks
            if isinstance(config[key], dict) and isinstance(value, dict):
                # Nested config (e.g., hampel, savgol, golden, normalize)
                for nested_key, nested_value in value.items():
                    if nested_key not in config[key]:
                        AppLogger.warning(f"Ignoring unknown nested config key: {key}.{nested_key}")
                        continue

                    # Validate based on known parameter constraints
                    if nested_key == "window_size":
                        if not isinstance(nested_value, int) or nested_value < 1 or nested_value > 100:
                            AppLogger.error(f"Invalid window_size: {nested_value}, must be int in range [1, 100]")
                            continue
                    elif nested_key == "sigma":
                        if not isinstance(nested_value, (int, float)) or nested_value < 1 or nested_value > 3:
                            AppLogger.error(f"Invalid sigma: {nested_value}, must be in range [1, 3]")
                            continue
                    elif nested_key == "polyorder":
                        if not isinstance(nested_value, int) or nested_value < 0 or nested_value > 5:
                            AppLogger.error(f"Invalid polyorder: {nested_value}, must be int in range [0, 5]")
                            continue
                    elif nested_key == "window":
                        if not isinstance(nested_value, int) or nested_value < 3 or nested_value > 100000:
                            AppLogger.error(f"Invalid window: {nested_value}, must be int in range [3, 100000]")
                            continue
                    elif nested_key == "target_len":
                        if not isinstance(nested_value, int) or nested_value < 100 or nested_value > 100000:
                            AppLogger.error(f"Invalid target_len: {nested_value}, must be int in range [100, 100000]")
                            continue
                    elif nested_key == "method":
                        if not isinstance(nested_value, str) or nested_value not in {"linear", "cubic"}:
                            AppLogger.error(f"Invalid method: {nested_value}, must be 'linear' or 'cubic'")
                            continue
                    elif nested_key in {"lower_percentile", "upper_percentile"}:
                        if not isinstance(nested_value, (int, float)) or nested_value < 0 or nested_value > 100:
                            AppLogger.error(f"Invalid percentile {nested_key}: {nested_value}, must be in range [0, 100]")
                            continue

                    config[key][nested_key] = nested_value
            else:
                # Top-level scalar config - validate type matches default
                if not isinstance(value, type(config[key])):
                    AppLogger.error(f"Type mismatch for config key {key}: expected {type(config[key]).__name__}, "
                                    f"got {type(value).__name__}")
                    continue
                config[key] = value

    AppLogger.debug(f"conf for batch process: {config}")
    return config


############################################
# unit test
############################################
if __name__ == "__main__":
    # synthetic data set for test purpose
    time = np.linspace(0, 100, 5000)
    values = np.sin(time / 10) + np.random.normal(0, 0.1, 5000)

    windows = [
        (0, 18),
        (20, 40),
        (40, 50),
        (60, 80),
        (84, 100),
    ]

    center, lower, upper, batches = gen_batch_envelop(
        time, values, windows, get_default_config()
    )
