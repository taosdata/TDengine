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

"""PyOD anomaly detection helpers."""

import numpy as np


def validate_contamination(value):
    """Validate and return the contamination parameter."""
    try:
        contamination = float(value)
    except (ValueError, TypeError):
        raise ValueError(f"contamination must be a number, got: {value!r}")
    if contamination <= 0 or contamination > 0.5:
        raise ValueError("contamination value out of range, valid range (0, 0.5]")
    return contamination


def validate_positive_int(value, name):
    """Parse value as a positive integer, raising ValueError with a clear message on failure."""
    try:
        result = int(value)
    except (ValueError, TypeError):
        raise ValueError(f"{name} must be an integer, got: {value!r}")
    if result <= 0:
        raise ValueError(f"{name} should be greater than 0")
    return result


def validate_int_min(value, name, minimum):
    """Parse value as an integer >= minimum."""
    try:
        result = int(value)
    except (ValueError, TypeError):
        raise ValueError(f"{name} must be an integer, got: {value!r}")
    if result < minimum:
        raise ValueError(f"{name} should be >= {minimum}")
    return result


def validate_nonneg_int(value, name):
    """Parse value as a non-negative integer, raising ValueError with a clear message on failure."""
    try:
        result = int(value)
    except (ValueError, TypeError):
        raise ValueError(f"{name} must be an integer, got: {value!r}")
    if result < 0:
        raise ValueError(f"{name} must be >= 0")
    return result


def validate_positive_float(value, name):
    """Parse value as a positive float, raising ValueError with a clear message on failure."""
    try:
        result = float(value)
    except (ValueError, TypeError):
        raise ValueError(f"{name} must be a number, got: {value!r}")
    if result <= 0:
        raise ValueError(f"{name} should be greater than 0")
    return result


def validate_bool_int(value, name):
    """Parse a boolean parameter accepting 0/1 or true/false/yes/no (case-insensitive)."""
    if isinstance(value, bool):
        return value
    s = str(value).strip().lower()
    if s in ("1", "true", "yes"):
        return True
    if s in ("0", "false", "no"):
        return False
    raise ValueError(f"{name} must be 0/1 or true/false, got: {value!r}")


def fit_predict(detector, input_data_lists, valid_code):
    """Run a PyOD detector and map labels to TDgpt anomaly codes."""
    arr_2d = np.asarray(input_data_lists, dtype=float).T
    detector.fit(arr_2d)
    return [-1 if label == 1 else valid_code for label in detector.labels_]
