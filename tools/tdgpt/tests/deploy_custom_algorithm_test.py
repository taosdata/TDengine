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

"""deploy_custom_algorithm.py helper tests"""

import argparse
import importlib.util
import json
from pathlib import Path

import pytest

SCRIPT = (
    Path(__file__).resolve().parents[1]
    / "skills"
    / "tdgpt-custom-algorithm"
    / "scripts"
    / "deploy_custom_algorithm.py"
)


def load_script():
    """import the deployment script without running its CLI"""
    spec = importlib.util.spec_from_file_location("deploy_custom_algorithm", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize(
    "values",
    [
        [5.0] * 12,
        [1.0, 1.0, 2.0],
        [0.0, 1.0],
        [float(index) for index in range(60)],
    ],
)
def test_binary_labels_always_carry_both_classes(values):
    """a median split that degenerates to one class must fall back to alternation"""
    labels = load_script().binary_labels(values)

    assert len(labels) == len(values)
    assert set(labels) == {0, 1}


def test_binary_labels_balance_ordered_series():
    """an ordered series splits at its median, not by position"""
    labels = load_script().binary_labels([float(index) for index in range(60)])

    assert labels == [0] * 30 + [1] * 30


def test_binary_labels_handle_empty_input():
    """an empty series produces no labels instead of raising"""
    assert load_script().binary_labels([]) == []


def args_with_sample(path):
    """build the CLI namespace a payload builder reads"""
    return argparse.Namespace(
        sample_file=str(path),
        values="",
        option="",
        config_json=None,
        forecast_rows=5,
        every=1000,
    )


@pytest.mark.parametrize(
    "payload",
    [
        '{"data": []}',
        '["a", "b"]',
        '{"data": [1, 2]}',
        '{"data": [[1, 2], [3]]}',
    ],
)
def test_malformed_sample_file_reports_deploy_error(tmp_path, payload):
    """a bad --sample-file fails as a deploy error instead of a traceback"""
    module = load_script()
    sample = tmp_path / "sample.json"
    sample.write_text(payload, encoding="utf-8")

    with pytest.raises(module.DeployError):
        module.build_regression_payload(args_with_sample(sample), "custom_reg")


def test_sample_file_columns_are_normalised(tmp_path):
    """a valid sample keeps both columns and derives the row count"""
    module = load_script()
    sample = tmp_path / "sample.json"
    sample.write_text('{"data": [[0, 1, 2], [1, 3, 5]]}', encoding="utf-8")

    payload = module.build_regression_payload(args_with_sample(sample), "custom_reg")

    assert payload["rows"] == 3
    assert payload["data"] == [[0.0, 1.0, 2.0], [1.0, 3.0, 5.0]]


def test_forecast_sample_requires_a_timestamp_column(tmp_path):
    """an empty timestamp column is rejected instead of being synthesised"""
    module = load_script()
    sample = tmp_path / "sample.json"
    sample.write_text('{"data": [[], [1, 2, 3]]}', encoding="utf-8")

    with pytest.raises(module.DeployError):
        module.build_fc_payload(args_with_sample(sample), "custom_fc")


def test_forecast_sample_keeps_exogenous_columns(tmp_path):
    """a full forecast sample keeps its past_dynamic_real and dynamic_real columns"""
    module = load_script()
    sample = tmp_path / "sample.json"
    sample.write_text(
        json.dumps(
            {
                "schema": [
                    ["ts", "TIMESTAMP", 8],
                    ["val", "DOUBLE", 8],
                    ["past_dynamic_real_1", "DOUBLE", 8],
                    ["dynamic_real_1", "DOUBLE", 8],
                ],
                "data": [
                    [1577808000000, 1577808001000],
                    [10.0, 10.5],
                    [1.0, 2.0],
                    [3.0, 4.0],
                ],
            }
        ),
        encoding="utf-8",
    )

    payload = module.build_fc_payload(args_with_sample(sample), "custom_fc")

    assert payload["data"] == [
        [1577808000000.0, 1577808001000.0],
        [10.0, 10.5],
        [1.0, 2.0],
        [3.0, 4.0],
    ]
    assert len(payload["data"]) == len(payload["schema"])


def test_forecast_sample_rejects_empty_timestamps_with_extra_columns(tmp_path):
    """extra columns do not make an empty timestamp column acceptable"""
    module = load_script()
    sample = tmp_path / "sample.json"
    sample.write_text(
        json.dumps(
            {
                "schema": [
                    ["ts", "TIMESTAMP", 8],
                    ["val", "DOUBLE", 8],
                    ["dynamic_real_1", "DOUBLE", 8],
                ],
                "data": [[], [1.0, 2.0], [5.0, 6.0]],
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(module.DeployError):
        module.build_fc_payload(args_with_sample(sample), "custom_fc")


def test_single_column_classification_sample_is_accepted(tmp_path):
    """classification accepts a feature-only sample, as the request handler does"""
    module = load_script()
    sample = tmp_path / "sample.json"
    sample.write_text('{"data": [[1.0, 2.0, 3.0]]}', encoding="utf-8")

    payload = module.build_classification_payload(args_with_sample(sample), "custom_cls")

    assert payload["data"] == [[1.0, 2.0, 3.0]]
    assert payload["rows"] == 3
    assert len(payload["schema"]) == 1


def test_single_column_regression_sample_is_accepted(tmp_path):
    """regression accepts a feature-only sample for a pre-trained model"""
    module = load_script()
    sample = tmp_path / "sample.json"
    sample.write_text('{"data": [[1.0, 2.0, 3.0]]}', encoding="utf-8")

    payload = module.build_regression_payload(args_with_sample(sample), "custom_reg")

    assert payload["data"] == [[1.0, 2.0, 3.0]]
    assert payload["rows"] == 3
    assert len(payload["schema"]) == 1


def regression_payload(count):
    """build a column-oriented regression request with ``count`` samples"""
    return {
        "algo": "custom_reg",
        "schema": [["x", "DOUBLE", 8], ["y", "DOUBLE", 8]],
        "data": [
            [float(index) for index in range(count)],
            [2.0 * index + 1.0 for index in range(count)],
        ],
        "rows": count,
    }


LISTING = {
    "details": [
        {
            "type": "regression",
            "algo": [{"name": "custom_reg", "builtins": False, "status": "ready"}],
        }
    ]
}


def test_verify_rejects_short_predictions(monkeypatch):
    """rows mirrors the prediction list, so the request supplies the expectation"""
    module = load_script()
    responses = {
        "/list": LISTING,
        "/regression": {"algo": "custom_reg", "res": [0], "rows": 1},
    }
    monkeypatch.setattr(
        module,
        "http_json",
        lambda base, path, payload=None, timeout=30, insecure=False: responses[path],
    )

    assert not module.verify(
        "http://127.0.0.1:6035", "regression", "custom_reg", regression_payload(60), 5, False
    )


def test_verify_accepts_one_prediction_per_row(monkeypatch):
    """a response covering every input sample still verifies"""
    module = load_script()
    responses = {
        "/list": LISTING,
        "/regression": {
            "algo": "custom_reg",
            "res": [float(index) for index in range(60)],
            "rows": 60,
        },
    }
    monkeypatch.setattr(
        module,
        "http_json",
        lambda base, path, payload=None, timeout=30, insecure=False: responses[path],
    )

    assert module.verify(
        "http://127.0.0.1:6035", "regression", "custom_reg", regression_payload(60), 5, False
    )
