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

"""Test the classification request handler without starting the service."""

import json
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import numpy as np

from lib.taosanalytics.handlers import classification
from lib.util import normalize_json_values, validate_columns


class FakeClassificationService:
    def set_input_data(self, input_data, schema=None):
        self.input_data = input_data
        self.schema = schema

    def set_params(self, params):
        self.params = params

    def execute(self):
        return [1 if row[0] >= 2.0 else 0 for row in self.input_data]


class ClassificationHandlerTest(unittest.TestCase):
    @staticmethod
    def request(payload):
        return SimpleNamespace(is_json=True, json=payload)

    def test_returns_one_label_per_input_row(self):
        payload = {
            "algo": "user_classification",
            "schema": [["feature", "DOUBLE", 8]],
            "data": [[1.0, 2.0, 3.0]],
            "option": "threshold=2",
        }
        with patch.object(
            classification.loader,
            "get_service",
            return_value=FakeClassificationService(),
        ):
            result = classification.handle_classification(self.request(payload))

        self.assertEqual(result["algo"], "user_classification")
        self.assertEqual(result["rows"], 3)
        self.assertEqual(result["res"], [0, 1, 1])

    def test_rejects_prediction_count_mismatch(self):
        class ShortService(FakeClassificationService):
            def execute(self):
                return [1]

        payload = {
            "algo": "user_classification",
            "schema": [["feature", "DOUBLE", 8]],
            "data": [[1.0, 2.0, 3.0]],
        }
        with patch.object(
            classification.loader, "get_service", return_value=ShortService()
        ):
            result = classification.handle_classification(self.request(payload))

        self.assertEqual(result["rows"], -1)
        self.assertIn("predictions", result["msg"])

    def test_rejects_ragged_columns(self):
        payload = {
            "algo": "user_classification",
            "schema": [["feature", "DOUBLE", 8]],
            "data": [[1, 2], [0, 1, 2, 3]],
        }

        result = classification.handle_classification(self.request(payload))

        self.assertEqual(result["rows"], -1)
        self.assertIn("data inconsistent", result["msg"])

    def test_rejects_non_object_body(self):
        result = classification.handle_classification(self.request([]))

        self.assertEqual(result["rows"], -1)
        self.assertIn("JSON object", result["msg"])

    def test_rejects_non_string_algo(self):
        payload = {
            "algo": 123,
            "schema": [["feature", "DOUBLE", 8]],
            "data": [[1.0, 2.0, 3.0]],
        }

        result = classification.handle_classification(self.request(payload))

        self.assertEqual(result["rows"], -1)
        self.assertIn("must be a string", result["msg"])

    def test_serialises_numpy_predictions(self):
        class NumpyService(FakeClassificationService):
            def execute(self):
                return [np.int64(0), np.int64(1)]

        payload = {
            "algo": "user_classification",
            "schema": [["feature", "DOUBLE", 8]],
            "data": [[1.0, 3.0]],
        }
        with patch.object(
            classification.loader, "get_service", return_value=NumpyService()
        ):
            result = classification.handle_classification(self.request(payload))

        self.assertEqual(result["rows"], 2)
        self.assertEqual(result["res"], [0, 1])
        self.assertEqual(json.loads(json.dumps(result))["res"], [0, 1])

    def test_rejects_object_columns(self):
        """a JSON object has a length, so it must be rejected before transposing"""
        payload = {
            "algo": "user_classification",
            "schema": [["feature", "DOUBLE", 8]],
            "data": [{"a": 1}, {"a": 2}],
        }

        result = classification.handle_classification(self.request(payload))

        self.assertEqual(result["rows"], -1)
        self.assertIn("list of columns", result["msg"])

    def test_validate_columns_rejects_object_columns(self):
        """a mapping reports a length but is not a column"""
        with self.assertRaises(ValueError):
            validate_columns([{"a": 1}, {"a": 2}])


class ValidationHelperTest(unittest.TestCase):
    def test_validate_columns_returns_row_count(self):
        self.assertEqual(validate_columns([[1, 2, 3], [4, 5, 6]]), 3)

    def test_validate_columns_rejects_ragged_input(self):
        with self.assertRaises(ValueError) as ctx:
            validate_columns([[1, 2], [3]])

        self.assertIn("data inconsistent", str(ctx.exception))

    def test_validate_columns_rejects_non_column_input(self):
        with self.assertRaises(ValueError):
            validate_columns([1, 2])

    def test_normalize_json_values_converts_numpy_scalars(self):
        converted = normalize_json_values([np.int64(0), np.float64(1.5), True])

        self.assertEqual(converted, [0, 1.5, True])
        self.assertIsInstance(converted[0], int)
        self.assertIsInstance(converted[1], float)
