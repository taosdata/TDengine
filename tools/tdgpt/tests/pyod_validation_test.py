# encoding:utf-8
"""Tests for pyod parameter validation helpers and set_params boundary behaviour."""

import os
import sys
import unittest

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/..")

from taosanalytics.algo.tool.pyod_util import (
    validate_bool_int,
    validate_contamination,
    validate_int_min,
    validate_nonneg_int,
    validate_positive_float,
    validate_positive_int,
)


class TestValidateContamination(unittest.TestCase):
    def test_upper_bound_accepted(self):
        self.assertEqual(validate_contamination("0.5"), 0.5)

    def test_small_positive_accepted(self):
        self.assertAlmostEqual(validate_contamination("0.001"), 0.001)

    def test_typical_value_accepted(self):
        self.assertAlmostEqual(validate_contamination(0.1), 0.1)

    def test_zero_rejected(self):
        with self.assertRaises(ValueError):
            validate_contamination("0")

    def test_above_half_rejected(self):
        with self.assertRaises(ValueError):
            validate_contamination("0.51")

    def test_non_numeric_rejected(self):
        with self.assertRaises(ValueError):
            validate_contamination("abc")

    def test_none_rejected(self):
        with self.assertRaises((ValueError, TypeError)):
            validate_contamination(None)


class TestValidatePositiveInt(unittest.TestCase):
    def test_valid(self):
        self.assertEqual(validate_positive_int("100", "x"), 100)

    def test_one_accepted(self):
        self.assertEqual(validate_positive_int("1", "x"), 1)

    def test_zero_rejected(self):
        with self.assertRaises(ValueError):
            validate_positive_int("0", "x")

    def test_negative_rejected(self):
        with self.assertRaises(ValueError):
            validate_positive_int("-1", "x")

    def test_non_numeric_rejected(self):
        with self.assertRaises(ValueError):
            validate_positive_int("abc", "n_estimators")

    def test_error_names_param(self):
        with self.assertRaises(ValueError, msg="n_estimators") as ctx:
            validate_positive_int("abc", "n_estimators")
        self.assertIn("n_estimators", str(ctx.exception))


class TestValidateIntMin(unittest.TestCase):
    def test_at_minimum_accepted(self):
        self.assertEqual(validate_int_min("2", "n_bins", 2), 2)

    def test_above_minimum_accepted(self):
        self.assertEqual(validate_int_min("10", "n_bins", 2), 10)

    def test_below_minimum_rejected(self):
        with self.assertRaises(ValueError):
            validate_int_min("1", "n_bins", 2)

    def test_non_numeric_rejected(self):
        with self.assertRaises(ValueError):
            validate_int_min("abc", "n_bins", 2)


class TestValidateNonnegInt(unittest.TestCase):
    def test_zero_accepted(self):
        self.assertEqual(validate_nonneg_int("0", "random_state"), 0)

    def test_positive_accepted(self):
        self.assertEqual(validate_nonneg_int("42", "random_state"), 42)

    def test_negative_rejected(self):
        with self.assertRaises(ValueError):
            validate_nonneg_int("-1", "random_state")

    def test_non_numeric_rejected(self):
        with self.assertRaises(ValueError):
            validate_nonneg_int("abc", "random_state")


class TestValidatePositiveFloat(unittest.TestCase):
    def test_integer_string_accepted(self):
        self.assertEqual(validate_positive_float("3", "n_components"), 3.0)

    def test_float_string_accepted(self):
        self.assertAlmostEqual(validate_positive_float("0.95", "n_components"), 0.95)

    def test_zero_rejected(self):
        with self.assertRaises(ValueError):
            validate_positive_float("0", "n_components")

    def test_negative_rejected(self):
        with self.assertRaises(ValueError):
            validate_positive_float("-1.0", "n_components")

    def test_non_numeric_rejected(self):
        with self.assertRaises(ValueError):
            validate_positive_float("abc", "n_components")


class TestValidateBoolInt(unittest.TestCase):
    def test_one_true(self):
        self.assertTrue(validate_bool_int("1", "standardization"))

    def test_zero_false(self):
        self.assertFalse(validate_bool_int("0", "standardization"))

    def test_true_string(self):
        self.assertTrue(validate_bool_int("true", "standardization"))

    def test_false_string(self):
        self.assertFalse(validate_bool_int("false", "standardization"))

    def test_yes_string(self):
        self.assertTrue(validate_bool_int("yes", "standardization"))

    def test_no_string(self):
        self.assertFalse(validate_bool_int("no", "standardization"))

    def test_uppercase_accepted(self):
        self.assertTrue(validate_bool_int("True", "standardization"))

    def test_python_bool_accepted(self):
        self.assertTrue(validate_bool_int(True, "standardization"))
        self.assertFalse(validate_bool_int(False, "standardization"))

    def test_invalid_rejected(self):
        with self.assertRaises(ValueError):
            validate_bool_int("2", "standardization")

    def test_non_numeric_string_rejected(self):
        with self.assertRaises(ValueError):
            validate_bool_int("abc", "standardization")


if __name__ == "__main__":
    unittest.main()
