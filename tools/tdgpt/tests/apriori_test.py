"""Unit tests for Apriori association rule mining"""

import os
import sys

import numpy as np
import pandas as pd
import pytest

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/..")

from taosanalytics.algo.association import _convert_to_matrix, do_apriori
from taosanalytics.algo.tool.apriori import AprioriAnalyzer


class TestAprioriAnalyzer:
    """Test AprioriAnalyzer class"""

    def test_execute_with_simple_matrix(self):
        """Test Apriori execution with simple binary matrix"""
        data = np.array(
            [
                [1, 1, 0, 0],
                [1, 1, 0, 0],
                [1, 0, 1, 0],
                [1, 0, 1, 0],
                [0, 1, 1, 0],
            ]
        )
        matrix = pd.DataFrame(data).astype(bool)

        analyzer = AprioriAnalyzer(min_support=0.4, min_confidence=0.5)
        analyzer.execute(matrix)

        rules = analyzer.get_rules()
        itemsets = analyzer.get_itemsets()

        assert len(itemsets) > 0, "Should find frequent itemsets"
        assert len(rules) >= 0, "Should return rules (may be empty)"

    def test_execute_with_no_support(self):
        """Test when no itemsets meet minimum support"""
        data = np.array(
            [
                [1, 0, 0, 0],
                [0, 1, 0, 0],
                [0, 0, 1, 0],
                [0, 0, 0, 1],
            ]
        )
        matrix = pd.DataFrame(data).astype(bool)

        analyzer = AprioriAnalyzer(min_support=0.5)
        analyzer.execute(matrix)

        itemsets = analyzer.get_itemsets()
        rules = analyzer.get_rules()

        assert len(itemsets) == 0, "Should find no itemsets with high support"
        assert len(rules) == 0, "Should find no rules"

    def test_get_rules_limit(self):
        """Test max_rules limit"""
        data = np.array(
            [
                [1, 1, 1],
                [1, 1, 0],
                [1, 0, 1],
                [1, 1, 1],
            ]
        )
        matrix = pd.DataFrame(data).astype(bool)

        analyzer = AprioriAnalyzer(min_support=0.25, min_confidence=0.3, max_rules=2)
        analyzer.execute(matrix)

        rules = analyzer.get_rules(limit=2)
        assert len(rules) <= 2, "Should respect max_rules limit"

    def test_invalid_input_empty(self):
        """Test with empty input"""
        analyzer = AprioriAnalyzer()

        with pytest.raises(ValueError):
            analyzer.execute(pd.DataFrame())

    def test_invalid_input_1d(self):
        """Test with 1D array"""
        analyzer = AprioriAnalyzer()

        with pytest.raises(ValueError):
            analyzer.execute(np.array([1, 2, 3]))

    def test_parameter_bounds(self):
        """Test parameter value clamping"""
        analyzer = AprioriAnalyzer(min_support=1.5, min_confidence=-0.2, max_rules=-10)

        assert analyzer.min_support == 1.0
        assert analyzer.min_confidence == 0.0
        assert analyzer.max_rules == 1


class TestConvertToMatrix:
    """Test _convert_to_matrix function"""

    def test_matrix_format_auto_detect(self):
        """Test auto-detection of matrix format"""
        data = [[1, 0, 1], [0, 1, 1], [1, 1, 0]]
        result = _convert_to_matrix(data, data_format=None)

        assert result.shape == (3, 3)
        # Check that all columns are boolean type
        for dtype in result.dtypes:
            assert isinstance(dtype.type, type) and issubclass(
                dtype.type, (bool, np.bool_)
            )

    def test_transactions_format_auto_detect(self):
        """Test auto-detection of transactions format"""
        data = [["A", "B"], ["B", "C"], ["A", "C"]]
        result = _convert_to_matrix(data, data_format=None)

        assert result.shape == (3, 3)
        assert list(result.columns) == ["A", "B", "C"]

    def test_transactions_format_explicit(self):
        """Test explicit transactions format"""
        data = [["item1", "item2"], ["item2", "item3"], ["item1", "item3"]]
        result = _convert_to_matrix(data, data_format="transactions")

        assert result.shape == (3, 3)
        assert set(result.columns) == {"item1", "item2", "item3"}

    def test_matrix_format_explicit(self):
        """Test explicit matrix format"""
        data = [[1, 0, 1], [0, 1, 1]]
        result = _convert_to_matrix(data, data_format="matrix")

        assert result.shape == (2, 3)

    def test_empty_data(self):
        """Test empty data"""
        with pytest.raises(ValueError):
            _convert_to_matrix([], data_format="matrix")

    def test_invalid_format(self):
        """Test invalid format string"""
        data = [[1, 0], [0, 1]]
        with pytest.raises(ValueError):
            _convert_to_matrix(data, data_format="invalid")

    def test_transactions_invalid_type(self):
        """Test transactions format with numeric items (should work)"""
        data = [[1, 2], [2, 3], [1, 3]]
        # Numeric items should work in transactions format
        result = _convert_to_matrix(data, data_format="transactions")
        assert result.shape == (3, 3)
        assert set(result.columns) == {1, 2, 3}


class TestDoApriori:
    """Test do_apriori function"""

    def test_do_apriori_basic(self):
        """Test basic do_apriori execution"""
        data = [
            [1, 1, 0, 0],
            [1, 1, 0, 0],
            [1, 0, 1, 0],
            [1, 0, 1, 0],
            [0, 1, 1, 0],
        ]
        schema = ["A", "B", "C", "D"]
        params = {
            "min_support": 0.4,
            "min_confidence": 0.5,
        }

        result = do_apriori(data, schema, params)

        assert "rules" in result
        assert "itemsets" in result
        assert "num_transactions" in result
        assert result["num_transactions"] == 5

    def test_do_apriori_transactions_format(self):
        """Test do_apriori with transactions format"""
        data = [
            ["milk", "bread"],
            ["milk", "bread", "eggs"],
            ["milk"],
            ["bread", "eggs"],
        ]
        params = {
            "min_support": 0.25,
            "min_confidence": 0.5,
            "data_format": "transactions",
        }

        result = do_apriori(data, None, params)

        assert "rules" in result
        assert result["num_transactions"] == 4

    def test_do_apriori_invalid_support(self):
        """Test invalid support parameter"""
        data = [[1, 0], [0, 1]]
        params = {"min_support": 1.5}

        with pytest.raises(ValueError):
            do_apriori(data, None, params)

    def test_do_apriori_invalid_confidence(self):
        """Test invalid confidence parameter"""
        data = [[1, 0], [0, 1]]
        params = {"min_confidence": -0.5}

        with pytest.raises(ValueError):
            do_apriori(data, None, params)

    def test_do_apriori_empty_data(self):
        """Test with empty data"""
        with pytest.raises(ValueError):
            do_apriori([], None, {})

    def test_do_apriori_adaptive_support(self):
        """Test adaptive support threshold"""
        # Small dataset with low min_support should be adjusted
        data = [[1, 0]] * 50
        params = {
            "min_support": 0.01,
        }

        result = do_apriori(data, None, params)
        assert result["num_transactions"] == 50

    def test_do_apriori_too_many_features(self):
        """Test feature count limit"""
        data = [[1] * 250]
        params = {}

        with pytest.raises(ValueError, match="Too many features"):
            do_apriori(data, None, params)

    def test_do_apriori_result_structure(self):
        """Test result structure"""
        data = [
            [1, 1, 0],
            [1, 1, 0],
            [1, 0, 1],
            [0, 1, 1],
        ]
        params = {"min_support": 0.25, "min_confidence": 0.5}

        result = do_apriori(data, None, params)

        assert isinstance(result["rules"], list)
        assert isinstance(result["itemsets"], list)
        assert isinstance(result["num_transactions"], int)
        assert isinstance(result["num_items"], int)

        # Check rule structure
        if result["rules"]:
            rule = result["rules"][0]
            assert "antecedents" in rule
            assert "consequents" in rule
            assert "support" in rule
            assert "confidence" in rule
            assert "lift" in rule
            assert "leverage" in rule
            assert "conviction" in rule

        # Check itemset structure
        if result["itemsets"]:
            itemset = result["itemsets"][0]
            assert "items" in itemset
            assert "support" in itemset


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
