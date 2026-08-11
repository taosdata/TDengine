"""Apriori algorithm implementation for association rule mining"""

import numpy as np
import pandas as pd
from mlxtend.frequent_patterns import apriori, association_rules

from taosanalytics.log import AppLogger


class AprioriAnalyzer:
    """Apriori algorithm analyzer for frequent itemset mining and association rule generation"""

    def __init__(self, min_support=0.3, min_confidence=0.6, max_rules=100):
        """
        Initialize Apriori analyzer.

        Args:
            min_support: Minimum support threshold (0.0-1.0)
            min_confidence: Minimum confidence threshold (0.0-1.0)
            max_rules: Maximum number of rules to return
        """
        self.min_support = max(0.01, min(1.0, min_support))
        self.min_confidence = max(0.0, min(1.0, min_confidence))
        self.max_rules = int(max(1, max_rules))
        self.frequent_itemsets = None
        self.rules = None

    def execute(self, matrix):
        """
        Execute Apriori algorithm on transaction-item matrix.

        Args:
            matrix: DataFrame or 2D numpy array where rows are transactions,
                   columns are items, and values are 0/1 (binary).

        Returns:
            None (stores results internally)

        Raises:
            ValueError: If input data is invalid
        """
        if not isinstance(matrix, (pd.DataFrame, np.ndarray)):
            raise ValueError("Input must be DataFrame or numpy array")

        if isinstance(matrix, np.ndarray):
            if matrix.ndim != 2:
                raise ValueError("Input array must be 2-dimensional")
            matrix = pd.DataFrame(matrix)

        if matrix.empty:
            raise ValueError("Input matrix is empty")

        if matrix.shape[0] == 0 or matrix.shape[1] == 0:
            raise ValueError("Input matrix has zero rows or columns")

        # Ensure all values are boolean/binary
        matrix = matrix.astype(bool)

        AppLogger.debug(
            "Running Apriori with support=%.2f on %d transactions, %d items",
            self.min_support,
            matrix.shape[0],
            matrix.shape[1],
        )

        try:
            self.frequent_itemsets = apriori(
                matrix, min_support=self.min_support, use_colnames=True
            )

            if self.frequent_itemsets.empty:
                AppLogger.warning(
                    "No frequent itemsets found with min_support=%.2f",
                    self.min_support,
                )
                return

            AppLogger.debug("Found %d frequent itemsets", len(self.frequent_itemsets))

            if len(self.frequent_itemsets) > 1:
                self.rules = association_rules(
                    self.frequent_itemsets,
                    metric="confidence",
                    min_threshold=self.min_confidence,
                )

                if not self.rules.empty:
                    self.rules = self.rules.sort_values("lift", ascending=False)
                    AppLogger.debug("Generated %d association rules", len(self.rules))
            else:
                self.rules = pd.DataFrame()

        except Exception as e:
            AppLogger.error("Apriori execution failed: %s", str(e))
            raise ValueError(f"Apriori execution failed: {str(e)}")

    def get_rules(self, limit=None):
        """
        Get association rules with all metrics.

        Args:
            limit: Maximum number of rules to return (uses max_rules if None)

        Returns:
            list of dicts with rule details or empty list if no rules
        """
        if self.rules is None or self.rules.empty:
            return []

        limit = limit or self.max_rules
        rules_subset = self.rules.head(limit)

        result = []
        for _, row in rules_subset.iterrows():
            antecedents = sorted(list(row["antecedents"]))
            consequents = sorted(list(row["consequents"]))

            rule_dict = {
                "antecedents": antecedents,
                "consequents": consequents,
                "support": float(row["support"]),
                "confidence": float(row["confidence"]),
                "lift": float(row["lift"]),
                "leverage": float(row["leverage"]),
                "conviction": float(row["conviction"]),
            }
            result.append(rule_dict)

        return result

    def get_itemsets(self):
        """
        Get frequent itemsets.

        Returns:
            list of dicts with itemset and support, or empty list if no itemsets
        """
        if self.frequent_itemsets is None or self.frequent_itemsets.empty:
            return []

        result = []
        for _, row in self.frequent_itemsets.iterrows():
            itemset_dict = {
                "items": sorted(list(row["itemsets"])),
                "support": float(row["support"]),
            }
            result.append(itemset_dict)

        return result
