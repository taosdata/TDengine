"""association rule mining handler functions"""

import pandas as pd

from taosanalytics.algo.tool.apriori import AprioriAnalyzer
from taosanalytics.log import AppLogger


def _convert_to_matrix(data, data_format=None):
    """
    Convert input data to transaction-item matrix.

    Args:
        data: Either a 2D list (matrix) or list of lists (transactions)
        data_format: "matrix", "transactions", or None (auto-detect)

    Returns:
        pd.DataFrame: 0-1 matrix where rows=transactions, columns=items

    Raises:
        ValueError: If data format is invalid
    """
    if not data or len(data) == 0:
        raise ValueError("empty data provided")

    # Auto-detect format if not specified
    if data_format is None:
        if isinstance(data[0], list) and len(data[0]) > 0:
            if all(isinstance(item, str) for item in data[0]):
                data_format = "transactions"
            else:
                data_format = "matrix"
        else:
            data_format = "matrix"

    if data_format == "matrix":
        matrix = pd.DataFrame(data)
        if matrix.empty:
            raise ValueError("matrix is empty")
        return matrix.astype(bool)

    elif data_format == "transactions":
        unique_items = set()
        for transaction in data:
            if not isinstance(transaction, (list, set)):
                raise ValueError(
                    "transaction must be a list or set in transactions format"
                )
            unique_items.update(transaction)

        if not unique_items:
            raise ValueError("no items found in transactions")

        sorted_items = sorted(list(unique_items))
        matrix_data = []

        for transaction in data:
            row = [1 if item in transaction else 0 for item in sorted_items]
            matrix_data.append(row)

        return pd.DataFrame(matrix_data, columns=sorted_items).astype(bool)

    else:
        raise ValueError(f"unsupported data_format: {data_format}")


def do_apriori(input_data, schema, params):
    """
    Execute Apriori association rule mining.

    Args:
        input_data: Transaction-item matrix (list of lists) or transaction list
        schema: Column names / item names (optional, generated if not provided)
        params: dict with parameters:
            - min_support: float (0.0-1.0, default 0.3)
            - min_confidence: float (0.0-1.0, default 0.6)
            - max_rules: int (default 100)
            - data_format: "matrix", "transactions", or None (auto-detect)

    Returns:
        dict with keys:
            - rules: list of association rules with metrics
            - itemsets: list of frequent itemsets
            - num_transactions: number of transactions
            - num_items: number of unique items

    Raises:
        ValueError: If input data or parameters are invalid
    """
    if not input_data:
        raise ValueError("no input data provided")

    # Parse parameters
    min_support = float(params.get("min_support", 0.3))
    min_confidence = float(params.get("min_confidence", 0.6))
    max_rules = int(params.get("max_rules", 100))
    data_format = params.get("data_format", None)

    if not (0.0 <= min_support <= 1.0):
        raise ValueError("min_support must be between 0.0 and 1.0")
    if not (0.0 <= min_confidence <= 1.0):
        raise ValueError("min_confidence must be between 0.0 and 1.0")
    if max_rules < 1:
        raise ValueError("max_rules must be >= 1")

    # Adaptive support threshold based on transaction count
    num_transactions = len(input_data)
    if num_transactions < 100 and min_support < 0.05:
        AppLogger.debug(
            "Adjusting min_support from %.2f to 0.05 for small dataset", min_support
        )
        min_support = 0.05

    AppLogger.debug(
        "Apriori mining: %d transactions, support=%.2f, confidence=%.2f",
        num_transactions,
        min_support,
        min_confidence,
    )

    # Convert input to matrix format
    try:
        matrix = _convert_to_matrix(input_data, data_format)
    except Exception as e:
        AppLogger.error("Failed to convert input data: %s", str(e))
        raise

    # Check feature count limit
    if matrix.shape[1] > 200:
        raise ValueError(
            f"Too many features ({matrix.shape[1]}), max 200 allowed to prevent "
            "excessive computation"
        )

    # Run Apriori algorithm
    try:
        analyzer = AprioriAnalyzer(
            min_support=min_support,
            min_confidence=min_confidence,
            max_rules=max_rules,
        )
        analyzer.execute(matrix)

        rules = analyzer.get_rules(limit=max_rules)
        itemsets = analyzer.get_itemsets()

        AppLogger.debug(
            "Apriori completed: %d itemsets, %d rules",
            len(itemsets),
            len(rules),
        )

        return {
            "rules": rules,
            "itemsets": itemsets,
            "num_transactions": num_transactions,
            "num_items": matrix.shape[1],
        }
    except Exception as e:
        AppLogger.error("Apriori execution failed: %s", str(e))
        raise ValueError(f"Apriori execution failed: {str(e)}")
