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

"""association rule mining: algorithm and handler"""

import pandas as pd

from lib.log import AppLogger
from lib.taosanalytics.algo.assoc.apriori import AprioriAnalyzer
from lib.util import do_initial_check


def _convert_to_matrix(data, data_format=None):
    if not data or len(data) == 0:
        raise ValueError("empty data provided")

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
    if not input_data:
        raise ValueError("no input data provided")

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

    try:
        matrix = _convert_to_matrix(input_data, data_format)
    except Exception as e:
        AppLogger.error("Failed to convert input data: %s", str(e))
        raise

    if matrix.shape[1] > 200:
        raise ValueError(
            f"Too many features ({matrix.shape[1]}), max 200 allowed to prevent "
            "excessive computation"
        )

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


def handle_association(request, api_version):
    if api_version != "v1":
        AppLogger.error("unsupported API version: %s", api_version)
        return {"msg": f"unsupported API version: {api_version}", "rows": -1}

    try:
        req_json = do_initial_check(request)
    except Exception as e:
        return {"msg": str(e), "rows": -1}

    if "data" not in req_json:
        return {"msg": "'data' is required in payload", "rows": -1}

    try:
        data = req_json.get("data")
        schema = req_json.get("schema", None)
        options = req_json.get("option", {})

        result_data = do_apriori(data, schema, options)

        res = {
            "rows": len(result_data.get("rules", [])),
            "algo": "apriori",
            "option": options,
            "rules": result_data["rules"],
            "itemsets": result_data["itemsets"],
            "num_transactions": result_data["num_transactions"],
            "num_items": result_data["num_items"],
        }

        AppLogger.debug("association analysis result: %s rows of rules", res["rows"])
        return res

    except ValueError as e:
        AppLogger.error("association analysis parameter error: %s", str(e))
        return {"msg": str(e), "rows": -1}
    except Exception as e:
        AppLogger.error("association analysis failed, %s", str(e))
        return {"msg": str(e), "rows": -1}
