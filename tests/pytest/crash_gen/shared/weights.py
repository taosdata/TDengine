"""Production rule weights for the grammar walker."""

from typing import List, Tuple, Optional
from .misc import Dice


# Weight config: {rule_name: [alt0_weight, alt1_weight, ...]}
# Rules not listed use uniform distribution.
# Rules listed with None also use uniform distribution (explicit marker).
WEIGHTS = {
    # --- Top-level query ---
    "query_or_subquery":       [80, 20],   # query_expression vs subquery
    "query_simple":            [85, 15],   # specification vs union

    # --- SELECT clause ---
    "set_quantifier_opt":      [80, 10, 10],  # empty / DISTINCT / ALL
    "tag_mode_opt":            [90, 10],
    "hint_list":               [95, 5],

    # --- FROM ---
    "from_clause_opt":         [5, 95],    # almost always has FROM
    "table_reference":         [70, 30],   # table_primary vs joined_table

    # --- SELECT items: prefer common_expression over NK_STAR ---
    "select_item":             [10, 40, 30, 20, 5],  # NK_STAR rare, expressions preferred

    # --- Expressions: prefer column_reference and function over literal ---
    "expression":              [5,    # literal (bare numbers/strings - avoid)
                               10,    # pseudo_column 
                               35,    # column_reference (preferred)
                               30,    # function_expression (preferred)
                               5,     # if_expression
                               5,     # case_when_expression 
                               5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5], # rest lower weight

    # --- Recursive path control ---
    "expr_or_subquery":        [90, 10],   # strongly prefer expression over subquery
    "common_expression":       [85, 15],   # prefer expr over boolean_value_expression
    "boolean_value_expression":[60, 20, 10, 10],  # prefer simple over OR/AND recursion
    "boolean_primary":         [90, 10],   # prefer predicate over parenthesized

    # --- Function expressions: prefer simple functions over complex ones ---
    "function_expression":     [40, 40, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 10, 10],

    # --- Literals: prefer meaningful values over bare integers ---
    "literal":                 [10,   # NK_INTEGER (bare numbers - lower weight)
                               15,    # NK_FLOAT
                               25,    # NK_STRING (strings more useful)
                               20,    # NK_BOOL
                               15,    # TIMESTAMP string
                               10,    # duration_literal
                               5,     # NULL
                               5],    # NK_QUESTION

    # --- WHERE ---
    "where_clause_opt":        [30, 70],   # often has WHERE

    # --- PARTITION BY ---
    "partition_by_clause_opt": [50, 50],

    # --- GROUP BY ---
    "group_by_clause_opt":     [60, 40],

    # --- HAVING ---
    "having_clause_opt":       [70, 30],

    # --- ORDER BY ---
    "order_by_clause_opt":     [40, 60],
    "ordering_specification_opt": [10, 50, 40],  # default/ASC/DESC
    "null_ordering_opt":       [70, 15, 15],

    # --- LIMIT ---
    "limit_clause_opt":        [50, 30, 10, 10],
    "slimit_clause_opt":       [70, 20, 5, 5],

    # --- FILL ---
    "fill_opt":                [60, 40],

    # --- EVERY ---
    "every_opt":               [70, 30],
}

DEFAULT_WEIGHT = 10


class WeightedChooser:
    """Choose from grammar alternatives using configured weights."""

    def __init__(self, weights_config: dict = None):
        self._config = weights_config if weights_config is not None else WEIGHTS

    def weighted_choice(self, rule: str, alternatives: list) -> Tuple[list, int]:
        """Return (chosen_alternative, index_in_alternatives_list).
        
        If rule has configured weights matching alt count, use weighted random.
        Otherwise use uniform random.
        """
        n = len(alternatives)
        if n == 0:
            raise ValueError(f"No alternatives for rule '{rule}'")
        if n == 1:
            return alternatives[0], 0

        configured = self._config.get(rule)
        if configured is not None and len(configured) == n:
            # Weighted selection
            total = sum(configured)
            r = Dice.throw(total)
            cumulative = 0
            for i, w in enumerate(configured):
                cumulative += w
                if r < cumulative:
                    return alternatives[i], i
            return alternatives[-1], n - 1
        else:
            # Uniform
            idx = Dice.throw(n)
            return alternatives[idx], idx

    def adjust_for_depth(self, rule: str, alternatives: list,
                         depth: int, max_depth: int) -> list:
        """At deep levels, filter out self-recursive alternatives."""
        if depth <= max_depth * 0.4:
            return alternatives

        non_recursive = [alt for alt in alternatives if rule not in alt]
        return non_recursive if non_recursive else alternatives


class AdaptiveWeightedChooser(WeightedChooser):
    """Dynamically boost weights for uncovered alternatives to improve coverage."""

    def __init__(self, coverage, weights_config=None):
        super().__init__(weights_config)
        self._coverage = coverage

    def weighted_choice(self, rule, alternatives):
        n = len(alternatives)
        if n == 0:
            raise ValueError(f"No alternatives for rule '{rule}'")
        if n == 1:
            return alternatives[0], 0

        # Build adjusted weights
        configured = self._config.get(rule)
        adjusted = []
        for i in range(n):
            if configured is not None and i < len(configured):
                base = configured[i]
            else:
                base = DEFAULT_WEIGHT
            
            # Gentle boost for uncovered alternatives (avoid runaway recursion)
            count = self._coverage.get_count(rule, i)
            if count == 0:
                adjusted.append(base * 2)   # 2x boost for never-triggered
            elif count < 5:
                adjusted.append(int(base * 1.3))  # 1.3x for under-covered
            else:
                adjusted.append(base)

        total = sum(adjusted)
        if total == 0:
            idx = Dice.throw(n)
            return alternatives[idx], idx

        r = Dice.throw(total)
        cumulative = 0
        for i, w in enumerate(adjusted):
            cumulative += w
            if r < cumulative:
                return alternatives[i], i
        return alternatives[-1], n - 1