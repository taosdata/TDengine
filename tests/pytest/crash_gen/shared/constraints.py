"""Semantic constraints for the grammar-driven SQL generator.

This module implements the constraint engine that filters grammar alternatives
based on semantic rules to generate valid SQL combinations.

PLANNED CONSTRAINTS REFERENCE:
=============================

The following constraints will be implemented in Task 12 once grammar_dql.py
is generated and actual alt indices are available:

FUNCTION × WINDOW CONSTRAINTS (~15):
- CSUM/DIFF/DERIVATIVE/IRATE/MAVG cannot pair with INTERVAL/SESSION/STATE_WINDOW  
- TWA function requires window context
- LAG/LEAD functions cannot use window
- FIRST/LAST functions need specific window types
- Aggregate functions with window restrictions
- Window function ordering requirements
- Partition by constraints for window functions
- Frame specification constraints
- Window function nesting restrictions
- Range vs rows frame constraints
- Unbounded preceding/following rules
- Current row specifications
- Window exclusion clause restrictions
- Multiple window specifications
- Window alias reference constraints

FUNCTION × TYPE CONSTRAINTS (~20):
- Math functions (ABS, CEIL, FLOOR, etc.) need numeric columns
- String functions (CONCAT, SUBSTR, TRIM, etc.) need string columns  
- GEOMETRY type only for ST_* spatial functions
- Date/time functions need timestamp/datetime columns
- Aggregate functions type compatibility
- Cast function source/target type compatibility
- Bitwise operations need integer types
- Trigonometric functions need numeric types
- Statistical functions need numeric collections
- Regular expression functions need string types
- JSON functions need JSON column types
- Array functions need array column types
- Binary functions need binary/varbinary types
- Hash functions input type restrictions
- Comparison operator type matching
- IN/NOT IN value type consistency
- BETWEEN operator type matching
- LIKE/ILIKE string type requirements
- NULL handling type constraints
- Implicit type conversion rules

JOIN CONSTRAINTS (~10):
- ASOF JOIN cannot be used in subqueries
- WINDOW JOIN requires WINDOW_OFFSET specification
- FULL OUTER JOIN limited to maximum 2 tables
- Self-join table alias requirements  
- Cross join cartesian product warnings
- Join condition type compatibility
- Multiple table join ordering
- Nested join parentheses requirements
- Join with aggregate function restrictions
- Lateral join context requirements

STRUCTURE CONSTRAINTS (~15):
- INTERP requires RANGE + EVERY + FILL clauses
- DISTINCT cannot be used with aggregate functions
- Aggregate and scalar mixing requires GROUP BY
- Subquery correlation restrictions
- CTE recursive definition rules
- UNION type compatibility requirements
- ORDER BY with LIMIT/OFFSET requirements
- HAVING clause requires GROUP BY or aggregate
- Window function in WHERE clause prohibition
- Aggregate functions in WHERE clause prohibition
- GROUP BY expression requirements
- SELECT list grouping requirements
- Partition pruning constraints
- Query complexity limits
- Nested query depth restrictions

TYPE CONSTRAINTS (~10):
- BLOB/GEOMETRY/VARBINARY not allowed in WHERE clauses
- BLOB/GEOMETRY/VARBINARY not allowed in GROUP BY clauses  
- BLOB/GEOMETRY/VARBINARY not allowed in ORDER BY clauses
- Large object types in comparison restrictions
- Binary type arithmetic operation restrictions
- Geometry type function compatibility
- JSON type indexing restrictions
- Array type operation constraints
- Complex type nesting limitations
- Type conversion safety rules
"""

from typing import List, Optional, Callable, Any


class WalkContext:
    """Context tracked during grammar walk — used by constraints to decide filtering."""
    
    def __init__(self):
        # Aggregate and window function tracking
        self.has_aggregate: bool = False
        self.has_window: bool = False
        self.window_type: Optional[str] = None
        
        # Query structure tracking
        self.in_subquery: bool = False
        self.join_type: Optional[str] = None
        self.has_group_by: bool = False
        
        # Column and function tracking
        self.selected_cols: list = []
        self.used_functions: set = set()
        self.table_type: Optional[str] = None
        
        # Type requirement flags
        self.need_numeric: bool = False
        self.need_string: bool = False
        self.current_function_category: Optional[str] = None


class Constraint:
    """A single semantic constraint rule.
    
    When condition(ctx) is True:
      - exclude_alts: remove these alternative indices from the choices
      - force_alt: force this specific alternative index
    """
    
    def __init__(self, rule: str,
                 condition: Callable[[WalkContext], bool],
                 exclude_alts: Optional[List[int]] = None,
                 force_alt: Optional[int] = None,
                 description: str = ""):
        self.rule = rule
        self.condition = condition
        self.exclude_alts = exclude_alts or []
        self.force_alt = force_alt
        self.description = description


class ConstraintEngine:
    """Apply semantic constraints to filter grammar alternatives."""

    def __init__(self, constraints: List[Constraint] = None):
        self._by_rule = {}
        for c in (constraints if constraints is not None else []):
            self._by_rule.setdefault(c.rule, []).append(c)

    def filter(self, rule: str, alternatives: list, ctx: WalkContext) -> list:
        """Return semantically valid alternatives for the given rule and context."""
        constraints = self._by_rule.get(rule, [])
        if not constraints:
            return alternatives

        valid_indices = set(range(len(alternatives)))
        forced = None

        for c in constraints:
            if not c.condition(ctx):
                continue
            if c.force_alt is not None and c.force_alt < len(alternatives):
                forced = c.force_alt
                break
            for idx in c.exclude_alts:
                valid_indices.discard(idx)

        if forced is not None:
            return [alternatives[forced]]

        result = [alternatives[i] for i in sorted(valid_indices)]
        return result if result else [alternatives[0]]  # fallback: at least one

    def add_constraint(self, constraint: Constraint):
        """Add a constraint at runtime."""
        self._by_rule.setdefault(constraint.rule, []).append(constraint)


# Inline test to verify functionality
if __name__ == "__main__":
    # Test basic constraint filtering
    
    ctx = WalkContext()
    ctx.has_aggregate = True
    c = Constraint("test_rule", condition=lambda ctx: ctx.has_aggregate, exclude_alts=[1, 2])
    engine = ConstraintEngine([c])
    alts = [["A"], ["B"], ["C"], ["D"]]
    result = engine.filter("test_rule", alts, ctx)
    assert result == [["A"], ["D"]]  # B and C excluded
    
    # When condition is False
    ctx2 = WalkContext()
    result2 = engine.filter("test_rule", alts, ctx2)
    assert result2 == alts  # nothing excluded
    
    print("OK")


# Initial P1 constraints based on actual grammar alt indices
def _create_initial_constraints():
    """Create initial high-priority semantic constraints."""
    constraints = []
    
    # P1: DISTINCT cannot be used with aggregate functions
    # set_quantifier_opt: [0] [] (empty), [1] ['DISTINCT'], [2] ['ALL'] 
    constraints.append(Constraint(
        "set_quantifier_opt",
        condition=lambda ctx: ctx.has_aggregate,
        exclude_alts=[1],  # exclude DISTINCT when aggregates present
        description="DISTINCT incompatible with aggregates"
    ))
    
    # P1: FROM clause should almost always be present  
    # from_clause_opt: [0] [] (empty), [1] ['FROM', 'table_reference_list']
    # This is already handled by WEIGHTS, but add constraint for emphasis
    
    return constraints


# Default constraint engine instance
DEFAULT_CONSTRAINTS = ConstraintEngine(_create_initial_constraints())