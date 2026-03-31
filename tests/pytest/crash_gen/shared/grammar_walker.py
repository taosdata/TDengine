"""Grammar-driven random SQL generator — core walker engine.

Uses ancestor-tracking to detect ALL recursion cycles (direct and indirect),
preventing infinite loops in deeply recursive grammar paths like
table_reference → joined_table → inner_joined → table_reference.
"""

from typing import List, Set
from .grammar_dql import GRAMMAR, TERMINALS, ENTRY_RULE, LIST_PATTERNS
from .terminal_resolver import TerminalResolver
from .constraints import WalkContext, ConstraintEngine
from .weights import WeightedChooser
from .coverage_tracker import CoverageTracker
from .schema_provider import SchemaProvider
from .misc import Dice


# Semantic shortcuts: rules resolved directly from schema
SEMANTIC_RULES = {
    "table_name":    "table_name",
    "column_name":   "column_name",
    "db_name":       "db_name",
    "table_alias":   "table_alias",
    "column_alias":  "column_alias",
}


class GenerationError(Exception):
    pass


def _precompute_reachable():
    """For each non-terminal, compute the set of non-terminals reachable from it.

    Used to detect indirect recursion: if rule A appears in its own
    reachable set, then alternatives containing rules on that cycle are
    recursive w.r.t. A.
    """
    all_rules = set(GRAMMAR.keys())
    adj = {}
    for rule, alts in GRAMMAR.items():
        children = set()
        for alt in alts:
            for sym in alt:
                if sym in all_rules:
                    children.add(sym)
        adj[rule] = children

    reachable = {}
    for start in all_rules:
        visited = set()
        queue = [start]
        while queue:
            node = queue.pop()
            if node in visited:
                continue
            visited.add(node)
            for child in adj.get(node, ()):
                if child not in visited:
                    queue.append(child)
        reachable[start] = visited
    return reachable


_REACHABLE = _precompute_reachable()


def _alt_is_recursive(symbol: str, alt: list) -> bool:
    """Check if choosing `alt` for `symbol` can eventually lead back to `symbol`."""
    if symbol in alt:
        return True
    for tok in alt:
        if tok in GRAMMAR and symbol in _REACHABLE.get(tok, ()):
            return True
    return False


class GrammarWalker:
    MAX_DEPTH = 12
    LIST_MIN = 1
    LIST_MAX = 3
    MAX_CALLS = 800

    def __init__(self, schema_provider: SchemaProvider,
                 weights: WeightedChooser = None,
                 constraints: ConstraintEngine = None,
                 coverage: CoverageTracker = None):
        self._resolver = TerminalResolver(schema_provider)
        self._weights = weights or WeightedChooser()
        self._constraints = constraints or ConstraintEngine()
        self._coverage = coverage

    def generate(self) -> str:
        """Generate one random SQL query."""
        self._depth = 0
        self._call_count = 0
        self._ctx = WalkContext()
        self._ancestors: Set[str] = set()
        try:
            tokens = self._expand(ENTRY_RULE)
            sql = self._join_tokens(tokens)
            if self._coverage:
                self._coverage.record_query()
            return sql
        except RecursionError as e:
            raise GenerationError(f"Recursion limit: {e}")

    def _expand(self, symbol: str) -> List[str]:
        """Recursively expand a grammar symbol into terminal tokens."""
        self._call_count += 1
        if self._call_count > self.MAX_CALLS:
            raise GenerationError(f"Budget exhausted ({self._call_count})")

        # Semantic shortcut for identifier rules
        if symbol in SEMANTIC_RULES:
            return [self._resolver.resolve(SEMANTIC_RULES[symbol], self._ctx)]

        # Terminal → resolve
        if symbol in TERMINALS:
            return [self._resolver.resolve(symbol, self._ctx)]

        # Not in grammar → treat as terminal
        if symbol not in GRAMMAR:
            return [self._resolver.resolve(symbol, self._ctx)]

        # Left-recursive list → iterative expansion
        if symbol in LIST_PATTERNS:
            return self._expand_list(symbol)

        alternatives = GRAMMAR[symbol]
        if not alternatives:
            return []

        in_cycle = symbol in self._ancestors
        depth_pct = self._depth / self.MAX_DEPTH if self.MAX_DEPTH else 1.0
        budget_pct = self._call_count / self.MAX_CALLS if self.MAX_CALLS else 1.0

        # --- Tier 1: Hard limit — pick shortest ---
        if depth_pct >= 1.0 or budget_pct > 0.7:
            alt = min(alternatives, key=len)
            alt_idx = alternatives.index(alt)

        # --- Tier 2: Cycle / deep / mid-budget — non-recursive only ---
        elif in_cycle or depth_pct > 0.5 or budget_pct > 0.4:
            non_rec = [a for a in alternatives if not _alt_is_recursive(symbol, a)]
            if non_rec:
                alt, _ = self._weights.weighted_choice(symbol, non_rec)
            else:
                alt = min(alternatives, key=len)
            alt_idx = alternatives.index(alt)

        # --- Tier 3: Normal — weighted + constrained ---
        else:
            filtered = self._constraints.filter(symbol, alternatives, self._ctx)
            alt, alt_idx = self._weights.weighted_choice(symbol, filtered)
            alt_idx = alternatives.index(alt)

        if self._coverage:
            self._coverage.record(symbol, alt_idx)

        self._update_context(symbol, alt)

        self._depth += 1
        self._ancestors.add(symbol)
        try:
            result = self._expand_alt(alt)
        finally:
            self._ancestors.discard(symbol)
            self._depth -= 1
        return result

    def _expand_alt(self, symbols: List[str]) -> List[str]:
        tokens = []
        for sym in symbols:
            tokens.extend(self._expand(sym))
        return tokens

    def _expand_list(self, symbol: str) -> List[str]:
        """Expand left-recursive list iteratively."""
        pattern = LIST_PATTERNS[symbol]
        item_rule = pattern["item"]
        sep = pattern.get("sep")
        count = self.LIST_MIN + Dice.throw(self.LIST_MAX - self.LIST_MIN + 1)

        tokens = []
        for i in range(count):
            if i > 0 and sep:
                tokens.extend(self._expand(sep))
            tokens.extend(self._expand(item_rule))

        if self._coverage:
            self._coverage.record(symbol, 0)
            if count > 1:
                self._coverage.record(symbol, 1)
        return tokens

    def _update_context(self, rule: str, alt: List[str]):
        """Track context for constraint decisions."""
        if rule == "twindow_clause_opt" and alt:
            for sym in alt:
                if sym in ("SESSION", "INTERVAL", "STATE_WINDOW",
                            "EVENT_WINDOW", "COUNT_WINDOW",
                            "ANOMALY_WINDOW"):
                    self._ctx.has_window = True
                    self._ctx.window_type = sym
                    break

        if rule == "group_by_clause_opt" and "GROUP" in alt:
            self._ctx.has_group_by = True

        if rule in ("inner_joined", "outer_joined", "semi_joined",
                     "anti_joined", "asof_joined", "win_joined"):
            for sym in alt:
                if sym in ("INNER", "LEFT", "RIGHT", "FULL",
                            "SEMI", "ANTI", "ASOF"):
                    self._ctx.join_type = sym

        if rule == "subquery":
            self._ctx.in_subquery = True

    def _join_tokens(self, tokens: List[str]) -> str:
        """Join tokens into SQL with smart spacing."""
        if not tokens:
            return ""
        result = [tokens[0]]
        no_space_before = {",", ")", ".", ";"}
        no_space_after = {"(", "."}
        for i in range(1, len(tokens)):
            prev = tokens[i - 1]
            curr = tokens[i]
            if curr in no_space_before or prev in no_space_after:
                result.append(curr)
            else:
                result.append(" ")
                result.append(curr)
        return "".join(result).strip()
