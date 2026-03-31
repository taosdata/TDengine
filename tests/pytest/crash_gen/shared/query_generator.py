"""High-level query generator interface for crash_gen integration."""

from typing import List
from .grammar_walker import GrammarWalker
from .schema_provider import SchemaProvider
from .weights import AdaptiveWeightedChooser
from .constraints import ConstraintEngine, DEFAULT_CONSTRAINTS
from .coverage_tracker import CoverageTracker
from .grammar_dql import GRAMMAR
from .misc import Dice, Logging


class QueryGenerator:
    """Generate random SQL queries using grammar-driven random walk.
    
    Drop-in replacement for TdSuperTable.generateQueries().
    """

    # Shared coverage tracker across all threads (approximate counts OK)
    _shared_coverage = None

    @classmethod
    def get_coverage(cls):
        if cls._shared_coverage is None:
            cls._shared_coverage = CoverageTracker(GRAMMAR)
        return cls._shared_coverage

    def __init__(self, schema):
        coverage = self.get_coverage()
        self._walker = GrammarWalker(
            schema_provider=schema,
            weights=AdaptiveWeightedChooser(coverage),
            constraints=DEFAULT_CONSTRAINTS,
            coverage=coverage,
        )

    def generate(self, count=5):
        """Generate `count` random SQL queries. Returns list with .getSql() interface."""
        queries = []
        for _ in range(count):
            try:
                sql = self._walker.generate()
                queries.append(_SqlQueryWrapper(sql))
            except Exception as e:
                # Use print instead of logging to avoid initialization issues in standalone tests
                try:
                    Logging.debug("[QueryGen] Skipped: {}".format(e))
                except:
                    # Fallback if logging not initialized
                    pass
        return queries

    @classmethod
    def coverage_report(cls):
        if cls._shared_coverage:
            return cls._shared_coverage.report()
        return "No coverage data."


class _SqlQueryWrapper:
    """Minimal wrapper matching SqlQuery.getSql() interface."""
    def __init__(self, sql):
        self._sql = sql

    def getSql(self):
        return self._sql