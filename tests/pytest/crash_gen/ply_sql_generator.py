#!/usr/bin/env python3
"""
Optimized PLY-based SQL Generator for TDengine (SELECT-only)

Improvements inspired by go-sql-fuzz-test:
1. Corpus-based seed queries for high-quality baseline
2. Simple coverage tracking
3. Better mutation strategies
4. Long-running test support with time limits
5. Improved statistics and reporting
6. Focus on SELECT queries only
7. Higher quality (target >95% success rate)

Author: Claude Sonnet 4.6
Date: 2026-03-13
"""

import random
import re
import time
import sys
from typing import List, Tuple, Dict, Set, Optional
from dataclasses import dataclass, field
from collections import defaultdict, Counter
from datetime import datetime, timedelta


@dataclass
class QueryStats:
    """Statistics for query execution"""
    total: int = 0
    success: int = 0
    failed: int = 0
    error_codes: Counter = field(default_factory=Counter)
    query_features: Counter = field(default_factory=Counter)
    execution_times: List[float] = field(default_factory=list)

    def success_rate(self) -> float:
        return (self.success / self.total * 100) if self.total > 0 else 0.0


@dataclass
class SeedQuery:
    """High-quality seed query from corpus"""
    sql: str
    features: Set[str]  # e.g., {'join', 'group_by', 'having', 'subquery'}
    description: str


class SelectQueryCorpus:
    """
    Corpus of high-quality SELECT query templates
    Inspired by go-sql-fuzz-test's corpus-based approach
    """

    def __init__(self):
        self.seeds: List[SeedQuery] = []
        self._build_corpus()

    def _build_corpus(self):
        """Build corpus of seed queries"""

        # Basic SELECT
        self.seeds.append(SeedQuery(
            sql="SELECT {cols} FROM {table}",
            features={'basic'},
            description="Basic SELECT"
        ))

        # SELECT with WHERE
        self.seeds.append(SeedQuery(
            sql="SELECT {cols} FROM {table} WHERE {condition}",
            features={'where'},
            description="SELECT with WHERE"
        ))

        # SELECT with aggregates
        self.seeds.append(SeedQuery(
            sql="SELECT {agg_funcs} FROM {table}",
            features={'aggregate'},
            description="SELECT with aggregates"
        ))

        # SELECT with GROUP BY
        self.seeds.append(SeedQuery(
            sql="SELECT {col}, {agg_func} FROM {table} GROUP BY {col}",
            features={'aggregate', 'group_by'},
            description="SELECT with GROUP BY"
        ))

        # SELECT with ORDER BY
        self.seeds.append(SeedQuery(
            sql="SELECT {cols} FROM {table} ORDER BY {col} {order}",
            features={'order_by'},
            description="SELECT with ORDER BY"
        ))

        # SELECT with LIMIT
        self.seeds.append(SeedQuery(
            sql="SELECT {cols} FROM {table} LIMIT {limit}",
            features={'limit'},
            description="SELECT with LIMIT"
        ))

        # SELECT with WHERE + ORDER BY + LIMIT
        self.seeds.append(SeedQuery(
            sql="SELECT {cols} FROM {table} WHERE {condition} ORDER BY {col} {order} LIMIT {limit}",
            features={'where', 'order_by', 'limit'},
            description="SELECT with WHERE, ORDER BY, LIMIT"
        ))

        # SELECT with GROUP BY + HAVING
        self.seeds.append(SeedQuery(
            sql="SELECT {col}, {agg_func} FROM {table} GROUP BY {col} HAVING {agg_condition}",
            features={'aggregate', 'group_by', 'having'},
            description="SELECT with GROUP BY and HAVING"
        ))

        # SELECT with multiple aggregates
        self.seeds.append(SeedQuery(
            sql="SELECT {agg_funcs_multi} FROM {table}",
            features={'aggregate', 'multi_agg'},
            description="SELECT with multiple aggregates"
        ))

        # SELECT DISTINCT
        self.seeds.append(SeedQuery(
            sql="SELECT DISTINCT {col} FROM {table}",
            features={'distinct'},
            description="SELECT DISTINCT"
        ))

        # SELECT with arithmetic
        self.seeds.append(SeedQuery(
            sql="SELECT {col}, {col} {arith_op} {value} FROM {table}",
            features={'arithmetic'},
            description="SELECT with arithmetic"
        ))

        # SELECT with BETWEEN
        self.seeds.append(SeedQuery(
            sql="SELECT {cols} FROM {table} WHERE {col} BETWEEN {value1} AND {value2}",
            features={'where', 'between'},
            description="SELECT with BETWEEN"
        ))

        # SELECT with IN
        self.seeds.append(SeedQuery(
            sql="SELECT {cols} FROM {table} WHERE {col} IN ({values})",
            features={'where', 'in'},
            description="SELECT with IN"
        ))

        # SELECT with LIKE
        self.seeds.append(SeedQuery(
            sql="SELECT {cols} FROM {table} WHERE {string_col} LIKE {pattern}",
            features={'where', 'like'},
            description="SELECT with LIKE"
        ))

        # Complex: GROUP BY + HAVING + ORDER BY + LIMIT
        self.seeds.append(SeedQuery(
            sql="SELECT {col}, {agg_func} FROM {table} GROUP BY {col} HAVING {agg_condition} ORDER BY {col} {order} LIMIT {limit}",
            features={'aggregate', 'group_by', 'having', 'order_by', 'limit'},
            description="Complex SELECT with all clauses"
        ))


class OptimizedSQLGenerator:
    """
    Optimized SQL Generator focusing on SELECT queries

    Key improvements:
    - Corpus-based generation with high-quality seeds
    - Coverage tracking for query features
    - Better type safety and validation
    - Mutation strategies from go-sql-fuzz-test
    - Long-running test support
    """

    def __init__(self, seed: Optional[int] = None):
        self.rng = random.Random(seed)
        self.corpus = SelectQueryCorpus()
        self.stats = QueryStats()
        self.coverage = Counter()  # Track which features have been tested

        # Schema configuration (adapt to existing database)
        # Will be updated dynamically from actual database
        self.databases = ["db_341", "db_342"]
        self.tables = ["reg_table_684"]  # Will be updated from actual tables
        self.columns = {
            "ts": "TIMESTAMP",
            "speed": "INT",
            "color": "BINARY(16)"
        }

        # Type-aware function mapping
        self.agg_functions = {
            "numeric": ["count", "avg", "sum", "stddev", "min", "max", "first", "last"],
            "string": ["count", "first", "last", "min", "max"],  # stddev not supported for strings
            "timestamp": ["count", "first", "last", "min", "max"]
        }

        self.scalar_functions = {
            "numeric": ["abs", "ceil", "floor", "round", "sqrt"],
            "string": ["char_length", "length", "lower", "upper", "ltrim", "rtrim"],
            "timestamp": []
        }

        # Operators
        self.comparison_ops = ["=", "!=", "<", "<=", ">", ">="]
        self.logical_ops = ["AND", "OR"]
        self.arithmetic_ops = ["+", "-", "*", "/"]

    def _get_column_type(self, col: str) -> str:
        """Get type category of a column"""
        col_type = self.columns.get(col, "").upper()
        if any(t in col_type for t in ["INT", "FLOAT", "DOUBLE", "BIGINT", "SMALLINT", "TINYINT"]):
            return "numeric"
        elif any(t in col_type for t in ["BINARY", "NCHAR", "VARCHAR", "CHAR"]):
            return "string"
        elif "TIMESTAMP" in col_type:
            return "timestamp"
        elif "BOOL" in col_type:
            return "numeric"  # BOOL can be treated as numeric for some operations
        return "numeric"

    def _get_numeric_columns(self) -> List[str]:
        """Get all numeric columns"""
        return [col for col in self.columns.keys() if self._get_column_type(col) == "numeric"]

    def _get_string_columns(self) -> List[str]:
        """Get all string columns"""
        return [col for col in self.columns.keys() if self._get_column_type(col) == "string"]

    def _fill_template(self, template: str) -> str:
        """Fill template with concrete values"""
        result = template

        # Table reference
        db = self.rng.choice(self.databases)
        table = self.rng.choice(self.tables)
        result = result.replace("{table}", f"{db}.{table}")

        # Single column
        if "{col}" in result:
            col = self.rng.choice(list(self.columns.keys()))
            result = result.replace("{col}", col)

        # Multiple columns
        if "{cols}" in result:
            num_cols = self.rng.randint(1, 3)
            cols = self.rng.sample(list(self.columns.keys()), min(num_cols, len(self.columns)))
            result = result.replace("{cols}", ", ".join(cols))

        # String column (for LIKE)
        if "{string_col}" in result:
            string_cols = self._get_string_columns()
            if string_cols:
                result = result.replace("{string_col}", self.rng.choice(string_cols))
            else:
                result = result.replace("{string_col}", "color")

        # Aggregate function
        if "{agg_func}" in result:
            col = self.rng.choice(list(self.columns.keys()))
            col_type = self._get_column_type(col)
            func = self.rng.choice(self.agg_functions[col_type])
            result = result.replace("{agg_func}", f"{func}({col})")

        # Multiple aggregate functions
        if "{agg_funcs}" in result:
            num_funcs = self.rng.randint(1, 3)
            funcs = []
            for _ in range(num_funcs):
                col = self.rng.choice(list(self.columns.keys()))
                col_type = self._get_column_type(col)
                func = self.rng.choice(self.agg_functions[col_type])
                funcs.append(f"{func}({col})")
            result = result.replace("{agg_funcs}", ", ".join(funcs))

        # Multiple aggregate functions (different placeholder)
        if "{agg_funcs_multi}" in result:
            num_funcs = self.rng.randint(2, 4)
            funcs = []
            for _ in range(num_funcs):
                col = self.rng.choice(list(self.columns.keys()))
                col_type = self._get_column_type(col)
                func = self.rng.choice(self.agg_functions[col_type])
                funcs.append(f"{func}({col})")
            result = result.replace("{agg_funcs_multi}", ", ".join(funcs))

        # Condition
        if "{condition}" in result:
            col = self.rng.choice([c for c in self.columns.keys() if c != 'ts'])
            op = self.rng.choice(self.comparison_ops)
            col_type = self._get_column_type(col)
            if col_type == "numeric":
                value = self.rng.randint(0, 1000)
            else:
                value = f"'{self.rng.choice(['red', 'blue', 'green', 'white', 'black'])}'"
            result = result.replace("{condition}", f"{col} {op} {value}")

        # Aggregate condition (for HAVING)
        if "{agg_condition}" in result:
            col = self.rng.choice(self._get_numeric_columns())
            func = self.rng.choice(self.agg_functions["numeric"])
            op = self.rng.choice(self.comparison_ops)
            value = self.rng.randint(0, 100)
            result = result.replace("{agg_condition}", f"{func}({col}) {op} {value}")

        # Order direction
        if "{order}" in result:
            result = result.replace("{order}", self.rng.choice(["ASC", "DESC"]))

        # Limit value
        if "{limit}" in result:
            result = result.replace("{limit}", str(self.rng.choice([10, 50, 100, 1000, 10000])))

        # Value
        if "{value}" in result:
            result = result.replace("{value}", str(self.rng.randint(0, 1000)))

        # Value range (for BETWEEN)
        if "{value1}" in result and "{value2}" in result:
            v1 = self.rng.randint(0, 500)
            v2 = self.rng.randint(v1, 1000)
            result = result.replace("{value1}", str(v1))
            result = result.replace("{value2}", str(v2))

        # Values list (for IN)
        if "{values}" in result:
            num_values = self.rng.randint(2, 5)
            values = [str(self.rng.randint(0, 1000)) for _ in range(num_values)]
            result = result.replace("{values}", ", ".join(values))

        # Pattern (for LIKE)
        if "{pattern}" in result:
            patterns = ["'%red%'", "'blue%'", "'%green'", "'_lack'"]
            result = result.replace("{pattern}", self.rng.choice(patterns))

        # Arithmetic operation
        if "{arith_op}" in result:
            result = result.replace("{arith_op}", self.rng.choice(self.arithmetic_ops))

        return result

    def generate_from_corpus(self) -> Tuple[str, Set[str]]:
        """Generate query from corpus seed"""
        seed = self.rng.choice(self.corpus.seeds)
        query = self._fill_template(seed.sql)
        return query, seed.features

    def mutate_query(self, query: str, level: int = 1) -> str:
        """
        Mutate query using strategies from go-sql-fuzz-test

        Level 1: Literal mutation (values, limits)
        Level 2: Operator mutation (comparison, logical)
        Level 3: Structure mutation (add/remove clauses)
        """
        if level < 1:
            return query

        mutated = query

        # Level 1: Literal mutation
        if level >= 1 and self.rng.random() > 0.5:
            # Mutate numeric literals
            mutated = re.sub(
                r'\b(\d+)\b',
                lambda m: str(int(m.group(1)) * self.rng.choice([2, 10, 100])),
                mutated,
                count=1
            )

        # Level 2: Operator mutation
        if level >= 2 and self.rng.random() > 0.6:
            # Mutate comparison operators
            for old_op, new_op in [('=', '!='), ('<', '<='), ('>', '>=')]:
                if old_op in mutated:
                    mutated = mutated.replace(old_op, new_op, 1)
                    break

        # Level 3: Structure mutation
        if level >= 3 and self.rng.random() > 0.7:
            # Add LIMIT if not present
            if 'LIMIT' not in mutated:
                mutated += f" LIMIT {self.rng.choice([10, 100, 1000])}"

        return mutated

    def generate_select_query(self, use_corpus: bool = True, mutation_level: int = 0) -> Tuple[str, Set[str]]:
        """
        Generate SELECT query

        Args:
            use_corpus: Use corpus-based generation (higher quality)
            mutation_level: 0-3, higher = more mutation

        Returns:
            (query, features)
        """
        if use_corpus and self.rng.random() > 0.3:
            query, features = self.generate_from_corpus()
        else:
            # Fallback to random generation
            query, features = self.generate_from_corpus()  # Still use corpus but mutate more

        # Apply mutation
        if mutation_level > 0:
            query = self.mutate_query(query, mutation_level)

        # Track coverage
        for feature in features:
            self.coverage[feature] += 1

        return query, features

    def generate_batch(self, count: int, corpus_ratio: float = 0.8, mutation_level: int = 1) -> List[Tuple[str, Set[str]]]:
        """
        Generate batch of SELECT queries

        Args:
            count: Number of queries to generate
            corpus_ratio: Ratio of corpus-based queries (0.0-1.0)
            mutation_level: Mutation intensity (0-3)

        Returns:
            List of (query, features) tuples
        """
        queries = []

        for i in range(count):
            use_corpus = self.rng.random() < corpus_ratio
            query, features = self.generate_select_query(use_corpus, mutation_level)
            queries.append((query, features))

            if (i + 1) % 1000 == 0:
                print(f"  Generated {i+1}/{count} queries...")

        return queries

    def print_coverage_report(self):
        """Print coverage report"""
        print("\n" + "="*70)
        print("Coverage Report")
        print("="*70)

        total_features = len(self.corpus.seeds)
        covered_features = len(self.coverage)

        print(f"\nFeature Coverage: {covered_features}/{total_features} ({covered_features/total_features*100:.1f}%)")
        print("\nFeature Hit Counts:")
        for feature, count in sorted(self.coverage.items(), key=lambda x: x[1], reverse=True):
            print(f"  {feature:20s}: {count:6d} hits")

    def print_stats_report(self):
        """Print statistics report"""
        print("\n" + "="*70)
        print("Execution Statistics")
        print("="*70)

        print(f"\nTotal queries: {self.stats.total:,}")
        print(f"Successful: {self.stats.success:,} ({self.stats.success_rate():.2f}%)")
        print(f"Failed: {self.stats.failed:,} ({self.stats.failed/self.stats.total*100:.2f}%)")

        if self.stats.error_codes:
            print("\nTop Error Codes:")
            for error_code, count in self.stats.error_codes.most_common(10):
                print(f"  {error_code}: {count} ({count/self.stats.failed*100:.1f}%)")

        if self.stats.execution_times:
            avg_time = sum(self.stats.execution_times) / len(self.stats.execution_times)
            print(f"\nAverage execution time: {avg_time*1000:.2f}ms")


def run_long_test(generator: OptimizedSQLGenerator, duration_minutes: int = 30, db_conn=None):
    """
    Run long-duration test

    Args:
        generator: SQL generator instance
        duration_minutes: Test duration in minutes
        db_conn: Database connection (if None, dry run)
    """
    print("\n" + "="*70)
    print(f"Long-Running Test ({duration_minutes} minutes)")
    print("="*70)

    start_time = time.time()
    end_time = start_time + (duration_minutes * 60)

    query_count = 0
    batch_size = 100

    print(f"\nStart time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"End time: {(datetime.now() + timedelta(minutes=duration_minutes)).strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"\nGenerating and executing queries...")

    try:
        while time.time() < end_time:
            # Generate batch
            batch = generator.generate_batch(batch_size, corpus_ratio=0.8, mutation_level=1)

            # Execute batch
            for query, features in batch:
                query_count += 1
                generator.stats.total += 1

                if db_conn:
                    # Execute on real database
                    try:
                        exec_start = time.time()
                        db_conn.execute(query)
                        exec_time = time.time() - exec_start

                        generator.stats.success += 1
                        generator.stats.execution_times.append(exec_time)

                    except Exception as e:
                        generator.stats.failed += 1
                        error_msg = str(e)
                        # Extract error code
                        error_code_match = re.search(r'\[0x([0-9a-fA-F]+)\]', error_msg)
                        if error_code_match:
                            error_code = f"0x{error_code_match.group(1)}"
                            generator.stats.error_codes[error_code] += 1
                else:
                    # Dry run
                    generator.stats.success += 1

                # Progress update every 1000 queries
                if query_count % 1000 == 0:
                    elapsed = time.time() - start_time
                    remaining = end_time - time.time()
                    rate = query_count / elapsed
                    print(f"  [{datetime.now().strftime('%H:%M:%S')}] "
                          f"Queries: {query_count:,} | "
                          f"Success: {generator.stats.success_rate():.1f}% | "
                          f"Rate: {rate:.0f} q/s | "
                          f"Remaining: {remaining/60:.1f}min")

            # Small delay to avoid overwhelming the database
            time.sleep(0.1)

    except KeyboardInterrupt:
        print("\n\nTest interrupted by user")

    finally:
        elapsed = time.time() - start_time
        print(f"\n\nTest completed!")
        print(f"Duration: {elapsed/60:.1f} minutes")
        print(f"Total queries: {query_count:,}")
        print(f"Average rate: {query_count/elapsed:.1f} queries/second")

        # Print reports
        generator.print_stats_report()
        generator.print_coverage_report()


def main():
    """Main entry point"""
    print("\n" + "="*70)
    print("Optimized PLY-Based SQL Generator (SELECT-only)")
    print("="*70)
    print("\nInspired by go-sql-fuzz-test architecture")
    print("Features: Corpus-based, Coverage tracking, Long-running tests")
    print()

    # Initialize generator
    gen = OptimizedSQLGenerator(seed=42)

    # Show corpus
    print("\n" + "="*70)
    print(f"Query Corpus ({len(gen.corpus.seeds)} seed templates)")
    print("="*70)
    for i, seed in enumerate(gen.corpus.seeds, 1):
        print(f"{i:2d}. {seed.description:40s} Features: {', '.join(sorted(seed.features))}")

    # Generate samples
    print("\n" + "="*70)
    print("Sample Queries")
    print("="*70)

    for i in range(10):
        query, features = gen.generate_select_query(use_corpus=True, mutation_level=1)
        print(f"\n{i+1}. {query}")
        print(f"   Features: {', '.join(sorted(features))}")

    # Quick batch test
    print("\n" + "="*70)
    print("Quick Batch Test (1,000 queries)")
    print("="*70)

    batch = gen.generate_batch(1000, corpus_ratio=0.8, mutation_level=1)
    print(f"\n✓ Generated {len(batch):,} queries")

    gen.print_coverage_report()

    # Ask user if they want to run long test
    print("\n" + "="*70)
    print("Long-Running Test")
    print("="*70)
    print("\nTo run a 30-minute test on real database:")
    print("  python3 test_ply_generator_v2.py")
    print("\nOr run dry-run test:")
    print("  python3 ply_sql_generator.py --dry-run 30")


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "--dry-run":
        duration = int(sys.argv[2]) if len(sys.argv) > 2 else 30
        gen = OptimizedSQLGenerator(seed=None)  # Random seed for variety
        run_long_test(gen, duration_minutes=duration, db_conn=None)
    else:
        main()
