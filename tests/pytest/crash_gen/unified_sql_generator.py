#!/usr/bin/env python3
"""
Unified SQL Generator Interface for crash_gen

Provides a clean interface for generating different types of SQL statements:
- SELECT queries (with grammar-based generation)
- INSERT statements
- ALTER statements
- DELETE statements
- CREATE/DROP statements

Features:
- Pluggable query type selection
- Configurable error injection rate
- Type-aware generation
- Compatible with crash_gen state machine
"""

import random
from typing import List, Tuple, Dict, Optional, Set
from dataclasses import dataclass
from collections import Counter
from enhanced_schema import EnhancedSchemaConfig, ColumnDef


@dataclass
class SQLQuery:
    """Generated SQL query with metadata"""
    sql: str
    query_type: str  # 'select', 'insert', 'alter', 'delete', 'create', 'drop'
    is_valid: bool  # True if expected to succeed, False if intentional error
    error_type: str = 'none'  # Type of error injected
    features: Set[str] = None  # Query features (for SELECT)

    def __post_init__(self):
        if self.features is None:
            self.features = set()


class UnifiedSQLGenerator:
    """
    Unified SQL Generator for crash_gen

    Usage:
        gen = UnifiedSQLGenerator(schema_config, seed=42)

        # Generate SELECT queries
        queries = gen.generate(query_type='select', count=100, error_rate=0.1)

        # Generate INSERT statements
        inserts = gen.generate(query_type='insert', count=50, error_rate=0.05)

        # Generate mixed queries
        mixed = gen.generate(query_type='all', count=200, error_rate=0.15)
    """

    def __init__(self,
                 schema_config: EnhancedSchemaConfig,
                 db_name: str = 'test_db',
                 table_name: str = 'sensor_data',
                 columns: List[ColumnDef] = None,
                 tags: List[ColumnDef] = None,
                 seed: Optional[int] = None):
        """
        Initialize SQL generator

        Args:
            schema_config: Enhanced schema configuration
            db_name: Database name
            table_name: Table name (super table)
            columns: List of column definitions
            tags: List of tag definitions
            seed: Random seed for reproducibility
        """
        self.schema = schema_config
        self.db_name = db_name
        self.table_name = table_name
        self.columns = columns or []
        self.tags = tags or []
        self.rng = random.Random(seed)

        # Statistics
        self.stats = {
            'total': 0,
            'valid': 0,
            'invalid': 0,
            'by_type': Counter(),
            'by_error': Counter(),
        }

        # SELECT query corpus (from ply_sql_generator.py)
        self._init_select_corpus()

        # Error injection strategies
        self._init_error_strategies()

    def _init_select_corpus(self):
        """Initialize SELECT query templates"""
        self.select_templates = [
            # Basic SELECT
            {
                'template': 'SELECT {cols} FROM {table}',
                'features': {'basic'},
            },
            # SELECT with WHERE
            {
                'template': 'SELECT {cols} FROM {table} WHERE {condition}',
                'features': {'where'},
            },
            # SELECT with aggregates
            {
                'template': 'SELECT {agg_funcs} FROM {table}',
                'features': {'aggregate'},
            },
            # SELECT with GROUP BY
            {
                'template': 'SELECT {col}, {agg_func} FROM {table} GROUP BY {col}',
                'features': {'aggregate', 'group_by'},
            },
            # SELECT with ORDER BY
            {
                'template': 'SELECT {cols} FROM {table} ORDER BY {col} {order}',
                'features': {'order_by'},
            },
            # SELECT with LIMIT
            {
                'template': 'SELECT {cols} FROM {table} LIMIT {limit}',
                'features': {'limit'},
            },
            # SELECT with WHERE + ORDER BY + LIMIT
            {
                'template': 'SELECT {cols} FROM {table} WHERE {condition} ORDER BY {col} {order} LIMIT {limit}',
                'features': {'where', 'order_by', 'limit'},
            },
            # SELECT with GROUP BY + HAVING
            {
                'template': 'SELECT {col}, {agg_func} FROM {table} GROUP BY {col} HAVING {agg_condition}',
                'features': {'aggregate', 'group_by', 'having'},
            },
            # SELECT DISTINCT
            {
                'template': 'SELECT DISTINCT {col} FROM {table}',
                'features': {'distinct'},
            },
            # SELECT with BETWEEN
            {
                'template': 'SELECT {cols} FROM {table} WHERE {col} BETWEEN {value1} AND {value2}',
                'features': {'where', 'between'},
            },
            # SELECT with IN
            {
                'template': 'SELECT {cols} FROM {table} WHERE {col} IN ({values})',
                'features': {'where', 'in'},
            },
            # SELECT with LIKE (string columns only)
            {
                'template': 'SELECT {cols} FROM {table} WHERE {string_col} LIKE {pattern}',
                'features': {'where', 'like'},
            },
        ]

        # Aggregate functions by type
        self.agg_functions = {
            'numeric': ['count', 'avg', 'sum', 'stddev', 'min', 'max', 'first', 'last'],
            'string': ['count', 'first', 'last', 'min', 'max'],
            'timestamp': ['count', 'first', 'last', 'min', 'max'],
        }

        # Scalar functions by type
        self.scalar_functions = {
            'numeric': ['abs', 'ceil', 'floor', 'round', 'sqrt'],
            'string': ['char_length', 'length', 'lower', 'upper', 'ltrim', 'rtrim'],
            'timestamp': [],
        }

        # Comparison operators
        self.comparison_ops = ['=', '!=', '<', '<=', '>', '>=']

    def _init_error_strategies(self):
        """Initialize error injection strategies"""
        self.error_strategies = {
            'missing_from': lambda q: q.replace(' FROM ', ' '),
            'invalid_column': lambda q: q.replace(self._get_random_column_name(), 'nonexistent_col_xyz'),
            'syntax_error': lambda q: q.replace('SELECT', 'SELECT SELECT'),
            'missing_paren': lambda q: q.replace(')', '', 1) if ')' in q else q,
            'invalid_operator': lambda q: q.replace('=', '===', 1) if '=' in q else q,
            'missing_comma': lambda q: q.replace(', ', ' ', 1) if ', ' in q else q,
            'extra_keyword': lambda q: q.replace('WHERE', 'WHERE WHERE', 1) if 'WHERE' in q else q,
            'invalid_function': lambda q: q.replace('count', 'invalid_func', 1) if 'count' in q else q,
            'type_mismatch': lambda q: q.replace("'", "123.456.789", 1) if "'" in q else q,
            'missing_table': lambda q: q.replace(f'{self.db_name}.{self.table_name}', 'nonexistent_db.nonexistent_table'),
        }

    def _get_random_column_name(self) -> str:
        """Get a random column name"""
        if self.columns:
            return self.rng.choice([c.name for c in self.columns if c.name != 'ts'])
        return 'col'

    def _get_columns_by_type(self, type_category: str) -> List[ColumnDef]:
        """Get columns of a specific type category"""
        return [col for col in self.columns
                if self.schema.get_column_type_category(col) == type_category]

    def _fill_select_template(self, template: str) -> str:
        """Fill SELECT template with actual values"""
        result = template

        # Table reference
        result = result.replace('{table}', f'{self.db_name}.{self.table_name}')

        # Single column
        if '{col}' in result:
            available_cols = [c for c in self.columns if c.name != 'ts']
            if available_cols:
                col = self.rng.choice(available_cols)
                result = result.replace('{col}', col.name)
            else:
                result = result.replace('{col}', 'ts')

        # Multiple columns
        if '{cols}' in result:
            available_cols = [c for c in self.columns if c.name != 'ts']
            if available_cols:
                num_cols = self.rng.randint(1, min(3, len(available_cols)))
                cols = self.rng.sample(available_cols, num_cols)
                result = result.replace('{cols}', ', '.join([c.name for c in cols]))
            else:
                # Fallback to ts if no other columns
                result = result.replace('{cols}', 'ts')

        # String column (for LIKE)
        if '{string_col}' in result:
            string_cols = self._get_columns_by_type('string')
            if string_cols:
                result = result.replace('{string_col}', self.rng.choice(string_cols).name)
            else:
                # Fallback to any column
                result = result.replace('{string_col}', self._get_random_column_name())

        # Aggregate function
        if '{agg_func}' in result:
            col = self.rng.choice([c for c in self.columns if c.name != 'ts'])
            col_type = self.schema.get_column_type_category(col)
            func = self.rng.choice(self.agg_functions[col_type])
            result = result.replace('{agg_func}', f'{func}({col.name})')

        # Multiple aggregate functions
        if '{agg_funcs}' in result:
            num_funcs = self.rng.randint(1, 3)
            funcs = []
            for _ in range(num_funcs):
                col = self.rng.choice([c for c in self.columns if c.name != 'ts'])
                col_type = self.schema.get_column_type_category(col)
                func = self.rng.choice(self.agg_functions[col_type])
                funcs.append(f'{func}({col.name})')
            result = result.replace('{agg_funcs}', ', '.join(funcs))

        # Condition
        if '{condition}' in result:
            col = self.rng.choice([c for c in self.columns if c.name != 'ts'])
            op = self.rng.choice(self.comparison_ops)
            value = self.schema.generate_value(col)
            result = result.replace('{condition}', f'{col.name} {op} {value}')

        # Aggregate condition (for HAVING)
        if '{agg_condition}' in result:
            numeric_cols = self._get_columns_by_type('numeric')
            if numeric_cols:
                col = self.rng.choice(numeric_cols)
                func = self.rng.choice(self.agg_functions['numeric'])
                op = self.rng.choice(self.comparison_ops)
                value = self.rng.randint(0, 1000)
                result = result.replace('{agg_condition}', f'{func}({col.name}) {op} {value}')

        # Order direction
        if '{order}' in result:
            result = result.replace('{order}', self.rng.choice(['ASC', 'DESC']))

        # Limit value
        if '{limit}' in result:
            result = result.replace('{limit}', str(self.rng.choice([10, 50, 100, 1000, 10000])))

        # Value range (for BETWEEN)
        if '{value1}' in result and '{value2}' in result:
            v1 = self.rng.randint(0, 500)
            v2 = self.rng.randint(v1, 1000)
            result = result.replace('{value1}', str(v1))
            result = result.replace('{value2}', str(v2))

        # Values list (for IN)
        if '{values}' in result:
            num_values = self.rng.randint(2, 5)
            values = [str(self.rng.randint(0, 1000)) for _ in range(num_values)]
            result = result.replace('{values}', ', '.join(values))

        # Pattern (for LIKE)
        if '{pattern}' in result:
            patterns = ["'%test%'", "'data%'", "'%value'", "'_item'"]
            result = result.replace('{pattern}', self.rng.choice(patterns))

        return result

    def generate_select(self) -> SQLQuery:
        """Generate a SELECT query"""
        template_info = self.rng.choice(self.select_templates)
        sql = self._fill_select_template(template_info['template'])
        return SQLQuery(
            sql=sql,
            query_type='select',
            is_valid=True,
            features=template_info['features']
        )

    def generate_insert(self, num_rows: int = 1) -> SQLQuery:
        """Generate an INSERT statement"""
        # Generate values for each row
        values_list = []
        for i in range(num_rows):
            values = []
            for col in self.columns:
                if col.name == 'ts':
                    if i == 0:
                        values.append('now()')
                    else:
                        values.append(f'now() + {i}s')
                else:
                    values.append(self.schema.generate_value(col))
            values_list.append(f"({', '.join(values)})")

        sql = f"INSERT INTO {self.db_name}.{self.table_name} VALUES {', '.join(values_list)}"
        return SQLQuery(sql=sql, query_type='insert', is_valid=True)

    def generate_alter(self) -> SQLQuery:
        """Generate an ALTER statement"""
        operations = [
            f"ADD COLUMN new_col_{self.rng.randint(1, 1000)} INT",
            f"DROP COLUMN {self._get_random_column_name()}",
            f"MODIFY COLUMN {self._get_random_column_name()} BIGINT",
        ]
        operation = self.rng.choice(operations)
        sql = f"ALTER TABLE {self.db_name}.{self.table_name} {operation}"
        return SQLQuery(sql=sql, query_type='alter', is_valid=True)

    def generate_delete(self) -> SQLQuery:
        """Generate a DELETE statement"""
        col = self.rng.choice([c for c in self.columns if c.name != 'ts'])
        op = self.rng.choice(self.comparison_ops)
        value = self.schema.generate_value(col)
        sql = f"DELETE FROM {self.db_name}.{self.table_name} WHERE {col.name} {op} {value}"
        return SQLQuery(sql=sql, query_type='delete', is_valid=True)

    def inject_error(self, query: SQLQuery) -> SQLQuery:
        """Inject an error into a query"""
        error_type, error_func = self.rng.choice(list(self.error_strategies.items()))
        try:
            modified_sql = error_func(query.sql)
            return SQLQuery(
                sql=modified_sql,
                query_type=query.query_type,
                is_valid=False,
                error_type=error_type,
                features=query.features
            )
        except:
            # If error injection fails, return original
            return query

    def generate(self,
                query_type: str = 'select',
                count: int = 100,
                error_rate: float = 0.0) -> List[SQLQuery]:
        """
        Generate SQL queries

        Args:
            query_type: Type of queries to generate
                       'select', 'insert', 'alter', 'delete', 'all'
            count: Number of queries to generate
            error_rate: Percentage of queries with intentional errors (0.0-1.0)

        Returns:
            List of SQLQuery objects
        """
        queries = []

        # Query type distribution for 'all'
        type_distribution = {
            'select': 0.60,
            'insert': 0.25,
            'alter': 0.10,
            'delete': 0.05,
        }

        for i in range(count):
            # Determine query type
            if query_type == 'all':
                rand = self.rng.random()
                cumulative = 0
                selected_type = 'select'
                for qtype, prob in type_distribution.items():
                    cumulative += prob
                    if rand < cumulative:
                        selected_type = qtype
                        break
            else:
                selected_type = query_type

            # Generate query
            if selected_type == 'select':
                query = self.generate_select()
            elif selected_type == 'insert':
                query = self.generate_insert(num_rows=self.rng.choice([1, 5, 10]))
            elif selected_type == 'alter':
                query = self.generate_alter()
            elif selected_type == 'delete':
                query = self.generate_delete()
            else:
                query = self.generate_select()  # Default

            # Inject error if needed
            if self.rng.random() < error_rate:
                query = self.inject_error(query)

            queries.append(query)

            # Update statistics
            self.stats['total'] += 1
            if query.is_valid:
                self.stats['valid'] += 1
            else:
                self.stats['invalid'] += 1
                self.stats['by_error'][query.error_type] += 1
            self.stats['by_type'][query.query_type] += 1

        return queries

    def print_stats(self):
        """Print generation statistics"""
        print("\n" + "="*70)
        print("SQL Generation Statistics")
        print("="*70)

        print(f"\nTotal queries: {self.stats['total']:,}")
        print(f"Valid: {self.stats['valid']:,} ({self.stats['valid']/self.stats['total']*100:.1f}%)")
        print(f"Invalid: {self.stats['invalid']:,} ({self.stats['invalid']/self.stats['total']*100:.1f}%)")

        print("\nQuery Type Distribution:")
        for qtype, count in self.stats['by_type'].most_common():
            print(f"  {qtype:10s}: {count:6,} ({count/self.stats['total']*100:.1f}%)")

        if self.stats['by_error']:
            print("\nError Type Distribution:")
            for error_type, count in self.stats['by_error'].most_common():
                print(f"  {error_type:20s}: {count:6,} ({count/self.stats['invalid']*100:.1f}%)")


def main():
    """Test unified SQL generator"""
    print("="*70)
    print("Unified SQL Generator for crash_gen")
    print("="*70)

    # Create schema
    schema = EnhancedSchemaConfig(seed=42)
    columns, tags = schema.generate_schema(num_columns=10, num_tags=3)

    # Create generator
    gen = UnifiedSQLGenerator(
        schema_config=schema,
        db_name='test_db',
        table_name='sensor_data',
        columns=columns,
        tags=tags,
        seed=42
    )

    # Generate SELECT queries
    print("\n" + "="*70)
    print("Sample SELECT Queries")
    print("="*70)
    select_queries = gen.generate(query_type='select', count=5, error_rate=0.0)
    for i, q in enumerate(select_queries, 1):
        print(f"\n{i}. {q.sql}")
        print(f"   Features: {', '.join(sorted(q.features))}")

    # Generate mixed queries with error injection
    print("\n" + "="*70)
    print("Mixed Queries with Error Injection (10% error rate)")
    print("="*70)
    mixed_queries = gen.generate(query_type='all', count=100, error_rate=0.1)

    # Show some examples
    print("\nValid queries:")
    for q in [q for q in mixed_queries if q.is_valid][:3]:
        print(f"  [{q.query_type}] {q.sql[:80]}...")

    print("\nInvalid queries (intentional errors):")
    for q in [q for q in mixed_queries if not q.is_valid][:3]:
        print(f"  [{q.error_type}] {q.sql[:80]}...")

    # Print statistics
    gen.print_stats()


if __name__ == "__main__":
    main()
