#!/usr/bin/env python3
"""
SQL Fuzz Driver Module

Drives SQL fuzz testing by integrating the unified SQL generator into crash_gen's existing architecture.

Usage in crash_gen_main.py:

    from sql_fuzz_driver import CrashGenSQLGenerator

    # In TdSuperTable class
    def generateQueries(self, dbc: DbConn) -> List[SqlQuery]:
        # Initialize generator (once)
        if not hasattr(self, '_sql_generator'):
            self._sql_generator = CrashGenSQLGenerator(
                db_name=self._dbName,
                table_name=self._stName,
                dbc=dbc,
                error_rate=0.1  # 10% error injection
            )

        # Generate queries
        return self._sql_generator.generate_queries(
            query_type='select',  # or 'insert', 'alter', 'delete', 'all'
            count=10
        )
"""

import sys
import os
from typing import List, Optional

# Add crash_gen directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from enhanced_schema import EnhancedSchemaConfig, ColumnDef
from unified_sql_generator import UnifiedSQLGenerator, SQLQuery


class CrashGenSQLGenerator:
    """
    Integration wrapper for crash_gen

    Bridges the unified SQL generator with crash_gen's existing architecture.
    Automatically discovers schema from database and generates queries.
    """

    def __init__(self,
                 db_name: str,
                 table_name: str,
                 dbc,  # DbConn object from crash_gen
                 error_rate: float = 0.0,
                 seed: Optional[int] = None):
        """
        Initialize crash_gen SQL generator

        Args:
            db_name: Database name
            table_name: Super table name
            dbc: Database connection (crash_gen DbConn object)
            error_rate: Error injection rate (0.0-1.0)
                       0.0 = no errors (100% valid SQL)
                       0.1 = 10% invalid SQL
                       0.2 = 20% invalid SQL
            seed: Random seed for reproducibility
        """
        self.db_name = db_name
        self.table_name = table_name
        self.dbc = dbc
        self.error_rate = error_rate
        self.seed = seed

        # Initialize schema config
        self.schema_config = EnhancedSchemaConfig(seed=seed)

        # Discover schema from database
        self.columns, self.tags = self._discover_schema()

        # Initialize unified generator
        self.generator = UnifiedSQLGenerator(
            schema_config=self.schema_config,
            db_name=db_name,
            table_name=table_name,
            columns=self.columns,
            tags=self.tags,
            seed=seed
        )

    def _discover_schema(self) -> tuple:
        """
        Discover schema from existing super table

        Returns:
            (columns, tags) tuple
        """
        try:
            # Query table schema
            self.dbc.query(f"DESCRIBE {self.db_name}.{self.table_name}")
            rows = self.dbc.getQueryResult()

            columns = []
            tags = []

            for row in rows:
                col_name = row[0]
                col_type = row[1]
                col_length = row[2] if len(row) > 2 else 0
                note = row[3] if len(row) > 3 else ""

                # Parse type and size
                if '(' in col_type:
                    # e.g., BINARY(64), NCHAR(128)
                    base_type = col_type.split('(')[0]
                    size = int(col_type.split('(')[1].rstrip(')'))
                else:
                    base_type = col_type
                    size = 0

                # Create column definition
                col_def = ColumnDef(
                    name=col_name,
                    type=base_type,
                    size=size,
                    is_tag=(note == 'TAG')
                )

                if note == 'TAG':
                    tags.append(col_def)
                else:
                    columns.append(col_def)

            return columns, tags

        except Exception as e:
            # If schema discovery fails, use default schema
            print(f"Warning: Schema discovery failed: {e}")
            print("Using default schema")

            # Default schema (compatible with existing crash_gen)
            columns = [
                ColumnDef('ts', 'TIMESTAMP'),
                ColumnDef('speed', 'INT'),
                ColumnDef('color', 'BINARY', 16),
            ]
            tags = [
                ColumnDef('b', 'BINARY', 200, is_tag=True),
                ColumnDef('f', 'FLOAT', is_tag=True),
            ]
            return columns, tags

    def generate_queries(self,
                        query_type: str = 'select',
                        count: int = 10) -> List:
        """
        Generate SQL queries compatible with crash_gen

        Args:
            query_type: Type of queries to generate
                       'select' - SELECT queries only
                       'insert' - INSERT statements only
                       'alter' - ALTER statements only
                       'delete' - DELETE statements only
                       'all' - Mixed query types
            count: Number of queries to generate

        Returns:
            List of SqlQuery objects (crash_gen format)
        """
        # Generate queries using unified generator
        queries = self.generator.generate(
            query_type=query_type,
            count=count,
            error_rate=self.error_rate
        )

        # Convert to crash_gen SqlQuery format
        # Note: crash_gen's SqlQuery is a simple string wrapper
        # We return the SQL strings directly
        return [q.sql for q in queries]

    def generate_select_queries(self, count: int = 10) -> List[str]:
        """Generate SELECT queries only"""
        return self.generate_queries(query_type='select', count=count)

    def generate_insert_queries(self, count: int = 10) -> List[str]:
        """Generate INSERT queries only"""
        return self.generate_queries(query_type='insert', count=count)

    def generate_alter_queries(self, count: int = 10) -> List[str]:
        """Generate ALTER queries only"""
        return self.generate_queries(query_type='alter', count=count)

    def generate_delete_queries(self, count: int = 10) -> List[str]:
        """Generate DELETE queries only"""
        return self.generate_queries(query_type='delete', count=count)

    def set_error_rate(self, error_rate: float):
        """
        Update error injection rate

        Args:
            error_rate: New error rate (0.0-1.0)
        """
        self.error_rate = error_rate

    def print_stats(self):
        """Print generation statistics"""
        self.generator.print_stats()


def example_integration():
    """
    Example of how to integrate into crash_gen_main.py

    Replace the generateQueries() method in TdSuperTable class:
    """
    example_code = '''
    # In crash_gen_main.py, TdSuperTable class:

    def generateQueries(self, dbc: DbConn) -> List[SqlQuery]:
        """
        Generate queries to test/exercise this super table

        Now uses unified SQL generator with grammar-based generation
        """
        # Initialize generator (once per table)
        if not hasattr(self, '_sql_generator'):
            from sql_fuzz_driver import CrashGenSQLGenerator

            # Get error rate from config (default 10%)
            error_rate = getattr(Config, 'sql_error_rate', 0.1)

            self._sql_generator = CrashGenSQLGenerator(
                db_name=self._dbName,
                table_name=self._stName,
                dbc=dbc,
                error_rate=error_rate,
                seed=None  # Random seed for variety
            )

        # Generate SELECT queries (10 queries per call)
        sql_strings = self._sql_generator.generate_select_queries(count=10)

        # Convert to SqlQuery objects (crash_gen format)
        ret = []
        for sql in sql_strings:
            ret.append(SqlQuery(sql))

        return ret
    '''

    print("="*70)
    print("crash_gen Integration Example")
    print("="*70)
    print(example_code)


def test_integration():
    """Test integration with mock DbConn"""
    print("\n" + "="*70)
    print("Testing Integration (Mock Mode)")
    print("="*70)

    # Mock DbConn class
    class MockDbConn:
        def query(self, sql):
            return 0

        def getQueryResult(self):
            # Return mock schema (compatible with existing crash_gen)
            return [
                ('ts', 'TIMESTAMP', 8, ''),
                ('speed', 'INT', 4, ''),
                ('color', 'BINARY(16)', 16, ''),
                ('b', 'BINARY(200)', 200, 'TAG'),
                ('f', 'FLOAT', 4, 'TAG'),
            ]

    # Create mock connection
    mock_dbc = MockDbConn()

    # Initialize generator
    gen = CrashGenSQLGenerator(
        db_name='test_db',
        table_name='fs_table',
        dbc=mock_dbc,
        error_rate=0.15,  # 15% error rate
        seed=42
    )

    print("\nDiscovered Schema:")
    print(f"  Columns: {len(gen.columns)}")
    for col in gen.columns:
        print(f"    - {col.name:15s} {col.type}")
    print(f"  Tags: {len(gen.tags)}")
    for tag in gen.tags:
        print(f"    - {tag.name:15s} {tag.type}")

    # Generate SELECT queries
    print("\n" + "="*70)
    print("Generated SELECT Queries (15% error rate)")
    print("="*70)

    queries = gen.generate_select_queries(count=10)
    for i, sql in enumerate(queries, 1):
        print(f"\n{i:2d}. {sql}")

    # Generate mixed queries
    print("\n" + "="*70)
    print("Generated Mixed Queries")
    print("="*70)

    mixed = gen.generate_queries(query_type='all', count=20)
    for i, sql in enumerate(mixed[:5], 1):  # Show first 5
        query_type = 'SELECT' if 'SELECT' in sql else 'INSERT' if 'INSERT' in sql else 'ALTER' if 'ALTER' in sql else 'DELETE'
        print(f"\n{i:2d}. [{query_type}] {sql[:80]}...")

    # Print statistics
    gen.print_stats()


def main():
    """Main entry point"""
    print("="*70)
    print("crash_gen SQL Generator Integration")
    print("="*70)
    print("\nThis module integrates the unified SQL generator into crash_gen.")
    print("It provides:")
    print("  - Automatic schema discovery")
    print("  - Grammar-based SELECT query generation")
    print("  - Configurable error injection")
    print("  - Compatible with existing crash_gen architecture")

    # Show integration example
    example_integration()

    # Run test
    test_integration()


if __name__ == "__main__":
    main()
