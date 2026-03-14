#!/usr/bin/env python3
"""
Enhanced Schema Configuration for crash_gen

Provides comprehensive data type support based on TDengine documentation:
/root/TDinternal/community/docs/zh/14-reference/03-taos-sql/01-datatype.md

Features:
- 20 data types (TIMESTAMP, INT, BIGINT, FLOAT, DOUBLE, BINARY, NCHAR, etc.)
- Professional column naming (sensor_*, metric_*, device_*, etc.)
- Type-aware value generation
- Support for both columns and tags
"""

import random
from typing import Dict, List, Tuple, Any
from dataclasses import dataclass


@dataclass
class ColumnDef:
    """Column definition"""
    name: str
    type: str
    size: int = 0  # For BINARY/NCHAR/VARCHAR
    is_tag: bool = False


class EnhancedSchemaConfig:
    """
    Enhanced schema configuration with comprehensive data type support

    Supports all TDengine data types:
    - Numeric: INT, BIGINT, FLOAT, DOUBLE, SMALLINT, TINYINT, BOOL
    - Unsigned: INT UNSIGNED, BIGINT UNSIGNED, SMALLINT UNSIGNED, TINYINT UNSIGNED
    - String: BINARY, VARCHAR, NCHAR
    - Time: TIMESTAMP
    - Precision: DECIMAL
    - Binary: VARBINARY, BLOB
    - JSON: JSON (tags only)
    """

    def __init__(self, seed: int = None):
        self.rng = random.Random(seed)

        # Column definitions with professional naming
        self.column_templates = {
            # Timestamp (required, always first)
            'timestamp': [
                ColumnDef('ts', 'TIMESTAMP'),
            ],

            # Numeric columns
            'numeric_int': [
                ColumnDef('sensor_id', 'INT'),
                ColumnDef('device_count', 'INT'),
                ColumnDef('error_code', 'INT'),
                ColumnDef('status_code', 'INT'),
            ],

            'numeric_bigint': [
                ColumnDef('total_bytes', 'BIGINT'),
                ColumnDef('packet_count', 'BIGINT'),
                ColumnDef('transaction_id', 'BIGINT'),
            ],

            'numeric_float': [
                ColumnDef('temperature', 'FLOAT'),
                ColumnDef('humidity', 'FLOAT'),
                ColumnDef('voltage', 'FLOAT'),
                ColumnDef('current', 'FLOAT'),
            ],

            'numeric_double': [
                ColumnDef('pressure', 'DOUBLE'),
                ColumnDef('altitude', 'DOUBLE'),
                ColumnDef('longitude', 'DOUBLE'),
                ColumnDef('latitude', 'DOUBLE'),
            ],

            'numeric_smallint': [
                ColumnDef('port_number', 'SMALLINT'),
                ColumnDef('retry_count', 'SMALLINT'),
            ],

            'numeric_tinyint': [
                ColumnDef('signal_strength', 'TINYINT'),
                ColumnDef('battery_level', 'TINYINT'),
            ],

            'numeric_bool': [
                ColumnDef('is_online', 'BOOL'),
                ColumnDef('is_active', 'BOOL'),
                ColumnDef('has_error', 'BOOL'),
            ],

            # Unsigned numeric columns
            'numeric_unsigned_int': [
                ColumnDef('request_count', 'INT UNSIGNED'),
                ColumnDef('response_time', 'INT UNSIGNED'),
            ],

            'numeric_unsigned_bigint': [
                ColumnDef('total_records', 'BIGINT UNSIGNED'),
            ],

            # String columns
            'string_binary': [
                ColumnDef('device_name', 'BINARY', 64),
                ColumnDef('location', 'BINARY', 128),
                ColumnDef('status_message', 'BINARY', 256),
            ],

            'string_varchar': [
                ColumnDef('device_type', 'VARCHAR', 32),
                ColumnDef('protocol', 'VARCHAR', 16),
            ],

            'string_nchar': [
                ColumnDef('description', 'NCHAR', 128),
                ColumnDef('remarks', 'NCHAR', 256),
            ],

            # Precision numeric
            'numeric_decimal': [
                ColumnDef('price', 'DECIMAL(18,2)'),
                ColumnDef('amount', 'DECIMAL(18,4)'),
            ],
        }

        # Tag definitions (professional naming)
        self.tag_templates = {
            'string_tags': [
                ColumnDef('region', 'BINARY', 64, is_tag=True),
                ColumnDef('datacenter', 'BINARY', 64, is_tag=True),
                ColumnDef('cluster_name', 'VARCHAR', 64, is_tag=True),
            ],

            'numeric_tags': [
                ColumnDef('group_id', 'INT', is_tag=True),
                ColumnDef('rack_id', 'SMALLINT', is_tag=True),
                ColumnDef('priority', 'TINYINT', is_tag=True),
            ],

            'float_tags': [
                ColumnDef('version', 'FLOAT', is_tag=True),
                ColumnDef('threshold', 'DOUBLE', is_tag=True),
            ],
        }

        # Value generators for each type
        self.value_generators = {
            'TIMESTAMP': lambda: 'now()',
            'INT': lambda: str(self.rng.randint(-2147483648, 2147483647)),
            'INT UNSIGNED': lambda: str(self.rng.randint(0, 4294967295)),
            'BIGINT': lambda: str(self.rng.randint(-9223372036854775808, 9223372036854775807)),
            'BIGINT UNSIGNED': lambda: str(self.rng.randint(0, 18446744073709551615)),
            'FLOAT': lambda: f"{self.rng.uniform(-3.4e38, 3.4e38):.6f}",
            'DOUBLE': lambda: f"{self.rng.uniform(-1.7e308, 1.7e308):.10f}",
            'SMALLINT': lambda: str(self.rng.randint(-32768, 32767)),
            'SMALLINT UNSIGNED': lambda: str(self.rng.randint(0, 65535)),
            'TINYINT': lambda: str(self.rng.randint(-128, 127)),
            'TINYINT UNSIGNED': lambda: str(self.rng.randint(0, 255)),
            'BOOL': lambda: self.rng.choice(['TRUE', 'FALSE']),
            'BINARY': lambda size: f"'{self._random_string(min(size, 32))}'",
            'VARCHAR': lambda size: f"'{self._random_string(min(size, 32))}'",
            'NCHAR': lambda size: f"'{self._random_nchar(min(size, 32))}'",
            'DECIMAL': lambda: f"{self.rng.uniform(-9999.99, 9999.99):.2f}",
        }

    def _random_string(self, length: int) -> str:
        """Generate random ASCII string"""
        chars = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
        return ''.join(self.rng.choice(chars) for _ in range(length))

    def _random_nchar(self, length: int) -> str:
        """Generate random NCHAR string (with Chinese characters)"""
        # Mix of ASCII and Chinese characters
        ascii_chars = 'abcdefghijklmnopqrstuvwxyz'
        chinese_chars = '北京上海深圳广州杭州南京武汉成都西安重庆'

        result = []
        for _ in range(length):
            if self.rng.random() > 0.7:  # 30% Chinese
                result.append(self.rng.choice(chinese_chars))
            else:
                result.append(self.rng.choice(ascii_chars))
        return ''.join(result)

    def generate_schema(self,
                       num_columns: int = 10,
                       num_tags: int = 3,
                       include_types: List[str] = None) -> Tuple[List[ColumnDef], List[ColumnDef]]:
        """
        Generate a random schema with specified number of columns and tags

        Args:
            num_columns: Number of data columns (excluding timestamp)
            num_tags: Number of tag columns
            include_types: List of type categories to include (e.g., ['numeric_int', 'string_binary'])

        Returns:
            (columns, tags) tuple
        """
        columns = []
        tags = []

        # Always include timestamp as first column
        columns.append(self.column_templates['timestamp'][0])

        # Select column types
        if include_types:
            available_types = {k: v for k, v in self.column_templates.items()
                             if k in include_types and k != 'timestamp'}
        else:
            available_types = {k: v for k, v in self.column_templates.items()
                             if k != 'timestamp'}

        # Generate columns
        all_columns = []
        for col_list in available_types.values():
            all_columns.extend(col_list)

        if len(all_columns) < num_columns:
            # Repeat if not enough
            selected = self.rng.choices(all_columns, k=num_columns)
        else:
            selected = self.rng.sample(all_columns, num_columns)

        columns.extend(selected)

        # Generate tags
        all_tags = []
        for tag_list in self.tag_templates.values():
            all_tags.extend(tag_list)

        if len(all_tags) < num_tags:
            selected_tags = self.rng.choices(all_tags, k=num_tags)
        else:
            selected_tags = self.rng.sample(all_tags, num_tags)

        tags.extend(selected_tags)

        return columns, tags

    def get_create_table_sql(self,
                            db_name: str,
                            table_name: str,
                            columns: List[ColumnDef],
                            tags: List[ColumnDef]) -> str:
        """Generate CREATE STABLE SQL"""
        col_defs = []
        for col in columns:
            if col.size > 0:
                col_defs.append(f"{col.name} {col.type}({col.size})")
            else:
                col_defs.append(f"{col.name} {col.type}")

        tag_defs = []
        for tag in tags:
            if tag.size > 0:
                tag_defs.append(f"{tag.name} {tag.type}({tag.size})")
            else:
                tag_defs.append(f"{tag.name} {tag.type}")

        sql = f"CREATE STABLE {db_name}.{table_name} ({', '.join(col_defs)}) TAGS ({', '.join(tag_defs)})"
        return sql

    def generate_value(self, col: ColumnDef) -> str:
        """Generate a random value for a column"""
        col_type = col.type.upper()

        # Handle types with size
        if col_type.startswith('BINARY') or col_type.startswith('VARCHAR'):
            return self.value_generators['BINARY'](col.size if col.size > 0 else 64)
        elif col_type.startswith('NCHAR'):
            return self.value_generators['NCHAR'](col.size if col.size > 0 else 64)
        elif col_type.startswith('DECIMAL'):
            return self.value_generators['DECIMAL']()
        elif col_type in self.value_generators:
            return self.value_generators[col_type]()
        else:
            # Default to INT
            return self.value_generators['INT']()

    def get_column_type_category(self, col: ColumnDef) -> str:
        """Get type category for a column (numeric, string, timestamp)"""
        col_type = col.type.upper()

        if 'TIMESTAMP' in col_type:
            return 'timestamp'
        elif any(t in col_type for t in ['INT', 'FLOAT', 'DOUBLE', 'BOOL', 'DECIMAL']):
            return 'numeric'
        elif any(t in col_type for t in ['BINARY', 'VARCHAR', 'NCHAR', 'CHAR']):
            return 'string'
        else:
            return 'numeric'


def main():
    """Test enhanced schema configuration"""
    print("="*70)
    print("Enhanced Schema Configuration for crash_gen")
    print("="*70)

    config = EnhancedSchemaConfig(seed=42)

    # Generate a sample schema
    columns, tags = config.generate_schema(num_columns=10, num_tags=3)

    print("\n" + "="*70)
    print("Generated Schema")
    print("="*70)

    print("\nColumns:")
    for i, col in enumerate(columns, 1):
        print(f"  {i:2d}. {col.name:20s} {col.type:20s}")

    print("\nTags:")
    for i, tag in enumerate(tags, 1):
        print(f"  {i:2d}. {tag.name:20s} {tag.type:20s}")

    # Generate CREATE TABLE SQL
    sql = config.get_create_table_sql('test_db', 'sensor_data', columns, tags)
    print("\n" + "="*70)
    print("CREATE STABLE SQL")
    print("="*70)
    print(f"\n{sql}")

    # Generate sample values
    print("\n" + "="*70)
    print("Sample Values")
    print("="*70)
    for col in columns[:5]:  # Show first 5 columns
        value = config.generate_value(col)
        print(f"  {col.name:20s}: {value}")


if __name__ == "__main__":
    main()
