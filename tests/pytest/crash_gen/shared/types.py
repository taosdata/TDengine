from typing import Any, BinaryIO, List, Dict, NewType
from enum import Enum
import random as _random

DirPath = NewType('DirPath', str)

QueryResult = NewType('QueryResult', List[List[Any]])

class TdDataType(Enum):
    '''
    Use a Python Enum types of represent all the data types in TDengine.

    Ref: https://www.taosdata.com/cn/documentation/taos-sql#data-type
    '''
    TIMESTAMP        = 'TIMESTAMP'
    INT              = 'INT'
    BIGINT           = 'BIGINT'
    FLOAT            = 'FLOAT'
    DOUBLE           = 'DOUBLE'
    BINARY           = 'BINARY'
    BINARY16         = 'BINARY(16)'  # TODO: get rid of this hack
    BINARY200        = 'BINARY(200)'
    SMALLINT         = 'SMALLINT'
    TINYINT          = 'TINYINT'
    BOOL             = 'BOOL'
    NCHAR            = 'NCHAR(64)'
    INT_UNSIGNED     = 'INT UNSIGNED'
    BIGINT_UNSIGNED  = 'BIGINT UNSIGNED'
    SMALLINT_UNSIGNED = 'SMALLINT UNSIGNED'
    TINYINT_UNSIGNED = 'TINYINT UNSIGNED'
    VARCHAR          = 'VARCHAR(64)'
    GEOMETRY         = 'GEOMETRY(128)'
    VARBINARY        = 'VARBINARY(64)'
    DECIMAL64        = 'DECIMAL(18,4)'
    JSON             = 'JSON'
    BLOB             = 'BLOB'

    @staticmethod
    def from_sql_type(sql_type_str):
        '''Map a DESCRIBE-returned type string back to a TdDataType member.'''
        # Try exact match first
        for member in TdDataType:
            if member.value == sql_type_str:
                return member
        # Fall back to base type match: check members whose value starts with
        # the base type, preferring longer (more specific) matches first.
        base = sql_type_str.split('(')[0].strip()
        candidates = []
        for member in TdDataType:
            member_base = member.value.split('(')[0].strip()
            if member_base == base:
                candidates.append(member)
        if candidates:
            # Return the longest-value match first (e.g. BINARY(200) before BINARY)
            candidates.sort(key=lambda m: len(m.value), reverse=True)
            # If the original string has a size spec, prefer the member whose
            # full value matches; otherwise return the longest specific variant.
            for c in candidates:
                if c.value == sql_type_str:
                    return c
            return candidates[0]
        raise ValueError(f"Unknown TDengine data type: {sql_type_str}")


class TdValueGenerator:
    """Unified SQL value generator for all TDengine data types."""

    @staticmethod
    def gen_value(data_type: TdDataType, seq: int) -> str:
        """Return a SQL-ready literal string for the given data type and sequence number."""
        if data_type == TdDataType.TIMESTAMP:
            raise ValueError("TIMESTAMP values must be handled separately (e.g. NOW or epoch)")

        if data_type == TdDataType.INT:
            return str(seq % 2147483647)
        if data_type == TdDataType.INT_UNSIGNED:
            return str(abs(seq) % 4294967295)
        if data_type == TdDataType.BIGINT:
            return str(seq * 1000000)
        if data_type == TdDataType.BIGINT_UNSIGNED:
            return str(abs(seq) * 1000000)
        if data_type == TdDataType.FLOAT:
            return str(round(0.9 + seq, 2))
        if data_type == TdDataType.DOUBLE:
            return str(round(0.999 + seq * 1.1, 6))
        if data_type == TdDataType.SMALLINT:
            return str(seq % 32767)
        if data_type == TdDataType.SMALLINT_UNSIGNED:
            return str(abs(seq) % 65535)
        if data_type == TdDataType.TINYINT:
            return str(seq % 127)
        if data_type == TdDataType.TINYINT_UNSIGNED:
            return str(abs(seq) % 255)
        if data_type == TdDataType.BOOL:
            return 'true' if seq % 2 == 0 else 'false'
        if data_type in (TdDataType.BINARY, TdDataType.BINARY16):
            return f"'bin_{seq % 9999}'"
        if data_type == TdDataType.BINARY200:
            city = 'Beijing_Shanghai_Guangzhou_Shenzhen_Hangzhou_Chengdu_'
            return f"'{city}{seq}'"
        if data_type == TdDataType.NCHAR:
            return f"'nchar_测试_{seq % 9999}'"
        if data_type == TdDataType.VARCHAR:
            return f"'varchar_{seq % 9999}'"
        if data_type == TdDataType.GEOMETRY:
            x = round(1.0 + seq * 0.01, 6)
            y = round(2.0 + seq * 0.01, 6)
            return f"'POINT({x} {y})'"
        if data_type == TdDataType.VARBINARY:
            hex_suffix = format(seq % 65536, '04X')
            return f"'\\x48656C6C6F{hex_suffix}'"
        if data_type == TdDataType.DECIMAL64:
            integer_part = str(seq % 99999999)
            frac = format(seq % 10000, '04d')
            return f'{integer_part}.{frac}'
        if data_type == TdDataType.JSON:
            return f"'{{\"k1\": \"v_{seq}\", \"k2\": {seq}}}'"
        if data_type == TdDataType.BLOB:
            hex_suffix = format(seq % 65536, '04X')
            return f"'\\x424C4F42{hex_suffix}'"

        raise ValueError(f"Unsupported data type for value generation: {data_type}")

    @staticmethod
    def gen_tag_value(data_type: TdDataType, seq: int) -> str:
        """Generate a tag value. Never returns NULL."""
        return TdValueGenerator.gen_value(data_type, seq)

    @staticmethod
    def gen_col_value(data_type: TdDataType, seq: int, null_chance: int = 5) -> str:
        """Generate a column value, with a 1/null_chance probability of returning NULL."""
        if _random.randint(1, null_chance) == 1:
            return 'NULL'
        return TdValueGenerator.gen_value(data_type, seq)

    @staticmethod
    def gen_where_condition(col_name: str, data_type: TdDataType, seq: int) -> str:
        """Generate a type-appropriate WHERE condition fragment."""
        numeric_types = {
            TdDataType.INT, TdDataType.INT_UNSIGNED,
            TdDataType.BIGINT, TdDataType.BIGINT_UNSIGNED,
            TdDataType.FLOAT, TdDataType.DOUBLE,
            TdDataType.SMALLINT, TdDataType.SMALLINT_UNSIGNED,
            TdDataType.TINYINT, TdDataType.TINYINT_UNSIGNED,
            TdDataType.DECIMAL64,
        }
        string_types = {TdDataType.BINARY, TdDataType.BINARY16,
                        TdDataType.BINARY200, TdDataType.NCHAR,
                        TdDataType.VARCHAR}

        if data_type in numeric_types:
            op = _random.choice(['>', '<', '>=', '<=', '='])
            val = TdValueGenerator.gen_value(data_type, seq)
            return f"{col_name} {op} {val}"
        if data_type == TdDataType.BOOL:
            bval = 'true' if seq % 2 == 0 else 'false'
            return f"{col_name} = {bval}"
        if data_type in string_types:
            return f"{col_name} LIKE '%{seq % 99}%'"
        if data_type in (TdDataType.GEOMETRY, TdDataType.VARBINARY):
            return f"{col_name} IS NOT NULL"
        if data_type == TdDataType.BLOB:
            return "1 = 1"
        return f"{col_name} IS NOT NULL"


TdColumns = Dict[str, TdDataType]
TdTags    = Dict[str, TdDataType]

IpcStream = NewType('IpcStream', BinaryIO)