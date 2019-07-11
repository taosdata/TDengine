"""Constants in TDengine python
"""

from .dbapi import *

class FieldType(object):
    """TDengine Field Types
    """
    # type_code
    C_NULL = 0
    C_BOOL = 1
    C_TINYINT = 2
    C_SMALLINT = 3
    C_INT = 4
    C_BIGINT = 5
    C_FLOAT = 6
    C_DOUBLE = 7
    C_BINARY = 8
    C_TIMESTAMP = 9
    C_NCHAR = 10
    # NULL value definition
    # NOTE: These values should change according to C definition in tsdb.h
    C_BOOL_NULL = 0x02
    C_TINYINT_NULL = -128
    C_SMALLINT_NULL = -32768
    C_INT_NULL = -2147483648
    C_BIGINT_NULL = -9223372036854775808
    C_FLOAT_NULL = float('nan')
    C_DOUBLE_NULL = float('nan')
    C_BINARY_NULL = bytearray([int('0xff', 16)])
    # Timestamp precision definition
    C_TIMESTAMP_MILLI = 0
    C_TIMESTAMP_MICRO = 1
