from typing import Any, BinaryIO, List, Dict, NewType
from enum import Enum

DirPath = NewType('DirPath', str)

QueryResult = NewType('QueryResult', List[List[Any]])

class TdDataType(Enum):
    '''
    Use a Python Enum types of represent all the data types in TDengine.

    Ref: https://www.taosdata.com/cn/documentation/taos-sql#data-type
    Ref: /root/TDinternal/community/docs/zh/14-reference/03-taos-sql/01-datatype.md

    Supports all 20 TDengine data types:
    1. TIMESTAMP - 时间戳
    2. INT - 整型
    3. INT UNSIGNED - 无符号整型
    4. BIGINT - 长整型
    5. BIGINT UNSIGNED - 无符号长整型
    6. FLOAT - 浮点型
    7. DOUBLE - 双精度浮点型
    8. BINARY - 单字节字符串
    9. SMALLINT - 短整型
    10. SMALLINT UNSIGNED - 无符号短整型
    11. TINYINT - 单字节整型
    12. TINYINT UNSIGNED - 无符号单字节整型
    13. BOOL - 布尔型
    14. NCHAR - 多字节字符串
    15. JSON - JSON类型(仅Tag)
    16. VARCHAR - BINARY别名
    17. GEOMETRY - 几何类型
    18. VARBINARY - 可变长二进制
    19. DECIMAL - 高精度数值
    20. BLOB - 二进制大对象
    '''
    # Basic types
    TIMESTAMP = 'TIMESTAMP'
    INT       = 'INT'
    BIGINT    = 'BIGINT'
    FLOAT     = 'FLOAT'
    DOUBLE    = 'DOUBLE'
    BINARY    = 'BINARY'
    SMALLINT  = 'SMALLINT'
    TINYINT   = 'TINYINT'
    BOOL      = 'BOOL'
    NCHAR     = 'NCHAR'

    # Unsigned integer types
    INT_UNSIGNED      = 'INT UNSIGNED'
    BIGINT_UNSIGNED   = 'BIGINT UNSIGNED'
    SMALLINT_UNSIGNED = 'SMALLINT UNSIGNED'
    TINYINT_UNSIGNED  = 'TINYINT UNSIGNED'

    # String types
    VARCHAR   = 'VARCHAR'

    # Binary types
    VARBINARY = 'VARBINARY'
    BLOB      = 'BLOB'

    # Precision numeric
    DECIMAL   = 'DECIMAL'

    # Geometric type
    GEOMETRY  = 'GEOMETRY'

    # JSON type (tags only)
    JSON      = 'JSON'

    # Legacy compatibility (TODO: get rid of these hacks)
    BINARY16  = 'BINARY(16)'
    BINARY200 = 'BINARY(200)'

TdColumns = Dict[str, TdDataType]
TdTags    = Dict[str, TdDataType]

IpcStream = NewType('IpcStream', BinaryIO)