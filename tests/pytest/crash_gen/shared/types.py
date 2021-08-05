from typing import Any, BinaryIO, List, Dict, NewType
from enum import Enum

DirPath = NewType('DirPath', str)

QueryResult = NewType('QueryResult', List[List[Any]])

class TdDataType(Enum):
    '''
    Use a Python Enum types of represent all the data types in TDengine.

    Ref: https://www.taosdata.com/cn/documentation/taos-sql#data-type
    '''
    TIMESTAMP = 'TIMESTAMP'
    INT       = 'INT'
    BIGINT    = 'BIGINT'
    FLOAT     = 'FLOAT'
    DOUBLE    = 'DOUBLE'
    BINARY    = 'BINARY'
    BINARY16  = 'BINARY(16)'  # TODO: get rid of this hack
    BINARY200 = 'BINARY(200)'
    SMALLINT  = 'SMALLINT'
    TINYINT   = 'TINYINT'
    BOOL      = 'BOOL'
    NCHAR     = 'NCHAR'

TdColumns = Dict[str, TdDataType]
TdTags    = Dict[str, TdDataType]

IpcStream = NewType('IpcStream', BinaryIO)