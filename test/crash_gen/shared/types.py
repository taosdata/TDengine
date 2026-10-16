# Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

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