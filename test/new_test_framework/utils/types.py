# Copyright (c) 2016 TAOS Data, Inc. <jhtao@taosdata.com>
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

# -*- coding: utf-8 -*-

from enum import Enum

class TDSmlProtocolType(Enum):
    '''
    Schemaless Protocol types
    0 - unknown
    1 - InfluxDB Line Protocol
    2 - OpenTSDB Telnet Protocl
    3 - OpenTSDB JSON Protocol
    '''
    UNKNOWN = 0
    LINE    = 1
    TELNET  = 2
    JSON    = 3

class TDSmlTimestampType(Enum):
    NOT_CONFIGURED = 0
    HOUR           = 1
    MINUTE         = 2
    SECOND         = 3
    MILLI_SECOND   = 4
    MICRO_SECOND   = 5
    NANO_SECOND    = 6


