###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

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


