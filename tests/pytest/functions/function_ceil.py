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

import sys
import taos
from util.log import *
from util.cases import *
from util.sql import *
import numpy as np


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())

    def run(self):
        tdSql.prepare()
        tdSql.execute("create stable super (ts timestamp, timestamp_t timestamp, int_col int, bigint_col bigint, float_col float,\
                double_col double, binary_col binary(8), smallint_col smallint, tinyint_col tinyint, bool_col bool, nchar_col nchar(8), \
                uint_col int unsigned, ubigint_col bigint unsigned, usmallint_col smallint unsigned,  utinyint_col tinyint unsigned) tags (int_tag int, bigint_tag bigint, \
                    float_tag float, double_tag double, binary_tag binary(8), smallint_tag smallint, tinyint_tag tinyint, bool_tag bool, nchar_tag nchar(8),\
                        uint_tag int unsigned, ubigint_tag bigint unsigned, usmallint_tag smallint unsigned, utinyint_tag tinyint unsigned)")

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())