###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, db_test.stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-

import sys
import taos
from util.log import tdLog
from util.cases import tdCases
from util.sql import tdSql
import json


class TDTestCase:
    def caseDescription(self):
        '''
        case1: [TD-12435] fix ` identifier in table column name if using create table as subquery
        '''
        return

    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def run(self):
        tdSql.prepare()

        print("============== STEP 1 ===== prepare data & validate json string")
        tdSql.execute("create table if not exists st(ts timestamp, dataInt int)")
        tdSql.execute("create table st_from_sub as select avg(`dataInt`) from st interval(1m)")
        tdSql.query("describe st_from_sub")
        tdSql.checkData(1, 0, 'avg__dataInt__')

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())