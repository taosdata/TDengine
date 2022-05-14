###################################################################
#           Copyright (c) 2021 by TAOS Technologies, Inc.
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
from util.log import *
from util.cases import *
from util.sql import *


class TDTestCase:
    def caseDescription(self):
        '''
        case1:[TD-6347] restrict like to be followed only by strings
        '''
        return

    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)
        self._conn = conn

    def run(self):
        print("running {}".format(__file__))
        tdSql.prepare()

        tdSql.execute('create stable st_1(ts timestamp , value int ) tags (ind int)')
        tdSql.execute('create stable st_2(ts timestamp , value int ) tags (ind int)')
        tdSql.execute('create table  tb_1 using st_1 tags(1)')
        tdSql.execute('create table  tb_2 using st_1 tags(2)')
        tdSql.error('show tables like tb__')
        tdSql.error('show stables like st__')
        tdSql.query('show tables like "tb__"')
        tdSql.checkRows(2)
        tdSql.query('show stables like "st__"')
        tdSql.checkRows(2)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
