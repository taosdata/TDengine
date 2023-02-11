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
        case1<shenglian zhou>: [TS-2523]join not supported on tables of different databases
        '''
        return

    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)
        self._conn = conn

    def run(self):
        print("running {}".format(__file__))
        tdSql.execute("drop database if exists d1")
        tdSql.execute("create database if not exists d1")
        tdSql.execute('use d1')

        tdSql.execute('create stable st(ts timestamp , value int ) tags (ind int)')
        tdSql.execute('insert into tb1 using st tags(1) values(now ,1)')
        tdSql.execute('insert into tb1 using st tags(1) values(now+1s ,2)')
        tdSql.execute('insert into tb1 using st tags(1) values(now+2s ,3)')
        tdSql.execute("drop database if exists d2")
        tdSql.execute("create database if not exists d2")
        tdSql.execute('use d2')

        tdSql.execute('create stable st(ts timestamp , value int ) tags (ind int)')
        tdSql.execute('insert into tb1 using st tags(1) values(now ,1)')
        tdSql.execute('insert into tb1 using st tags(1) values(now+1s ,2)')
        tdSql.execute('insert into tb1 using st tags(1) values(now+2s ,3)')

        tdSql.error('select t1.value, t2.value from d1.st t1, d2.st t2 where t1.ts=t2.ts and t1.ind = t2.ind');

        tdSql.execute('drop database d1')
        tdSql.execute('drop database d2')
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
