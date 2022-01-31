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
        case1<ganlin zhao>: [TD-13246] Coredump when parentheses appear before the insert_sql
        '''
        return

    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)
        self._conn = conn

    def run(self):
        print("running {}".format(__file__))
        tdSql.prepare()

        tdSql.execute('create stable st(ts timestamp , value int) tags (ind int)')
        tdSql.execute('create table  ctb using st tags(1)')
        tdSql.execute('create table  tb (ts timestamp, value int)')
        tdSql.query('insert into ctb values(now, 1)');
        tdSql.query('insert into tb values(now, 1)');

        tdSql.error('(insert into ctb values(now, 1)');
        tdSql.error('(insert into tb values(now, 1)');
        tdSql.error('(insert into ctb values');
        tdSql.error('(insert into ctb');
        tdSql.error('(insert into');
        tdSql.error('(insert');
        tdSql.error('(');

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
