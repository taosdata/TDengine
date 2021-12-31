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
        case1<shenglian zhou>: [TS-2016]fix select * from (select * from empty_stable) 
        ''' 
        return
    
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)
        self._conn = conn

    def run(self):
        print("running {}".format(__file__))
        tdSql.execute("drop database if exists td12229")
        tdSql.execute("create database if not exists td12229")
        tdSql.execute('use td12229')

        tdSql.execute('create stable st(ts timestamp , value int ) tags (ind int)') 
        tdSql.execute('insert into tb1 using st tags(1) values(now ,1)') 
        tdSql.execute('insert into tb1 using st tags(1) values(now+1s ,2)') 
        tdSql.execute('insert into tb1 using st tags(1) values(now+2s ,3)') 
        tdSql.execute('create stable ste(ts timestamp , value int ) tags (ind int)') 
        tdSql.query('select * from st')
        tdSql.checkRows(3)
        tdSql.query('select * from (select * from ste)')
        tdSql.checkRows(0)
        tdSql.query('select * from st union all select * from ste')
        tdSql.checkRows(3)
        tdSql.query('select * from ste union all select * from st')
        tdSql.checkRows(3)
        tdSql.query('select count(ts) from ste group by tbname union all select count(ts) from st group by tbname;')
        tdSql.checkRows(1)
        tdSql.query('select count(ts) from st group by tbname union all select count(ts) from ste group by tbname;')
        tdSql.checkRows(1)
        tdSql.execute('drop database td12229')
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
