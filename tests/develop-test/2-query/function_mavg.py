
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
        case1<shenglian zhou>: [TD-10799]mavg(col, 4-3 ) promots error 
        ''' 
        return
    
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)
        self._conn = conn

    def run(self):
        print("running {}".format(__file__))
        tdSql.execute("drop database if exists td10799")
        tdSql.execute("create database if not exists td10799")
        tdSql.execute('use td10799')

        tdSql.execute('create stable st(ts timestamp , value int ) tags (ind int)') 
        tdSql.execute('insert into tb1 using st tags(1) values(now ,1)') 
        tdSql.execute('insert into tb1 using st tags(1) values(now+1s ,2)') 
        tdSql.execute('insert into tb1 using st tags(1) values(now+2s ,3)') 
        tdSql.query('select * from st')
        tdSql.checkRows(3)
        tdSql.query('select mavg(value, 100) from st group by tbname')
        tdSql.checkRows(0)
        tdSql.error('select mavg(value, 4-3) from st group by tbname')
        tdSql.execute('drop database td10799')
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
