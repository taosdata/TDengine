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
        case1<shenglian zhou>: [TD-12145]function/clause program inserted column will be use as ts in outerquery        
        case2<shenglian zhou>: [TD-12164]elapsed function can only take primary timestamp as first parameter
        case3<shenglian zhou>: [TD-12165]_c0 can not be alias name
        ''' 
        return
    
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)
        self._conn = conn

    def run(self):
        print("running {}".format(__file__))
        tdSql.execute("drop database if exists td12145")
        tdSql.execute("create database if not exists td12145")
        tdSql.execute('use td12145')

        tdSql.execute('create stable st(ts timestamp , value int ) tags (ind int)') 
        tdSql.execute('insert into tb1 using st tags(1) values(now ,1)') 
        tdSql.execute('insert into tb1 using st tags(1) values(now+1s ,2)') 
        tdSql.execute('insert into tb1 using st tags(1) values(now+2s ,3)') 
        tdSql.error('select elapsed(ts00 ,1s) from (select elapsed(ts,1s) ts00 from tb1)')
        tdSql.error('select elapsed(ts00 ,1s) from (select value ts00 from tb1)')
        tdSql.error('select _c0 from (select value as _c0 , _c0 from st)')
        tdSql.error('select ts from (select value as _c0 , ts from st)')
        tdSql.query('select ts, max(nestvalue) from (select csum(value) nestvalue from tb1)')
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 6)

        tdSql.execute('drop database td12145')
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
