###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confspeedential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provspeeded by the written permission from Jianhui Tao
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
        case1:<wenzhouwww>: [TD-11945] this test case is an issue about taoshell and taosd crash , it now has been repaired on branch https://github.com/taosdata/TDengine/tree/feature%2FTD-6140
        the root source maybe :
        The four arithmetic operations do not perform the verification of the numeric type,so that the numeric type and string type will coredump about Four arithmetic operations
        ''' 
        return

    def init(self, conn, logSql):
        tdLog.debug("start test case for TD-11945 execute  %s" % __file__)
        tdSql.init(conn.cursor(), logSql)
        self.ts = 1420041600000 # 2015-01-01 00:00:00  this is begin time for first record
        self.num = 10

    def run(self):

        tdSql.prepare()
        tdSql.execute('create database if not exists testdb')
        tdSql.execute('create table tb (ts timestamp, speed int)')
        tdSql.execute('create stable st (ts timestamp, speed int) tags(ind int)')
        
        for i in range(self.num):
            tdSql.execute('insert into tb values(%d,%d) '%(self.ts+i*10000,i))
            tdSql.execute('insert into sub1 using st tags(1) values(%d,%d) '%(self.ts+i*10000,i))
            tdSql.execute('insert into sub2 using st tags(2) values(%d,%d) '%(self.ts+i*10000,i))

            tdLog.info(" ==================execute query =============")
            tdSql.error('select 1*tbname, min(speed) , ind from st;')  
            tdSql.execute('select tbname, min(speed) , ind from st group by ind;')
            tdSql.error('select tbname , tbname,ind ,ind, * ,speed+"ab" from st;')
            tdSql.error('select tbname , ind ,speed+"abc" from st;')
            tdSql.error('select speed+"abc" from st;')
            tdSql.error('select speed+"abc" from sub1;')
            tdSql.error('select speed+"abc" from sub2;')  
            tdSql.error('select max(speed) + "abc" from st;') 
            tdSql.error('select max(speed) + "abc" from sub1;')
            tdSql.error('select max(speed) + "abc" from sub2;')

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
