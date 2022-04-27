###################################################################
#           Copyright (c) 2020 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-

from posixpath import split
import sys
import os 

from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *

class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)
        
        self.ts = 1420041600000 # 2015-01-01 00:00:00  this is begin time for first record
        self.num = 10

    def caseDescription(self):

        '''
        case1 <wenzhouwww>: [TD-11943] : 
            this test case is an test case for unexpected coredump about taosd ;
            root cause : the pExpr2 of sql select tbname, max(col)+5 from child_table has two functions, col_proj and scalar_expr.
            for function col_proj (tbname column), it is a tag during master scan stage, the input data is not set. 

        ''' 
        return 

   
    def run(self):
        tdSql.prepare()
        tdSql.execute("create database if not exists testdb keep 36500;")
        tdSql.execute("use testdb;")
        tdSql.execute("create stable st (ts timestamp , id int , value double) tags(hostname binary(10) ,ind int);")
        for i in range(self.num):
            tdSql.execute("insert into sub_%s using st tags('host_%s' , %d) values (%d , %d , %f );"%(str(i),str(i),i*10,self.ts+10000*i,i*2,i+10.00))

        tdSql.query("select tbname ,max(value) from st;")
        tdSql.checkRows(1)
        tdSql.checkData(0,1,19)
        tdSql.query("select tbname ,max(value)+5 from st;")
        tdSql.checkRows(1)
        tdSql.checkData(0,1,24)
        tdSql.query("select tbname ,max(value) from sub_1;")
        tdSql.checkRows(1)
        tdSql.checkData(0,1,11)
        tdSql.query("select tbname ,max(value)+5 from sub_1;")
        tdSql.checkRows(1)
        tdSql.checkData(0,1,16)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())


